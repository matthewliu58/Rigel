package upload

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"math/rand"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"rigel-client/util"
	"time"
)

var (
	ErrFileSizeFailed          = errors.New("get file size failed")
	ErrChunkSplitFailed        = errors.New("split file failed")
	ErrUploadTimeout           = errors.New("upload timeout")
	ErrInterfaceNotImplemented = errors.New("implementation is nil")
)

type (
	ChunkStatus int
)

const (
	ChunkStatusInit           ChunkStatus = 0
	ChunkStatusTransferring   ChunkStatus = 1
	ChunkStatusTransferFailed ChunkStatus = 2
	ChunkStatusCompleted      ChunkStatus = 3
)

const (
	MaxConcurrency          = 10
	QueueBufferSize         = 100
	CheckInterval           = 10 * time.Second // 分块超时检查间隔
	ChunkExpireTime         = 10 * time.Second // 分块超时重传阈值
	UploadTimeout           = 3 * time.Minute  // 整体上传超时时间
	TaskSubmitRetryInterval = 1 * time.Second
	ChunkSubmitDelay        = 100 * time.Millisecond
	ChunkSizeInMemory       = 512 * 1024 * 1024 // 512MB
)

type ChunkEventType int

const (
	ChunkExpired ChunkEventType = iota
	ChunkFinished
)

type ChunkEvent struct {
	Type    ChunkEventType               // 事件类型
	Indexes map[string]*split.ChunkState // 超时分块索引
}

type ChunkTask struct {
	Ctx        context.Context // 带requestID的上下文
	Index      string
	Chunks     *util.SafeMap
	ObjectName string
	Upload     base.UploadStruct
	Pre        string // 保留原有pre入参，完全兼容
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}

type WorkerPool struct {
	TaskCh chan ChunkTask
	cancel func() // 新增：协程池退出信号
}

// -------------------------- 6. 核心函数 --------------------------

// StartChunkTimeoutChecker 保留pre入参，同时用上下文透传，新增全局超时控制
// 参数新增：
//
//	globalTimeout - 检查器整体运行超时时间（0表示不限制，仅靠ctx控制）
func StartChunkTimeoutChecker(
	ctx context.Context,
	s *util.SafeMap,
	interval time.Duration,
	expire time.Duration,
	globalTimeout time.Duration, // 新增：检查器整体超时时间
	events chan<- ChunkEvent,
	pre string, // 保留原有pre入参
	logger *slog.Logger,
) {

	// 2. 构建带全局超时的上下文（核心修复：添加超时控制）
	var cancel func()
	if globalTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, globalTimeout)
		defer cancel() // 确保超时/退出时释放资源
	}

	// 3. 初始化定时器
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("StartChunkTimeoutChecker",
		slog.String("pre", pre),
		slog.Duration("interval", interval),
		slog.Duration("expire", expire),
		slog.Duration("global_timeout", globalTimeout))

	// 4. 循环逻辑：增加全局超时退出分支
	for {
		select {
		case <-ticker.C:
			expired, finished, hasInitSendingChunk := CollectExpiredChunks(s, expire, pre, logger)

			// 原有业务逻辑：检查分片状态并发送事件
			if !hasInitSendingChunk {
				if finished {
					events <- ChunkEvent{Type: ChunkFinished}
					logger.Info("Chunk checker exit: all chunks finished", slog.String("pre", pre))
					return // 正常退出
				}
				if len(expired) > 0 {
					events <- ChunkEvent{Type: ChunkExpired, Indexes: expired}
					logger.Warn("Chunk checker detect expired chunks",
						slog.String("pre", pre),
						slog.Any("expired_indexes", expired))
				}
			}

		case <-ctx.Done():
			// 上下文结束（超时/手动取消），退出循环
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				logger.Warn("Chunk checker exit: global timeout reached",
					slog.String("pre", pre),
					slog.Duration("timeout", globalTimeout))
			} else {
				logger.Info("Chunk checker exit: context canceled",
					slog.String("pre", pre),
					slog.Any("err", err))
			}
			return // 超时/取消退出
		}
	}
}

// CollectExpiredChunks 保留pre入参，状态枚举替换Acked
func CollectExpiredChunks(
	s *util.SafeMap,
	expire time.Duration,
	pre string, // 保留pre入参
	logger *slog.Logger,
) (expired map[string]*split.ChunkState, finished, hasInitSendingChunk bool) {
	now := time.Now()
	expired = make(map[string]*split.ChunkState)
	finished = true // 先假设都 ack 了

	logger.Info("CollectExpiredChunks", slog.String("pre", pre),
		slog.Any("now", now), slog.Any("expire", expire))
	chunks := s.GetAll()

	for _, v := range chunks {
		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		// 核心改造：用枚举替代原Acked数值判断
		status := ChunkStatus(v_.Acked)

		//还没发送完不能resubmit
		if status == ChunkStatusInit {
			logger.Warn("Resubmit rejected, initial transmission is still in progress",
				slog.String("pre", pre), slog.String("index", v_.Index))
			return expired, false, true
		}

		if status == ChunkStatusTransferring || status == ChunkStatusTransferFailed {
			finished = false // 只要发现一个没 ack，就没完成
			if !v_.LastSend.IsZero() && now.Sub(v_.LastSend) > expire {
				expired[v_.Index] = v_
			}
		}
	}

	return expired, finished, false
}

// NewWorkerPool 保留pre入参，上下文透传 + 新增取消逻辑
//func NewWorkerPool(
//	fo base.FileOperateInterfaces,
//	queueSize int,
//	routingInfo RoutingInfo,
//	handler func(base.FileOperateInterfaces, ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
//	inMemory bool,
//	pre string, // 保留pre入参
//	logger *slog.Logger,
//) *WorkerPool {
//	taskCh := make(chan ChunkTask, queueSize)
//	// 新增：创建取消上下文，用于终止协程池
//	ctx, cancel := context.WithCancel(context.Background())
//
//	p := &WorkerPool{
//		TaskCh: taskCh,
//		cancel: cancel, // 保存取消函数
//	}
//	logger.Info("NewWorkerPool", slog.String("pre", pre), "queueSize", queueSize)
//
//	workerNum := len(routingInfo.Routing)
//	if workerNum <= 0 {
//		for i := 0; i < MaxConcurrency; i++ {
//			go func(workerID int) {
//				logger.Info("Worker for direct init", slog.String("pre", pre), "worker", workerID)
//
//				for {
//					select {
//					case <-ctx.Done(): // 监听取消信号
//						logger.Info("Worker exit: context canceled", slog.String("pre", pre), "worker", workerID)
//						return
//					case task, ok := <-taskCh: // 监听任务通道（关闭时ok=false）
//						if !ok {
//							logger.Info("Worker exit: task channel closed", slog.String("pre", pre), "worker", workerID)
//							return
//						}
//
//						err := handler(
//							fo,
//							task,
//							"",
//							nil,
//							inMemory,
//							pre, // 传递pre入参
//							logger,
//						)
//
//						if err != nil {
//							logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
//						} else {
//							logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task.Index)
//						}
//					}
//				}
//			}(i)
//		}
//	} else {
//
//		for i := 0; i < MaxConcurrency; i++ {
//
//			rand.Seed(time.Now().UnixNano())
//			index := rand.Intn(workerNum)
//
//			go func(workerID int, pathInfo PathInfo) {
//				rate_ := pathInfo.Rate
//				bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
//				limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
//
//				logger.Info("Worker for redirect init", slog.String("pre", pre),
//					"worker", workerID, "rate", rate_, "hops", pathInfo.Hops)
//
//				for {
//					select {
//					case <-ctx.Done(): // 监听取消信号
//						logger.Info("Worker exit: context canceled", slog.String("pre", pre), "worker", workerID)
//						return
//					case task, ok := <-taskCh: // 监听任务通道
//						if !ok {
//							logger.Info("Worker exit: task channel closed", slog.String("pre", pre), "worker", workerID)
//							return
//						}
//
//						// 上下文附加pre
//						err := handler(
//							fo,
//							task,
//							pathInfo.Hops,
//							limiter,
//							inMemory,
//							pre, // 传递pre入参
//							logger,
//						)
//
//						if err != nil {
//							logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
//						} else {
//							logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task.Index)
//						}
//					}
//				}
//			}(i, routingInfo.Routing[index])
//
//		}
//	}
//	return p
//}

// NewWorkerPool 保留pre入参，上下文透传 + 新增取消逻辑
func NewWorkerPool(
	fo base.FileOperateInterfaces,
	queueSize int,
	routingInfo RoutingInfo,
	handler func(base.FileOperateInterfaces, ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	inMemory bool,
	pre string, // 保留pre入参
	logger *slog.Logger,
) *WorkerPool {
	taskCh := make(chan ChunkTask, queueSize)
	// 新增：创建取消上下文，用于终止协程池
	ctx, cancel := context.WithCancel(context.Background())

	p := &WorkerPool{
		TaskCh: taskCh,
		cancel: cancel, // 保存取消函数
	}
	logger.Info("NewWorkerPool", slog.String("pre", pre), "queueSize", queueSize)

	// 提取通用 worker 执行函数，消除重复代码
	runWorker := func(workerID int, hops string, limiter *rate.Limiter, workerType string) {
		logger.Info(fmt.Sprintf("Worker for %s init", workerType), slog.String("pre", pre), "worker", workerID,
			slog.String("hops", hops), slog.Any("limiter", limiter))

		for {
			select {
			case <-ctx.Done(): // 监听取消信号
				logger.Info("Worker exit: context canceled", slog.String("pre", pre), "worker", workerID)
				return
			case task, ok := <-taskCh: // 监听任务通道（关闭时ok=false）
				if !ok {
					logger.Info("Worker exit: task channel closed", slog.String("pre", pre), "worker", workerID)
					return
				}

				err := handler(
					fo,
					task,
					hops,
					limiter,
					inMemory,
					pre, // 传递pre入参
					logger,
				)

				if err != nil {
					logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
				} else {
					logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task.Index)
				}
			}
		}
	}

	workerNum := len(routingInfo.Routing)
	if workerNum <= 0 {
		// direct 分支：hops为空，limiter为nil
		for i := 0; i < MaxConcurrency; i++ {
			go runWorker(i, "", nil, "direct")
		}
	} else {
		// redirect 分支：初始化limiter和hops
		rand.Seed(time.Now().UnixNano())
		for i := 0; i < MaxConcurrency; i++ {
			index := rand.Intn(workerNum)
			pathInfo := routingInfo.Routing[index]
			rate_ := pathInfo.Rate
			bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
			limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
			go runWorker(i, pathInfo.Hops, limiter, "redirect")
		}
	}
	return p
}

// Stop 新增：终止协程池（关闭任务通道 + 取消上下文）
func (p *WorkerPool) Stop() {
	if p.cancel != nil {
		p.cancel() // 触发所有worker的ctx.Done()
	}
	close(p.TaskCh) // 关闭任务通道
}

// ChunkEventLoop 保留pre入参，状态枚举替换Acked + 监听取消信号
func ChunkEventLoop(ctx context.Context, fo base.FileOperateInterfaces, upload base.UploadStruct,
	chunks *util.SafeMap, workerPool *WorkerPool, events <-chan ChunkEvent, done chan struct{}, inMemory bool,
	pre string, logger *slog.Logger) {

	logger.Info("ChunkEventLoop", slog.String("pre", pre))

	for {
		select {
		case ev, ok := <-events: // 监听事件通道（关闭时ok=false）
			if !ok {
				logger.Info("ChunkEventLoop exit: events channel closed", slog.String("pre", pre))
				close(done)
				return
			}
			switch ev.Type {
			case ChunkExpired:
				logger.Warn("ChunkExpired", slog.String("pre", pre), "indexes", ev.Indexes)
				StartChunkSubmitLoop(ctx, chunks, workerPool, upload, true, ev.Indexes, pre, logger)

			case ChunkFinished:
				logger.Info("ChunkFinished", slog.String("pre", pre),
					slog.String("fileName", upload.File.NewFileName))

				var parts []string
				chunks_ := chunks.GetAll()
				for _, v := range chunks_ {
					v_, ok := v.(*split.ChunkState)
					if !ok {
						continue
					}

					// 用枚举判断状态
					if ChunkStatus(v_.Acked) != ChunkStatusCompleted {
						logger.Error("upload failed", slog.String("pre", pre),
							slog.String("fileName", upload.File.NewFileName), "index", v_.Index)
						close(done)
						return
					}
					logger.Info("upload success", slog.String("pre", pre), slog.String("fileName",
						upload.File.NewFileName), "index", v_.Index, "ObjectName", v_.ObjectName)
					parts = append(parts, v_.ObjectName)
				}

				parts = util.SortPartStrings(parts)

				var err error
				if fo.ComposeFile.ComposeFile == nil {
					logger.Error("ComposeFile is nil", slog.String("pre", pre))
					return
				}
				err = fo.ComposeFile.ComposeFile(ctx, upload.File.NewFileName, parts, pre, logger)
				if err != nil {
					logger.Error("Compose failed", slog.String("pre", pre),
						slog.String("fileName", upload.File.NewFileName), slog.Any("err", err))
				}
				close(done)

				_ = util.DeleteFilesInDir(upload.Proxy.LocalDir, parts, pre, logger)

				return
			}

		case <-ctx.Done(): // 监听主上下文取消信号（超时/手动终止）
			logger.Info("ChunkEventLoop exit: context canceled", slog.String("pre", pre), "err", ctx.Err())
			close(done)
			return
		}
	}
}

// Submit 保留原有逻辑，兼容pre
func (p *WorkerPool) Submit(task ChunkTask) bool {
	select {
	case p.TaskCh <- task:
		return true
	default:
		// 队列满了，可以选择丢 / 打日志 / 统计
		return false
	}
}

// StartChunkSubmitLoop 保留pre入参，状态枚举判断 + 监听取消信号
func StartChunkSubmitLoop(
	ctx context.Context,
	chunks *util.SafeMap,
	workerPool *WorkerPool,
	uploadInfo base.UploadStruct,
	resubmit bool,
	resubmitIndexes map[string]*split.ChunkState,
	pre string, // 保留pre入参
	logger *slog.Logger,
) {
	logger.Info("StartChunkSubmitLoop", slog.String("pre", pre), "fileName", uploadInfo.File.NewFileName)
	chunks_ := chunks.GetAll()

	for _, v := range chunks_ {
		// 每次循环都检查上下文是否取消，避免无效提交
		select {
		case <-ctx.Done():
			logger.Info("StartChunkSubmitLoop exit: context canceled", slog.String("pre", pre))
			return
		default:
			time.Sleep(ChunkSubmitDelay)
		}

		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		// 用枚举判断状态
		status := ChunkStatus(v_.Acked)
		if resubmit {
			if _, ok := resubmitIndexes[v_.Index]; !ok || status == ChunkStatusCompleted {
				continue
			}
		} else {
			if status != ChunkStatusInit {
				continue
			}
		}

		task := ChunkTask{
			Ctx:        ctx,
			Index:      v_.Index,
			Chunks:     chunks,
			ObjectName: v_.ObjectName,
			Upload:     uploadInfo,
			Pre:        pre, // 赋值pre字段
		}

		if !workerPool.Submit(task) {
			logger.Warn("workerPool full", slog.String("pre", pre))
			time.Sleep(TaskSubmitRetryInterval)
			break
		}
	}
}

func UploadFunc_(
	cleintB bool,
	us base.UploadStruct,
	pre string, // 保留原有pre入参
	logger *slog.Logger) base.FileOperateInterfaces {

	logger.Info("UploadFunc_", slog.String("pre", pre), slog.Any("us", us))
	fo := base.InitInterface(cleintB, us, pre, logger)

	return fo
}

// Upload 核心入口：保留pre入参，上下文透传 + 统一取消所有goroutine
func UploadFunc(
	cleintB bool,
	us base.UploadStruct,
	handler func(base.FileOperateInterfaces, ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	routing RoutingInfo,
	noSplitB bool,
	pre string, // 保留原有pre入参
	logger *slog.Logger) error {

	logger.Info("UploadFunc", slog.String("pre", pre), slog.Any("us", us))

	// 核心修复1：创建带全局超时的可取消上下文（管控所有goroutine）
	ctx, cancel := context.WithTimeout(context.Background(), UploadTimeout)
	defer func() {
		cancel() // 无论正常/异常退出，都取消上下文
		logger.Info("Upload context canceled", slog.String("pre", pre))
	}()

	// 1. 初始化接口
	fo := base.InitInterface(cleintB, us, pre, logger)

	// 3. 获取文件真实长度
	var fileSize int64
	var err error
	if fo.GetFileSize.GetFileSize == nil {
		logger.Info("GetFileSize is nil, use default implementation")
		return fmt.Errorf("%w: GetFileSize is nil", ErrInterfaceNotImplemented)
	}
	fileSize, err = fo.GetFileSize.GetFileSize(ctx, us.File.FileName, pre, logger)
	if err != nil {
		logger.Error("Get file size failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("%w: %s", ErrFileSizeFailed, err.Error())
	}
	logger.Info("Get file size success", slog.String("pre", pre), slog.Int64("size", fileSize))

	// 4. 文件分块
	chunks := util.NewSafeMap()
	_, err = split.SplitFilebyRange(fileSize, us.File.FileStart, us.File.FileLength,
		us.File.FileName, us.File.NewFileName, noSplitB, chunks, pre, logger)
	if err != nil {
		logger.Error("Split file failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("%w: %s", ErrChunkSplitFailed, err.Error())
	}

	//inMemory := false
	//if chunkSize >= ChunkSizeInMemory || uploadInfo.Source.SourceType == util.LocalDisk {
	//	inMemory = true
	//}
	//In-memory mode is enabled by default
	inMemory := true

	//启动定时重传 & check传输完毕
	done := make(chan struct{})
	events := make(chan ChunkEvent, QueueBufferSize)
	// 启动超时检查器（传入带超时的ctx）
	go StartChunkTimeoutChecker(ctx, chunks, CheckInterval, ChunkExpireTime, UploadTimeout, events, pre, logger)

	//启动消费者 默认一个http并发度
	workerPool := NewWorkerPool(fo, QueueBufferSize, routing, handler, inMemory, pre, logger)
	defer workerPool.Stop() // 核心修复2：退出时终止协程池

	//events 消费（传入带超时的ctx）
	go ChunkEventLoop(ctx, fo, us, chunks, workerPool, events, done, inMemory, pre, logger)

	// 4. 启动分片上传（传入带超时的ctx）
	go StartChunkSubmitLoop(ctx, chunks, workerPool, us, false, nil, pre, logger)

	newFileName := us.File.NewFileName
	select {
	case <-done:
		logger.Info("Function finished", slog.String("pre", pre), slog.String("newFileName", newFileName))
	case <-ctx.Done():
		// 超时/取消触发
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Warn("Timeout exit", slog.String("pre", pre), slog.String("newFileName", newFileName))
			return fmt.Errorf("%w: %s", ErrUploadTimeout, newFileName)
		}
		logger.Info("Upload canceled", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("upload canceled: %w", err)
	}

	logger.Info("Main program finished", slog.String("pre", pre), slog.String("newFileName", newFileName))
	return nil
}

// GetTransferReader 保留pre入参，上下文透传
func GetTransferReader(
	ctx context.Context,
	fo base.FileOperateInterfaces,
	upload base.UploadStruct,
	start, length int64,
	objectName string,
	inMemory bool,
	pre string, // 保留pre入参
	logger *slog.Logger,
) (io.ReadCloser, error) {

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("GetTransferReader canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	var reader io.ReadCloser
	var err error
	if fo.DownloadFile.DownloadFile == nil {
		logger.Error("DownloadFile is nil", slog.String("pre", pre))
		return nil, fmt.Errorf("%w: DownloadFile is nil", ErrInterfaceNotImplemented)
	}
	reader, err = fo.DownloadFile.DownloadFile(ctx, upload.File.FileName, objectName,
		start, length, "", inMemory, pre, logger)
	if err != nil {
		logger.Error("GetTransferReader failed", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	}

	logger.Info("GetTransferReader success",
		slog.String("pre", pre),
		slog.String("fileName", upload.File.FileName),
		slog.Int64("start", start),
		slog.Int64("length", length),
		slog.String("objectName", objectName))

	return reader, nil
}
