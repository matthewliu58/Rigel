package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"golang.org/x/time/rate"

	"rigel-client/download"
	"rigel-client/upload/compose"
	"rigel-client/upload/split"
	"rigel-client/util"
)

// -------------------------- 1. 包级哨兵错误定义 --------------------------
var (
	ErrFileSizeFailed    = errors.New("get file size failed")
	ErrChunkSplitFailed  = errors.New("split file failed")
	ErrUploadTimeout     = errors.New("upload timeout")
	ErrChunkMergeFailed  = errors.New("merge chunks failed")
	ErrTaskSubmitFailed  = errors.New("task submit failed (queue full)")
	ErrInvalidChunkState = errors.New("invalid chunk state")
	ErrUnsupportedType   = errors.New("unsupported source/dest type")
)

// -------------------------- 2. 分块状态枚举（替换原Acked 0/1/2） --------------------------
type ChunkStatus int

const (
	ChunkStatusInit         ChunkStatus = 0 // 初始状态，未开始传输（对应原Acked=0）
	ChunkStatusTransferring ChunkStatus = 1 // 传输中，已发送但未确认（对应原Acked=1）
	ChunkStatusCompleted    ChunkStatus = 2 // 传输完成，已确认（对应原Acked=2）
)

// String 状态转字符串，便于日志/调试
func (s ChunkStatus) String() string {
	switch s {
	case ChunkStatusInit:
		return "init"
	case ChunkStatusTransferring:
		return "transferring"
	case ChunkStatusCompleted:
		return "completed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// -------------------------- 3. 上下文Key定义（透传requestID/pre） --------------------------
type ctxKey string

const (
	CtxKeyRequestID ctxKey = "request_id"
)

// WithRequestID 给上下文附加requestID（pre）
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, CtxKeyRequestID, requestID)
}

// GetRequestID 从上下文获取requestID（兼容pre）
func GetRequestID(ctx context.Context) string {
	id, ok := ctx.Value(CtxKeyRequestID).(string)
	if !ok {
		return "unknown" // 兜底值
	}
	return id
}

// -------------------------- 4. 原有常量定义（完全保留） --------------------------
const (
	MaxConcurrency  = 10  // 协程池最大并发数
	QueueBufferSize = 100 // 任务队列缓冲大小

	GCPCLoud   = "gcp-cloud"
	RemoteDisk = "remote-disk"
	LocalDisk  = "local-disk"

	CheckInterval           = 10 * time.Second  // 分块超时检查间隔
	ChunkExpireTime         = 120 * time.Second // 分块超时重传阈值
	UploadTimeout           = 5 * time.Minute   // 整体上传超时时间
	ChunkSizeInMemory       = 512 * 1024 * 1024 // 512MB
	TaskSubmitRetryInterval = 3 * time.Second
	ChunkSubmitDelay        = 200 * time.Millisecond
)

// -------------------------- 5. 结构体定义（保留pre/RequestID，兼容原有逻辑） --------------------------
type ChunkEventType int

const (
	ChunkExpired ChunkEventType = iota
	ChunkFinished
)

type ChunkEvent struct {
	Type    ChunkEventType               // 事件类型
	Indexes map[string]*split.ChunkState // 超时分块索引
}

type SourceInfo struct {
	SourceType string // 源类型（disk/cloud）
	User       string // SSH用户名
	HostPort   string // SSH主机IP
	//SSHPort    string // SSH端口
	Password   string // SSH密码
	RemoteDir  string
	BucketName string
	CredFile   string
}

type DestInfo struct {
	DestType   string // 目标类型（disk/cloud）
	FileSys    util.FileSys
	BucketName string
	CredFile   string
}

type FileInfo struct {
	Start       int64  // 分块起始偏移
	Length      int64  // 分块长度
	FileName    string // 源文件名称
	NewFileName string // 目标文件名称
}

type ChunkTask struct {
	Ctx          context.Context // 带requestID的上下文
	Index        string
	Chunks       *util.SafeMap
	ObjectName   string
	File         FileInfo
	Source       SourceInfo
	Dest         DestInfo
	LocalBaseDir string
	Pre          string // 保留原有pre入参，完全兼容
}

type UploadInfo struct {
	File         FileInfo
	Source       SourceInfo
	Dest         DestInfo
	LocalBaseDir string
}

type WorkerPool struct {
	TaskCh chan ChunkTask
}

// -------------------------- 6. 核心函数（保留pre入参 + 上下文透传） --------------------------

// StartChunkTimeoutChecker 保留pre入参，同时用上下文透传
func StartChunkTimeoutChecker(
	ctx context.Context,
	s *util.SafeMap,
	interval time.Duration,
	expire time.Duration,
	events chan<- ChunkEvent,
	pre string, // 保留原有pre入参
	logger *slog.Logger,
) {
	// 上下文附加pre，双重保障
	ctx = WithRequestID(ctx, pre)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("StartChunkTimeoutChecker", slog.String("pre", pre),
		slog.Any("interval", interval), slog.Any("expire", expire))

	for {
		select {
		case <-ticker.C:
			expired, finished, unfinished := CollectExpiredChunks(ctx, s, expire, pre, logger)

			if !unfinished {
				if finished {
					events <- ChunkEvent{Type: ChunkFinished}
					return
				}
				if len(expired) > 0 {
					events <- ChunkEvent{Type: ChunkExpired, Indexes: expired}
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// CollectExpiredChunks 保留pre入参，状态枚举替换Acked
func CollectExpiredChunks(
	ctx context.Context,
	s *util.SafeMap,
	expire time.Duration,
	pre string, // 保留pre入参
	logger *slog.Logger,
) (expired map[string]*split.ChunkState, finished, unfinished bool) {
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
			logger.Info("还没发送完不能resubmit", slog.String("pre", pre),
				slog.String("index", v_.Index))
			return expired, false, true
		}

		if status == ChunkStatusTransferring {
			finished = false // 只要发现一个没 ack，就没完成

			if !v_.LastSend.IsZero() && now.Sub(v_.LastSend) > expire {
				expired[v_.Index] = v_
			}
		}
	}

	return expired, finished, false
}

// NewWorkerPool 保留pre入参，上下文透传
func NewWorkerPool(
	queueSize int,
	routingInfo util.RoutingInfo,
	handler func(ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	inMemory bool,
	pre string, // 保留pre入参
	logger *slog.Logger,
) *WorkerPool {
	p := &WorkerPool{TaskCh: make(chan ChunkTask, queueSize)}
	logger.Info("NewWorkerPool", slog.String("pre", pre), "queueSize", queueSize)

	workerNum := len(routingInfo.Routing)
	if workerNum <= 0 {
		for i := 0; i < MaxConcurrency; i++ {
			go func(workerID int) {
				logger.Info("Worker for direct init", slog.String("pre", pre), "worker", workerID)

				for task := range p.TaskCh {
					// 上下文附加pre，确保task的ctx也带pre
					task.Ctx = WithRequestID(task.Ctx, pre)
					err := handler(
						task,
						"",
						nil,
						inMemory,
						pre, // 传递pre入参
						logger,
					)

					if err != nil {
						logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
					} else {
						logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task)
					}
				}
			}(i)
		}
	} else {
		for i := 0; i < workerNum; i++ {
			go func(workerID int, pathInfo util.PathInfo) {
				rate_ := pathInfo.Rate
				bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
				limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))

				logger.Info("Worker for redirect init", slog.String("pre", pre),
					"worker", workerID, "rate", rate_, "hops", pathInfo.Hops)

				for task := range p.TaskCh {
					// 上下文附加pre
					task.Ctx = WithRequestID(task.Ctx, pre)
					err := handler(
						task,
						pathInfo.Hops,
						limiter,
						inMemory,
						pre, // 传递pre入参
						logger,
					)

					if err != nil {
						logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
					} else {
						logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task)
					}
				}
			}(i, routingInfo.Routing[i])
		}
	}
	return p
}

// ChunkEventLoop 保留pre入参，状态枚举替换Acked
func ChunkEventLoop(ctx context.Context, chunks *util.SafeMap, workerPool *WorkerPool,
	uploadInfo UploadInfo, events <-chan ChunkEvent, done chan struct{}, pre string, // 保留pre入参
	logger *slog.Logger) {

	logger.Info("ChunkEventLoop", slog.String("pre", pre))

	for {
		select {
		case ev := <-events:
			switch ev.Type {
			case ChunkExpired:

				logger.Warn("超时重传", slog.String("pre", pre), "indexes", ev.Indexes)
				StartChunkSubmitLoop(ctx, chunks, workerPool, uploadInfo, true, ev.Indexes, pre, logger)

			case ChunkFinished:

				logger.Info("传输完成", slog.String("pre", pre),
					slog.String("fileName", uploadInfo.File.NewFileName))

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
							slog.String("fileName", uploadInfo.File.NewFileName), "index", v_.Index)
						close(done)
						return
					}
					logger.Info("传输完成", slog.String("pre", pre), slog.String("fileName",
						uploadInfo.File.NewFileName), "index", v_.Index, "ObjectName", v_.ObjectName)
					parts = append(parts, v_.ObjectName)
				}

				parts = util.SortPartStrings(parts)

				var err error
				if uploadInfo.Dest.DestType == GCPCLoud {

					bucketName := uploadInfo.Dest.BucketName
					credFile := uploadInfo.Dest.CredFile
					fileName := uploadInfo.File.NewFileName
					err = compose.ComposeTree(ctx, bucketName, fileName, credFile, parts, pre, logger)

				} else if uploadInfo.Dest.DestType == RemoteDisk {

					mergeURL := uploadInfo.Dest.FileSys.Merge
					finalFileName := uploadInfo.File.NewFileName
					_, _, err = compose.ChunkMergeClient(ctx, mergeURL, finalFileName, parts, true, pre, logger)

				}

				if err != nil {
					logger.Error("compose failed", slog.String("pre", pre),
						slog.String("fileName", uploadInfo.File.NewFileName), slog.Any("err", err))
				}
				close(done)

				//清理临时文件
				if uploadInfo.Dest.DestType == GCPCLoud || uploadInfo.Dest.DestType == RemoteDisk {
					_ = util.DeleteFilesInDir(uploadInfo.LocalBaseDir, parts)
				}

				return
			}

		case <-ctx.Done():
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

// StartChunkSubmitLoop 保留pre入参，状态枚举判断
func StartChunkSubmitLoop(
	ctx context.Context,
	chunks *util.SafeMap,
	workerPool *WorkerPool,
	uploadInfo UploadInfo,
	resubmit bool,
	resubmitIndexes map[string]*split.ChunkState,
	pre string, // 保留pre入参
	logger *slog.Logger,
) {
	logger.Info("StartChunkSubmitLoop", slog.String("pre", pre), "fileName", uploadInfo.File.NewFileName)
	chunks_ := chunks.GetAll()

	for _, v := range chunks_ {
		time.Sleep(200 * time.Millisecond)

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
			Ctx:          WithRequestID(ctx, pre), // 上下文附加pre
			Index:        v_.Index,
			Chunks:       chunks,
			ObjectName:   v_.ObjectName,
			File:         uploadInfo.File,
			Source:       uploadInfo.Source,
			Dest:         uploadInfo.Dest,
			LocalBaseDir: uploadInfo.LocalBaseDir,
			Pre:          pre, // 赋值pre字段
		}

		if !workerPool.Submit(task) {
			// 队列满了，本轮结束，等下个 tick
			logger.Warn("workerPool full", slog.String("pre", pre))
			time.Sleep(3 * time.Second)
			break
		}
	}
}

// Upload 核心入口：保留pre入参，上下文透传 + 状态枚举
func Upload(uploadInfo UploadInfo,
	handler func(ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	routing util.RoutingInfo,
	pre string, // 保留原有pre入参
	logger *slog.Logger) error {

	logger.Info("Upload", slog.String("pre", pre), slog.Any("uploadInfo", uploadInfo))

	// 3. 获取文件真实长度
	ctx := WithRequestID(context.Background(), pre) // 上下文附加pre
	var fileSize int64
	var err error
	switch uploadInfo.Source.SourceType {
	case download.GCPCLoud:

		fileSize, err = download.GetGCSObjectSize(ctx, uploadInfo.Source.BucketName,
			uploadInfo.File.FileName, uploadInfo.Source.CredFile, pre, logger)

	case download.RemoteDisk:

		RemoteDiskSSHConfig := util.SSHConfig{
			User:     uploadInfo.Source.User,
			HostPort: uploadInfo.Source.HostPort,
			Password: uploadInfo.Source.Password,
		}

		fileSize, err = download.GetRemoteFileSize(ctx, RemoteDiskSSHConfig,
			uploadInfo.Source.RemoteDir, uploadInfo.File.FileName, pre, logger)

	case download.LocalDisk:

		fileSize, err = download.GetLocalFileSize(ctx, uploadInfo.LocalBaseDir, uploadInfo.File.FileName, pre, logger)
	}
	if err != nil {

		logger.Error("Get file size failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("%w: %s", ErrFileSizeFailed, err.Error())
	}

	logger.Info("Get file size success", slog.String("pre", pre), slog.Int64("size", fileSize))

	// 4. 文件分块
	chunks := util.NewSafeMap()

	chunkSize, err := split.SplitFilebyRange(fileSize, uploadInfo.File.Start, uploadInfo.File.Length,
		uploadInfo.File.FileName, uploadInfo.File.NewFileName, chunks, pre, logger)
	if err != nil {
		logger.Error("Split file failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("%w: %s", ErrChunkSplitFailed, err.Error())
	}
	//512MB
	inMemory := false
	if chunkSize >= int64(512*1024*1024) {
		inMemory = true
	}

	//启动定时重传 & check传输完毕
	done := make(chan struct{})
	events := make(chan ChunkEvent, 100)
	interval := 10 * time.Duration(time.Second)
	expire := 120 * time.Duration(time.Second)
	go StartChunkTimeoutChecker(ctx, chunks, interval, expire, events, pre, logger)

	//启动消费者 默认一个http并发度
	workerPool := NewWorkerPool(QueueBufferSize, routing, handler, inMemory, pre, logger)

	//events 消费
	go ChunkEventLoop(ctx, chunks, workerPool, uploadInfo, events, done, pre, logger)

	// 4. 启动分片上传
	go StartChunkSubmitLoop(ctx, chunks, workerPool, uploadInfo,
		false, nil, pre, logger)

	newFileName := uploadInfo.File.NewFileName

	// 5分钟超时定时器
	timeout := 5 * time.Minute
	select {
	case <-done:
		logger.Info("Function 正常完成", slog.String("pre", pre), slog.String("newFileName", newFileName))
	case <-time.After(timeout):
		logger.Warn("等待 5 分钟超时，退出等待", slog.String("pre", pre),
			slog.String("newFileName", newFileName))
		return fmt.Errorf("%w: %s", ErrUploadTimeout, newFileName)
	}

	logger.Info("主程序执行完毕", slog.String("pre", pre), slog.String("newFileName", newFileName))
	return nil
}

// GetTransferReader 保留pre入参，上下文透传
func GetTransferReader(
	ctx context.Context,
	source SourceInfo,
	file FileInfo,
	localBaseDir string,
	objectName string,
	inMemory bool,
	pre string, // 保留pre入参
	logger *slog.Logger,
) (io.ReadCloser, error) {
	// 上下文附加pre，双重保障
	ctx = WithRequestID(ctx, pre)
	var reader io.ReadCloser
	var err error

	switch source.SourceType {
	case GCPCLoud: // GCS云存储源
		reader, err = download.DownloadFromGCSbyClient(
			ctx,
			localBaseDir,
			source.BucketName,
			file.FileName,
			objectName,
			source.CredFile,
			file.Start,
			file.Length,
			inMemory,
			pre, // 传递pre入参
			logger,
		)
		if err != nil {
			logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			return nil, err
		}

	case RemoteDisk: // 远程磁盘（SSH）源
		remoteDiskSSHConfig := util.SSHConfig{
			User:     source.User,
			HostPort: source.HostPort,
			Password: source.Password,
		}

		reader, _, err = download.SSHDDReadRangeChunk(
			ctx,
			remoteDiskSSHConfig,
			source.RemoteDir,
			file.FileName,
			objectName,
			localBaseDir,
			file.Start,
			file.Length,
			"", // bs参数传空，函数内部自动适配
			inMemory,
			pre, // 传递pre入参
			logger,
		)
		if err != nil {
			logger.Error("SSHDDReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
			return nil, err
		}

	case LocalDisk: // 本地磁盘源
		reader, _, err = download.LocalReadRangeChunk(
			ctx,
			localBaseDir,
			file.FileName,
			file.Start,
			file.Length,
			pre, // 传递pre入参
			logger,
		)
		if err != nil {
			logger.Error("LocalReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
			return nil, err
		}

	default: // 未知源类型
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, source.SourceType)
	}

	logger.Info("GetTransferReader success",
		slog.String("pre", pre),
		slog.String("sourceType", source.SourceType),
		slog.String("fileName", file.FileName),
		slog.String("objectName", objectName))

	return reader, nil
}
