package upload

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"rigel-client/download"
	"rigel-client/upload/compose"
	"rigel-client/upload/split"
	"rigel-client/util"
	"time"
)

const (
	MaxConcurrency  = 10  // 协程池最大并发数
	QueueBufferSize = 100 // 任务队列缓冲大小

	GCPCLoud   = "gcp-cloud"
	RemoteDisk = "remote-disk"
	LocalDisk  = "local-disk"
)

type ChunkEventType int

const (
	ChunkExpired ChunkEventType = iota
	ChunkFinished
)

type ChunkEvent struct {
	Type    ChunkEventType
	Indexes map[string]*split.ChunkState
}

type SourceInfo struct {
	SourceType string // 源类型（disk/cloud）

	User      string // SSH用户名（如root）
	Host      string // SSH主机IP（如192.168.1.20）
	SSHPort   string // SSH端口（默认22）
	Password  string // SSH密码
	RemoteDir string

	BucketName string
	CredFile   string
}

type DestInfo struct {
	DestType string // 目标类型（disk/cloud）

	FileSys util.FileSys

	BucketName string
	CredFile   string
}

type FileInfo struct {
	Start       int64  // 分块起始偏移
	Length      int64  // 分块长度
	FileName    string // 源文件名称
	NewFileName string // 目标文件名称
}

// UploadTask 分块上传任务结构体
type ChunkTask_ struct {
	Ctx          context.Context
	Index        string // 分块编号
	Chunks       *util.SafeMap
	ObjectName   string
	File         FileInfo
	Source       SourceInfo // 源类型（disk/cloud）
	Dest         DestInfo   // 目标类型（disk/cloud）
	LocalBaseDir string
	Pre          string // 日志前缀
}

type UploadInfo struct {
	File         FileInfo
	Source       SourceInfo // 源类型（disk/cloud）
	Dest         DestInfo   // 目标类型（disk/cloud）
	LocalBaseDir string
}

type WorkerPool_ struct {
	TaskCh chan ChunkTask_
}

func StartChunkTimeoutChecker_(
	ctx context.Context,
	s *util.SafeMap,
	interval time.Duration,
	expire time.Duration,
	events chan<- ChunkEvent,
	pre string,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)

	logger.Info("StartChunkTimeoutChecker_", slog.String("pre", pre),
		slog.Any("interval", interval), slog.Any("expire", expire))

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				expired, finished, unfinished := CollectExpiredChunks_(s, expire, pre, logger)

				if !unfinished {
					if finished {
						events <- ChunkEvent{
							Type: ChunkFinished,
						}
						return
					}

					if len(expired) > 0 {
						events <- ChunkEvent{
							Type:    ChunkExpired,
							Indexes: expired,
						}
					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()
}

func CollectExpiredChunks_(
	s *util.SafeMap,
	expire time.Duration,
	pre string,
	logger *slog.Logger,
) (expired map[string]*split.ChunkState, finished, unfinished bool) {
	now := time.Now()
	expired = make(map[string]*split.ChunkState)
	finished = true // 先假设都 ack 了

	logger.Info("CollectExpiredChunks_", slog.String("pre", pre),
		slog.Any("now", now), slog.Any("expire", expire))
	chunks_ := s.GetAll()

	for _, v := range chunks_ {
		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		//还没发送完不能resubmit
		if v_.Acked == 0 {
			logger.Info("还没发送完不能resubmit", slog.String("pre", pre),
				slog.String("index", v_.Index))
			return expired, false, true
		}

		if v_.Acked == 1 {
			finished = false // 只要发现一个没 ack，就没完成

			if !v_.LastSend.IsZero() && now.Sub(v_.LastSend) > expire {
				expired[v_.Index] = v_
			}
		}
	}

	return expired, finished, false
}

func NewWorkerPool_(
	queueSize int,
	routingInfo util.RoutingInfo,
	handler func(ChunkTask_, string, *rate.Limiter, bool, string, *slog.Logger) error,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) *WorkerPool_ {

	p := &WorkerPool_{TaskCh: make(chan ChunkTask_, queueSize)}
	logger.Info("NewWorkerPool_", slog.String("pre", pre), "queueSize", queueSize)

	workerNum := len(routingInfo.Routing) //todo 并发数可以增加 2-3倍
	if workerNum <= 0 {
		for i := 0; i < MaxConcurrency; i++ {
			go func(workerID int) {
				logger.Info("Worker for direct init", slog.String("pre", pre), "worker", workerID)

				for task := range p.TaskCh {

					err := handler(
						task,
						"",
						nil,
						inMemory,
						pre,
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

				rate_ := pathInfo.Rate                 //maxMbps
				bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
				limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))

				logger.Info("Worker for redirect init", slog.String("pre", pre),
					"worker", workerID, "rate", rate_, "hops", pathInfo.Hops)

				for task := range p.TaskCh {

					err := handler(
						task,
						pathInfo.Hops,
						limiter,
						inMemory,
						pre,
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

func ChunkEventLoop_(ctx context.Context, chunks *util.SafeMap, workerPool *WorkerPool_,
	uploadInfo UploadInfo, events <-chan ChunkEvent, done chan struct{}, pre string, logger *slog.Logger) {

	logger.Info("ChunkEventLoop_", slog.String("pre", pre))

	for {
		select {
		case ev := <-events:
			switch ev.Type {
			case ChunkExpired:
				logger.Warn("超时重传", slog.String("pre", pre), "indexes", ev.Indexes)
				StartChunkSubmitLoop_(ctx, chunks, workerPool, uploadInfo, true, ev.Indexes, pre, logger)
			case ChunkFinished:
				var parts []string

				logger.Info("传输完成", slog.String("pre", pre),
					slog.String("fileName", uploadInfo.File.NewFileName))
				chunks_ := chunks.GetAll()
				for _, v := range chunks_ {
					v_, ok := v.(*split.ChunkState)
					if !ok {
						continue
					}
					if v_.Acked != 2 {
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

func (p *WorkerPool_) Submit_(task ChunkTask_) bool {
	select {
	case p.TaskCh <- task:
		//fmt.Println("submit task", task)
		return true
	default:
		// 队列满了，可以选择丢 / 打日志 / 统计
		return false
	}
}

func StartChunkSubmitLoop_(
	ctx context.Context,
	chunks *util.SafeMap,
	workerPool *WorkerPool_,
	uploadInfo UploadInfo,
	resubmit bool,
	resubmitIndexes map[string]*split.ChunkState,
	pre string,
	logger *slog.Logger,
) {
	logger.Info("StartChunkSubmitLoop_", slog.String("pre", pre), "fileName", uploadInfo.File.NewFileName)
	chunks_ := chunks.GetAll()

	for _, v := range chunks_ {

		time.Sleep(200 * time.Millisecond)

		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		if resubmit {
			if _, ok := resubmitIndexes[v_.Index]; !ok || v_.Acked == 2 {
				continue
			}
		} else {
			if v_.Acked != 0 {
				continue
			}
		}

		task := ChunkTask_{
			Ctx:        ctx,
			Index:      v_.Index,
			Chunks:     chunks,
			ObjectName: v_.ObjectName,
			File:       uploadInfo.File,
			Source:     uploadInfo.Source,
			Dest:       uploadInfo.Dest,
		}

		if !workerPool.Submit_(task) {
			// 队列满了，本轮结束，等下个 tick
			logger.Warn("workerPool full", slog.String("pre", pre))
			time.Sleep(3 * time.Second)
			break
		}
	}
}

func Upload(uploadInfo UploadInfo,
	handler func(ChunkTask_, string, *rate.Limiter, bool, string, *slog.Logger) error,
	direct bool,
	pre string, logger *slog.Logger) error {

	// 3. 获取文件真实长度
	ctx := context.Background()
	var fileSize int64
	var err error
	switch uploadInfo.Source.SourceType {
	case download.GCPCLoud:
		fileSize, err = download.GetGCSObjectSize(ctx, uploadInfo.Source.BucketName,
			uploadInfo.File.FileName, uploadInfo.Source.CredFile, pre, logger)
	case download.RemoteDisk:
		RemoteDiskSSHConfig := util.SSHConfig{
			User:     uploadInfo.Source.User,
			Host:     uploadInfo.Source.Host + ":" + uploadInfo.Source.SSHPort,
			Password: uploadInfo.Source.Password,
		}
		fileSize, err = download.GetRemoteFileSize(ctx, RemoteDiskSSHConfig,
			uploadInfo.Source.RemoteDir, uploadInfo.File.FileName, pre, logger)
	case download.LocalDisk:
		fileSize, err = download.GetLocalFileSize(ctx, uploadInfo.LocalBaseDir, uploadInfo.File.FileName, pre, logger)
	}
	if err != nil {
		logger.Error("Get file size failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	logger.Info("Get file size success", slog.String("pre", pre), slog.Int64("size", fileSize))

	// 4. 文件分块
	chunks := util.NewSafeMap()

	chunkSize, err := split.SplitFilebyRange(fileSize, uploadInfo.File.Start, uploadInfo.File.Length,
		uploadInfo.File.FileName, uploadInfo.File.NewFileName, chunks, pre, logger)
	if err != nil {
		logger.Error("Split file failed", slog.String("pre", pre), slog.Any("err", err))
		return err
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
	StartChunkTimeoutChecker_(ctx, chunks, interval, expire, events, pre, logger)

	//启动消费者 默认一个http并发度
	workerPool := NewWorkerPool_(QueueBufferSize, util.RoutingInfo{}, handler, inMemory, pre, logger)

	//events 消费
	go ChunkEventLoop_(ctx, chunks, workerPool, uploadInfo, events, done, pre, logger)

	// 4. 启动分片上传
	go StartChunkSubmitLoop_(ctx, chunks, workerPool, uploadInfo,
		false, nil, pre, logger)

	newFileName := uploadInfo.File.NewFileName

	// 5分钟超时定时器
	timeout := 5 * time.Minute
	select {
	case <-done:
		logger.Info("Function 正常完成", slog.String("per", pre), slog.String("newFileName", newFileName))
	case <-time.After(timeout):
		logger.Warn("等待 5 分钟超时，退出等待", slog.String("per", pre),
			slog.String("newFileName", newFileName))
		return fmt.Errorf("等待 5 分钟超时，退出等待, newFileName: %s", newFileName)
	}

	logger.Info("主程序执行完毕", slog.String("per", pre), slog.String("newFileName", newFileName))
	return nil
}

// GetTransferReader 根据不同源类型（GCS/远程磁盘/本地磁盘）获取流式Reader
// 核心功能：抽离原UploadRedirectImp中获取reader的逻辑，解耦且可复用
func GetTransferReader(
	ctx context.Context,
	source SourceInfo,
	file FileInfo,
	localBaseDir string,
	objectName string,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) {
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
			pre,
			logger,
		)
		if err != nil {
			logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			return nil, err
		}

	case RemoteDisk: // 远程磁盘（SSH）源
		remoteDiskSSHConfig := util.SSHConfig{
			User:     source.User,
			Host:     source.Host + ":" + source.SSHPort,
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
			pre,
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
			pre,
			logger,
		)
		if err != nil {
			logger.Error("LocalReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
			return nil, err
		}

	default: // 未知源类型
		return nil, fmt.Errorf("unsupported source type: %s", source.SourceType)
	}

	logger.Info("GetTransferReader success",
		slog.String("pre", pre),
		slog.String("sourceType", source.SourceType),
		slog.String("fileName", file.FileName),
		slog.String("objectName", objectName))

	return reader, nil
}
