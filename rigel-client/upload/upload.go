package upload

import (
	"context"
	"golang.org/x/time/rate"
	"log/slog"
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
