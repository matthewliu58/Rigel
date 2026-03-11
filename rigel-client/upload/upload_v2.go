package upload

import (
	"context"
	"fmt"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net/http"
	"os"
	"rigel-client/limit_rate"
	"rigel-client/upload/compose"
	"rigel-client/upload/split"
	"rigel-client/util"
	"strings"
	"time"
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

type ChunkTask struct {
	Ctx        context.Context
	Index      string
	Chunks     *util.SafeMap
	UploadInfo UploadFileInfo
	ObjectName string
}

type WorkerPool struct {
	TaskCh chan ChunkTask
}

type UploadFileInfo struct {
	LocalFilePath string
	BucketName    string
	FileName      string
	CredFile      string
}

// 分片+限流+ack+compose 功能的upload
func UploadToGCSbyReDirectImp(uploadInfo UploadFileInfo, routingInfo util.RoutingInfo,
	pre string, logger *slog.Logger) error {

	logger.Info("UploadToGCSbyReDirectHttpsV2", slog.String("pre", pre),
		slog.Any("uploadInfo", uploadInfo), slog.Any("routingInfo", routingInfo))

	// 定时器控制最大等待时间
	done := make(chan struct{})
	ctx := context.Background()
	localFilePath := uploadInfo.LocalFilePath
	fileName := uploadInfo.FileName

	//获取分片
	chunks := util.NewSafeMap()
	fi, err := os.Stat(localFilePath)
	if err != nil {
		logger.Error("os.Stat failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	size := fi.Size()
	_, err = split.SplitFilebyRange(size, 0, -1, fileName, fileName, chunks, pre, logger)
	if err != nil {
		logger.Error("split.SplitFilebyRange failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	//启动定时重传 & check传输完毕
	events := make(chan ChunkEvent, 100)
	interval := 10 * time.Duration(time.Second)
	expire := 120 * time.Duration(time.Second)
	StartChunkTimeoutChecker(ctx, chunks, interval, expire, events, pre, logger)

	//启动消费者 默认一个http并发度
	workerPool := NewWorkerPool(100, routingInfo, uploadChunkRedirect, pre, logger)

	//events 消费
	go ChunkEventLoop(ctx, chunks, workerPool, uploadInfo, events, done, pre, logger)

	// 4. 启动分片上传
	go StartChunkSubmitLoop(ctx, chunks, workerPool, uploadInfo,
		false, nil, pre, logger)

	// 5分钟超时定时器
	timeout := 5 * time.Minute
	select {
	case <-done:
		logger.Info("FunctionA 正常完成", slog.String("per", pre), fileName)
	case <-time.After(timeout):
		logger.Warn("等待 5 分钟超时，退出等待", slog.String("per", pre),
			slog.String("fileName", fileName))
		return fmt.Errorf("等待 5 分钟超时，退出等待, fileName: %s", fileName)
	}

	logger.Info("主程序执行完毕", slog.String("per", pre), slog.String("fileName", fileName))
	return nil
}

func CollectExpiredChunks(
	s *util.SafeMap,
	expire time.Duration,
	pre string,
	logger *slog.Logger,
) (expired map[string]*split.ChunkState, finished, unfinished bool) {
	now := time.Now()
	expired = make(map[string]*split.ChunkState)
	finished = true // 先假设都 ack 了

	logger.Info("CollectExpiredChunks", slog.String("pre", pre),
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

func StartChunkTimeoutChecker(
	ctx context.Context,
	s *util.SafeMap,
	interval time.Duration,
	expire time.Duration,
	events chan<- ChunkEvent,
	pre string,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)

	logger.Info("定时器启动", slog.String("pre", pre),
		slog.Any("interval", interval), slog.Any("expire", expire))

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				expired, finished, unfinished := CollectExpiredChunks(s, expire, pre, logger)

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

func ChunkEventLoop(ctx context.Context, chunks *util.SafeMap, workerPool *WorkerPool,
	uploadInfo UploadFileInfo, events <-chan ChunkEvent, done chan struct{}, pre string, logger *slog.Logger) {

	logger.Info("事件循环启动", slog.String("pre", pre))

	for {
		select {
		case ev := <-events:
			switch ev.Type {
			case ChunkExpired:
				logger.Warn("超时重传", slog.String("pre", pre), "indexes", ev.Indexes)
				StartChunkSubmitLoop(ctx, chunks, workerPool, uploadInfo, true, ev.Indexes, pre, logger)
			case ChunkFinished:
				var parts = []string{}
				bucketName := uploadInfo.BucketName
				fileName := uploadInfo.FileName
				credFile := uploadInfo.CredFile

				logger.Info("传输完成", slog.String("pre", pre), "fileName", fileName)
				chunks_ := chunks.GetAll()
				for _, v := range chunks_ {
					v_, ok := v.(*split.ChunkState)
					if !ok {
						continue
					}
					if v_.Acked != 2 {
						logger.Error("upload failed", slog.String("pre", pre),
							"fileName", fileName, "index", v_.Index)
						close(done)
						return
					}
					logger.Info("传输完成", slog.String("pre", pre),
						"fileName", fileName, "index", v_.Index, "ObjectName", v_.ObjectName)
					parts = append(parts, v_.ObjectName)
				}
				err := compose.ComposeTree(ctx, bucketName, fileName, credFile, parts, pre, logger)
				if err != nil {
					logger.Error("compose failed", slog.String("pre", pre), "fileName", fileName, "err", err)
				}
				close(done)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func NewWorkerPool(
	queueSize int,
	routingInfo util.RoutingInfo,
	handler func(ChunkTask, string, *rate.Limiter, string, *slog.Logger) error,
	pre string,
	logger *slog.Logger,
) *WorkerPool {
	p := &WorkerPool{
		TaskCh: make(chan ChunkTask, queueSize),
	}

	logger.Info("WorkerPool 启动", slog.String("pre", pre), "queueSize", queueSize)

	workerNum := len(routingInfo.Routing)

	for i := 0; i < workerNum; i++ {
		go func(workerID int, pathInfo util.PathInfo) {

			rate_ := pathInfo.Rate                 //maxMbps
			bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
			limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))

			logger.Info("Worker 启动", slog.String("pre", pre),
				"worker", workerID, "rate", rate_, "hops", pathInfo.Hops)

			for task := range p.TaskCh {

				err := handler(
					task,
					pathInfo.Hops,
					limiter,
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

	return p
}

func (p *WorkerPool) Submit(task ChunkTask) bool {
	select {
	case p.TaskCh <- task:
		//fmt.Println("submit task", task)
		return true
	default:
		// 队列满了，可以选择丢 / 打日志 / 统计
		return false
	}
}

func StartChunkSubmitLoop(
	ctx context.Context,
	chunks *util.SafeMap,
	workerPool *WorkerPool,
	uploadInfo UploadFileInfo,
	resubmit bool,
	resubmitIndexes map[string]*split.ChunkState,
	pre string,
	logger *slog.Logger,
) {
	logger.Info("开始分片上传", slog.String("pre", pre), "fileName", uploadInfo.FileName)
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

		task := ChunkTask{
			Ctx:        ctx,
			Index:      v_.Index,
			Chunks:     chunks,
			UploadInfo: uploadInfo,
			ObjectName: v_.ObjectName,
		}

		if !workerPool.Submit(task) {
			// 队列满了，本轮结束，等下个 tick
			logger.Warn("workerPool full", slog.String("pre", pre))
			time.Sleep(10 * time.Second)
			break
		}
	}

}

func uploadChunkRedirect(task ChunkTask, hops string, rateLimiter *rate.Limiter, pre string, logger *slog.Logger) error {
	ctx := task.Ctx

	logger.Info("开始上传分片", slog.String("pre", pre),
		"fileName", task.UploadInfo.FileName, "index", task.Index, "hops", hops)

	// 1. 生成 access token（和 uploadChunkV2 保持一致）
	jsonBytes, err := os.ReadFile(task.UploadInfo.CredFile)
	if err != nil {
		return fmt.Errorf("read cred file: %w", err)
	}

	creds, err := google.CredentialsFromJSON(
		ctx,
		jsonBytes,
		"https://www.googleapis.com/auth/devstorage.full_control",
	)
	if err != nil {
		return fmt.Errorf("parse credentials: %w", err)
	}

	token, err := creds.TokenSource.Token()
	if err != nil {
		return fmt.Errorf("get token: %w", err)
	}

	// 2. 打开 chunk 文件（或整文件）
	file, err := os.Open(task.UploadInfo.LocalFilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	chunk_, _ := task.Chunks.Get(task.Index)
	chunk := chunk_.(*split.ChunkState)

	// 3. 读取 chunk 内容
	section := io.NewSectionReader(file, chunk.Offset, chunk.Size)

	// 3. 限流 reader
	body := limit_rate.NewRateLimitedReader(ctx, section, rateLimiter)

	// 4. 解析 hops
	hopList := strings.Split(hops, ",")
	if len(hopList) == 0 {
		return fmt.Errorf("invalid X-Hops: %s", hops)
	}
	firstHop := hopList[0]

	// 5. 构造 URL
	url := fmt.Sprintf(
		"http://%s/%s/%s",
		firstHop,
		task.UploadInfo.BucketName,
		task.ObjectName,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Hops", hops)
	req.Header.Set("X-Chunk-Index", "1")
	req.Header.Set("X-Rate-Limit-Enable", "true")

	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	task.Chunks.Set(task.Index, &split.ChunkState{
		Index:      chunk.Index,
		FileName:   chunk.FileName,
		ObjectName: chunk.ObjectName,
		Offset:     chunk.Offset,
		Size:       chunk.Size,
		LastSend:   time.Now(),
		Acked:      1,
	})
	logger.Info("上传分片", slog.String("pre", pre),
		"url", url, "fileName", task.UploadInfo.FileName, "index", task.Index, "hops", hops)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(b))
	}

	// 6. 成功后更新状态（重新 set，不 mutate）
	chunk_, _ = task.Chunks.Get(task.Index)
	chunk = chunk_.(*split.ChunkState)
	task.Chunks.Set(task.Index, &split.ChunkState{
		Index:      chunk.Index,
		FileName:   chunk.FileName,
		ObjectName: chunk.ObjectName,
		Offset:     chunk.Offset,
		Size:       chunk.Size,
		LastSend:   chunk.LastSend,
		Acked:      2,
	})
	logger.Info("上传分片成功", slog.String("pre", pre),
		"fileName", task.UploadInfo.FileName, "index", task.Index, "hops", hops)

	return nil
}
