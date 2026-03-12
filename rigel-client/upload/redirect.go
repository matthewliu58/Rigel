package upload

import (
	"fmt"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/limit_rate"
	"rigel-client/upload/split"
	upload2 "rigel-client/upload/upload"
	"strings"
	"time"
)

func UploadRedirectImp(task ChunkTask_, hops string, rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	logger.Info("UploadRedirectImp", slog.String("pre", pre), slog.Any("task", task))

	// --------------- 第一步：初始状态设置（Acked=1）---------------
	// 先获取当前分片的基础信息（避免空指针）
	chunkVal, ok := task.Chunks.Get(task.Index)
	if !ok {
		err := fmt.Errorf("chunk index %d not found in Chunks map", task.Index)
		logger.Error("get chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	chunk, ok := chunkVal.(*split.ChunkState)
	if !ok {
		err := fmt.Errorf("chunk index %d type is not *split.ChunkState", task.Index)
		logger.Error("chunk type assert failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// 初始状态：标记为开始传输（Acked=1）
	initialChunkState := &split.ChunkState{
		Index:       chunk.Index,
		FileName:    chunk.FileName,
		NewFileName: chunk.NewFileName,
		ObjectName:  chunk.ObjectName,
		Offset:      chunk.Offset,
		Size:        chunk.Size,
		LastSend:    time.Now(),
		Acked:       1, // 1=开始传输
	}
	task.Chunks.Set(task.Index, initialChunkState)
	logger.Info("set chunk initial state", slog.String("pre", pre), slog.String("index", task.Index), slog.Int("acked", 1))

	// 定义defer函数：异常时统一设置Acked=0（兜底）
	var finalErr error
	defer func() {
		if finalErr != nil {
			// 出错时：更新状态为失败（Acked=0）
			errorChunkState := &split.ChunkState{
				Index:       chunk.Index,
				FileName:    chunk.FileName,
				NewFileName: chunk.NewFileName,
				ObjectName:  chunk.ObjectName,
				Offset:      chunk.Offset,
				Size:        chunk.Size,
				LastSend:    initialChunkState.LastSend,
				Acked:       0, // 0=传输失败
			}
			task.Chunks.Set(task.Index, errorChunkState)
			logger.Error("chunk transfer failed, set acked=0", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		}
	}()

	// --------------- 第二步：获取Reader（读取源文件）---------------
	ctx := task.Ctx
	source_ := task.Source
	file := task.File
	dest := task.Dest

	reader, err := GetTransferReader(ctx, source_, file, task.LocalBaseDir, task.ObjectName, inMemory, pre, logger)
	if err != nil {
		return err
	}
	defer func() {
		if reader != nil {
			_ = reader.Close() // 确保Reader关闭，无论成功/失败
		}
	}()

	logger.Info("download object success", slog.String("pre", pre), slog.String("objectName", task.ObjectName))

	// --------------- 第三步：上传到目标端 ---------------
	if dest.DestType == GCPCLoud {
		if err := upload2.UploadToGCSbyClient(ctx, task.LocalBaseDir, dest.BucketName,
			task.ObjectName, dest.CredFile, inMemory, reader, pre, logger); err != nil {
			logger.Error("UploadToGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	} else if dest.DestType == RemoteDisk {

	}

	// --------------- 第四步：成功状态更新（Acked=2）---------------
	successChunkState := &split.ChunkState{
		Index:       chunk.Index,
		FileName:    chunk.FileName,
		NewFileName: chunk.NewFileName,
		ObjectName:  chunk.ObjectName,
		Offset:      chunk.Offset,
		Size:        chunk.Size,
		LastSend:    initialChunkState.LastSend, // 保留开始传输时间
		Acked:       2,                          // 2=传输成功
	}
	task.Chunks.Set(task.Index, successChunkState)
	logger.Info("chunk transfer success, set acked=2", slog.String("pre", pre), slog.String("index", task.Index))

	logger.Info("ClientUploadHandler success", slog.String("pre", pre))
	return nil
}

func UploadToGCSbyProxy(task ChunkTask_, hops string, rateLimiter *rate.Limiter,
	reader io.ReadCloser, inMemory bool, pre string, logger *slog.Logger) error {

	logger.Info("UploadToGCSbyProxy", slog.String("pre", pre), slog.Any("task", task),
		slog.String("hops", hops), slog.Bool("inMemory", inMemory))

	ctx := task.Ctx
	dest := task.Dest
	var reader_ io.ReadCloser = reader

	// 2. 打开 chunk 文件（或整文件）
	if !inMemory {
		// 模式2：inMemory=false → 从本地文件上传（原有逻辑）
		localFilePath := filepath.Join(task.LocalBaseDir, task.ObjectName) // 修复路径拼接

		// 打开本地文件（错误日志加pre）
		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("Failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		defer f.Close()
		reader_ = f
	}

	// 3. 限流 reader
	body := limit_rate.NewRateLimitedReader(ctx, reader_, rateLimiter)

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
		dest.BucketName,
		task.ObjectName,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	// 1. 生成 access token（和 uploadChunkV2 保持一致）
	jsonBytes, err := os.ReadFile(dest.CredFile)
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

	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Hops", hops)
	req.Header.Set("X-Chunk-Index", "1")
	req.Header.Set("X-Rate-Limit-Enable", "true")

	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(b))
	}

	return nil
}
