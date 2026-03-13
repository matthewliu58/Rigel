package upload

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/limit_rate"
	"strings"
	"time"
)

// UploadToGCSbyClient 上传数据到GCS（支持本地文件/内存流式两种模式，含pre日志前缀）
// 参数说明：
//
//	inMemory: true=从reader读取数据（内存流式上传），false=从本地文件读取上传
//	dataReader: inMemory=true时传入待上传的Reader（如storage.Reader/bytes.Reader等），false时传nil
//	pre: 日志前缀（用于定位请求/任务）
//	其他参数保持原有含义
func UploadToGCSbyClient(
	ctx context.Context,
	LocalBaseDir, bucketName, objectName, credFile string,
	inMemory bool, // 新增：true=内存流式上传，false=本地文件上传
	dataReader io.Reader, // 新增：inMemory=true时传入的数据源Reader
	pre string, // 补充：日志前缀（关键追溯字段）
	logger *slog.Logger,
) error {
	// 日志区分上传模式（添加pre前缀）
	if inMemory {
		logger.Info("Uploading data to GCS (in-memory mode, no local file)",
			slog.String("pre", pre), // 核心：添加日志前缀
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to GCS (disk mode)",
			slog.String("pre", pre), // 核心：添加日志前缀
			slog.String("LocalBaseDir", LocalBaseDir),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
	}

	// 设置GCS凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建GCS客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client",
			slog.String("pre", pre), // 错误日志也加pre
			slog.Any("err", err))
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	// 获取bucket handle
	bucket := client.Bucket(bucketName)

	// 创建带超时的上传上下文
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// 初始化GCS Writer
	wc := bucket.Object(objectName).NewWriter(ctx)
	wc.StorageClass = "STANDARD"
	wc.ContentType = "application/octet-stream"
	defer func() {
		// 确保Writer关闭，即使上传失败（错误日志加pre）
		if err := wc.Close(); err != nil {
			logger.Error("Failed to close GCS writer",
				slog.String("pre", pre),
				slog.Any("err", err))
		}
	}()

	// 模式1：inMemory=true → 从传入的Reader流式上传（不落盘）
	if inMemory {
		if dataReader == nil {
			logger.Error("In-memory mode requires non-nil dataReader",
				slog.String("pre", pre))
			return fmt.Errorf("in-memory mode requires non-nil dataReader")
		}

		// 从Reader拷贝数据到GCS
		if _, err := io.Copy(wc, dataReader); err != nil {
			logger.Error("Failed to copy in-memory data to bucket",
				slog.String("pre", pre),
				slog.Any("err", err))
			return fmt.Errorf("failed to copy in-memory data to bucket: %w", err)
		}

		logger.Info("In-memory upload success",
			slog.String("pre", pre), // 成功日志加pre
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
		return nil
	}

	// 模式2：inMemory=false → 从本地文件上传（原有逻辑）
	localFilePath := filepath.Join(LocalBaseDir, objectName) // 修复路径拼接

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

	// 从本地文件拷贝到GCS（错误日志加pre）
	if _, err := io.Copy(wc, f); err != nil {
		logger.Error("Failed to copy local file to bucket",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Any("err", err))
		return fmt.Errorf("failed to copy local file to bucket: %w", err)
	}

	logger.Info("Local file upload success",
		slog.String("pre", pre), // 成功日志加pre
		slog.String("localFilePath", localFilePath),
		slog.String("bucketName", bucketName),
		slog.String("objectName", objectName))

	return nil
}

// UploadToGCSbyProxy 通过代理向GCS上传文件/分片（修复后）
// 核心功能：支持内存流式上传（inMemory=true）和本地文件上传（inMemory=false），带限流和GCP鉴权
func UploadToGCSbyProxy(
	task ChunkTask,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) error {
	logger.Info("UploadToGCSbyProxy start",
		slog.String("pre", pre),
		slog.Any("task", task),
		slog.String("hops", hops),
		slog.Bool("inMemory", inMemory))

	// ---------------------- 1. 基础参数校验（防空指针） ----------------------
	if task.Dest.BucketName == "" {
		err := fmt.Errorf("dest bucket name is empty")
		logger.Error("invalid dest config", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if task.Dest.CredFile == "" {
		err := fmt.Errorf("dest cred file path is empty")
		logger.Error("invalid dest config", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	ctx := task.Ctx
	dest := task.Dest
	var proxyReader io.ReadCloser = reader

	// 定义资源关闭defer（统一释放所有Reader）
	defer func() {
		if proxyReader != nil && proxyReader != reader {
			_ = proxyReader.Close() // 关闭本地文件Reader
		}
		// 外部传入的reader由调用方负责关闭，此处不主动关闭（避免重复关闭）
	}()

	// ---------------------- 2. 选择上传源：内存流 / 本地文件 ----------------------
	if !inMemory {
		// 模式1：inMemory=false → 从本地文件读取
		localFilePath := filepath.Join(task.LocalBaseDir, task.ObjectName)
		localFilePath = filepath.Clean(localFilePath) // 标准化路径（防多斜杠）

		logger.Info("prepare to read local file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))

		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		proxyReader = f // 替换为本地文件Reader
		logger.Info("local file opened successfully",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))
	} else {
		// 模式2：inMemory=true → 使用外部传入的内存Reader
		if proxyReader == nil {
			err := fmt.Errorf("inMemory=true but reader is nil")
			logger.Error("invalid reader", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		logger.Info("use in-memory reader for upload", slog.String("pre", pre))
	}

	// ---------------------- 3. 限流包装Reader ----------------------
	rateLimitedBody := limit_rate.NewRateLimitedReader(ctx, proxyReader, rateLimiter)
	logger.Info("rate limiter applied to reader", slog.String("pre", pre))

	// ---------------------- 4. 解析hops并构造URL ----------------------
	hopList := strings.Split(hops, ",")
	if len(hopList) == 0 {
		err := fmt.Errorf("invalid X-Hops: %s (split empty)", hops)
		logger.Error("parse hops failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	firstHop := hopList[0]

	url := fmt.Sprintf(
		"http://%s/%s/%s",
		firstHop,
		dest.BucketName,
		task.ObjectName,
	)
	logger.Info("construct upload URL",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("firstHop", firstHop))

	// ---------------------- 5. 生成GCP Access Token ----------------------
	logger.Info("start to generate GCP access token", slog.String("pre", pre))
	jsonBytes, err := os.ReadFile(dest.CredFile)
	if err != nil {
		logger.Error("read cred file failed",
			slog.String("pre", pre),
			slog.String("credFile", dest.CredFile),
			slog.Any("err", err))
		return fmt.Errorf("read cred file: %w", err)
	}

	creds, err := google.CredentialsFromJSON(
		ctx,
		jsonBytes,
		"https://www.googleapis.com/auth/devstorage.full_control",
	)
	if err != nil {
		logger.Error("parse GCP credentials failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("parse credentials: %w", err)
	}

	token, err := creds.TokenSource.Token()
	if err != nil {
		logger.Error("get GCP token failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("get token: %w", err)
	}
	logger.Info("GCP access token generated successfully", slog.String("pre", pre))

	// ---------------------- 6. 构造并发送HTTP请求 ----------------------
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Hops", hops)
	req.Header.Set("X-Chunk-Index", "1")
	req.Header.Set("X-Rate-Limit-Enable", "true")
	req.Header.Set("X-Source-Type", task.Source.SourceType)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	logger.Info("send HTTP request to proxy",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("timeout", "5m"))

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("HTTP request failed",
			slog.String("pre", pre),
			slog.String("url", url),
			slog.Any("err", err))
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close() // 确保响应体关闭

	// ---------------------- 7. 校验响应状态 ----------------------
	if resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("failed to read response body")
			logger.Error("read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("upload to GCS by proxy failed",
			slog.String("pre", pre),
			slog.Int("statusCode", resp.StatusCode),
			slog.String("response", string(respBody)))
		return err
	}

	logger.Info("UploadToGCSbyProxy success",
		slog.String("pre", pre),
		slog.String("objectName", task.ObjectName),
		slog.String("url", url))

	return nil
}
