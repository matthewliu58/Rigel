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
	"rigel-client/util"
	"strings"
	"time"
)

const (
	gcpScopes = "https://www.googleapis.com/auth/devstorage.full_control"
	gcpCred   = "GOOGLE_APPLICATION_CREDENTIALS"
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

	logger.Info("UploadToGCSbyClient", slog.String("pre", pre))
	// 仅在「上传开始前」检查ctx是否已取消（避免启动无效上传）
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 日志区分上传模式（添加pre前缀）
	if inMemory {
		logger.Info("Uploading data to GCS (in-memory mode, no local file)",
			slog.String("pre", pre),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to GCS (disk mode)",
			slog.String("pre", pre),
			slog.String("LocalBaseDir", LocalBaseDir),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
	}

	// 设置GCS凭证
	os.Setenv(gcpCred, credFile)

	// 创建GCS客户端（传入ctx，支持取消客户端创建过程）
	ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute) // 避免卡住
	defer cancel()
	client, err := storage.NewClient(ctx_)
	if err != nil {
		logger.Error("Failed to create storage client",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close() // 确保客户端关闭

	// 获取bucket handle
	bucket := client.Bucket(bucketName)

	// 初始化GCS Writer（传入外层ctx，不重新创建超时）
	wc := bucket.Object(objectName).NewWriter(ctx_)
	wc.StorageClass = "STANDARD"
	wc.ContentType = "application/octet-stream"
	defer func() {
		// 确保Writer关闭，捕获关闭错误
		if err := wc.Close(); err != nil {
			logger.Error("Failed to close GCS writer",
				slog.String("pre", pre),
				slog.Any("err", err))
		}
	}()

	// 模式1：inMemory=true → 从传入的Reader流式上传
	if inMemory {
		if dataReader == nil {
			err := fmt.Errorf("in-memory mode requires non-nil dataReader")
			logger.Error("In-memory mode invalid", slog.String("pre", pre), slog.Any("err", err))
			return err
		}

		// 保留原阻塞式io.Copy，等待拷贝完成
		if _, err := io.Copy(wc, dataReader); err != nil {
			logger.Error("Failed to copy in-memory data to bucket",
				slog.String("pre", pre),
				slog.Any("err", err))
			return fmt.Errorf("failed to copy in-memory data to bucket: %w", err)
		}

		logger.Info("In-memory upload success",
			slog.String("pre", pre),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName))
		return nil
	}

	// 模式2：inMemory=false → 从本地文件上传
	localFilePath := filepath.Join(LocalBaseDir, objectName)

	// 打开本地文件
	f, err := os.Open(localFilePath)
	if err != nil {
		logger.Error("Failed to open local file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Any("err", err))
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer f.Close()

	// 保留原阻塞式io.Copy，等待拷贝完成
	if _, err := io.Copy(wc, f); err != nil {
		logger.Error("Failed to copy local file to bucket",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Any("err", err))
		return fmt.Errorf("failed to copy local file to bucket: %w", err)
	}

	logger.Info("Local file upload success",
		slog.String("pre", pre),
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
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyProxy canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

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

	reds, err := google.CredentialsFromJSON(ctx, jsonBytes, gcpScopes)
	if err != nil {
		logger.Error("Parse GCP credentials failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("parse credentials: %w", err)
	}

	token, err := reds.TokenSource.Token()
	if err != nil {
		logger.Error("Get GCP token failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("get token: %w", err)
	}
	logger.Info("GCP access token generated successfully", slog.String("pre", pre))

	// ---------------------- 6. 构造并发送HTTP请求 ----------------------
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("Create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderXSourceType, task.Source.SourceType)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{Timeout: 1 * time.Minute}
	logger.Info("Send HTTP request to proxy",
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
			logger.Error("Read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("Upload to GCS by proxy failed",
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
