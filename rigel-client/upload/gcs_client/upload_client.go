package gcs_client

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

type Upload struct {
	localBaseDir string
	bucketName   string
	credFile     string
}

// NewUpload 仿照统一风格初始化新版Upload结构体
func NewUpload(
	localBaseDir, bucketName, credFile string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		credFile:     credFile,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool, // 新增：true=内存流式上传，false=本地文件上传
	pre string, // 补充：日志前缀（关键追溯字段）
	logger *slog.Logger,
) error {

	logger.Info("UploadToGCSbyClient", slog.String("pre", pre), slog.String("objectName", objectName))
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
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to GCS (disk mode)",
			slog.String("pre", pre),
			slog.String("LocalBaseDir", u.localBaseDir),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	// 设置GCS凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", u.credFile)

	// 创建GCS客户端（传入ctx，支持取消客户端创建过程）
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute) // 避免卡住
	//defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close() // 确保客户端关闭

	// 获取bucket handle
	bucket := client.Bucket(u.bucketName)

	// 初始化GCS Writer（传入外层ctx，不重新创建超时）
	wc := bucket.Object(objectName).NewWriter(ctx)
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
		if reader == nil {
			err := fmt.Errorf("in-memory mode requires non-nil dataReader")
			logger.Error("In-memory mode invalid", slog.String("pre", pre), slog.Any("err", err))
			return err
		}

		// 保留原阻塞式io.Copy，等待拷贝完成
		if _, err := io.Copy(wc, reader); err != nil {
			logger.Error("Failed to copy in-memory data to bucket",
				slog.String("pre", pre),
				slog.Any("err", err))
			return fmt.Errorf("failed to copy in-memory data to bucket: %w", err)
		}

		logger.Info("In-memory upload success",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
		return nil
	}

	// 模式2：inMemory=false → 从本地文件上传
	localFilePath := filepath.Join(u.localBaseDir, objectName)

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
		slog.String("bucketName", u.bucketName),
		slog.String("objectName", objectName))

	return nil
}
