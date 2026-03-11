package upload

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
