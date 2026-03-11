package upload

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

// UploadToGCS 上传本地文件到指定 GCS bucket
func UploadToGCSbyClient(ctx context.Context, LocalBaseDir, bucketName, objectName, credFile string,
	logger *slog.Logger) error {

	logger.Info("Uploading file to GCS bucket using client library", LocalBaseDir, objectName)

	localFilePath := LocalBaseDir + objectName

	// 使用环境变量配置凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	// 打开本地文件
	f, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer f.Close()

	// 获取 bucket handle
	bucket := client.Bucket(bucketName)

	// 上传文件
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	wc := bucket.Object(objectName).NewWriter(ctx)
	wc.StorageClass = "STANDARD"
	wc.ContentType = "application/octet-stream"

	if _, err := io.Copy(wc, f); err != nil {
		return fmt.Errorf("failed to copy file to bucket: %w", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	logger.Info("Upload success client library", localFilePath, objectName)

	return nil
}
