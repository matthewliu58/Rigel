package download

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

// DownloadFromGCSbyClient 从 GCS bucket 下载文件（支持完整下载/分片读取）
// 参数说明：
//
//	start: 读取起始字节（从0开始，完整下载传0）
//	length: 读取字节长度（完整下载传-1，分片读取传具体值如10*1024*1024*1024）
func DownloadFromGCSbyClient(ctx context.Context, LocalBaseDir, bucketName, objectName, newFileName, credFile string,
	start, length int64, pre string, logger *slog.Logger) (string, error) {

	//split := false

	// 日志区分完整下载/分片读取
	if length <= 0 {
		logger.Info("Downloading full file from GCS current bucket using client library",
			slog.String("pre", pre), slog.String("objectName", objectName),
			slog.String("objectName", objectName), slog.String("LocalBaseDir", LocalBaseDir))
	} else {
		logger.Info("Downloading file range from GCS current bucket using client library",
			slog.String("pre", pre), slog.String("objectName", objectName),
			slog.String("newFileName", newFileName), slog.String("LocalBaseDir", LocalBaseDir),
			slog.Int64("start_byte", start), slog.Int64("length_byte", length))
		//split = true
	}

	//objectName = buildLocalFileName(objectName, start, length, split)
	localFilePath := filepath.Join(LocalBaseDir, newFileName)

	// 设置凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		return objectName, fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	// 获取 bucket 和 object
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	// 创建 reader（核心改动：支持范围读取）
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var rc *storage.Reader
	if length <= 0 {
		// 完整下载：使用原 NewReader
		rc, err = obj.NewReader(ctx)
	} else {
		// 分片读取：使用 NewRangeReader（start=起始字节，length=读取长度）
		rc, err = obj.NewRangeReader(ctx, start, length)
	}
	if err != nil {
		return objectName, fmt.Errorf("failed to create object reader: %w", err)
	}
	defer rc.Close()

	// 创建本地文件（分片读取建议文件名带范围，如 bigfile_0_10GB.bin）
	f, err := os.Create(localFilePath)
	if err != nil {
		return objectName, fmt.Errorf("failed to create local file: %w", err)
	}
	defer f.Close()

	// 写入本地文件
	if _, err := io.Copy(f, rc); err != nil {
		return objectName, fmt.Errorf("failed to copy object to local file: %w", err)
	}

	// 日志反馈结果
	if length <= 0 {
		logger.Info("Full file download success", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("localFilePath", localFilePath))
	} else {
		logger.Info("File range download success", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", start), slog.Int64("length_byte", length))
	}

	return objectName, nil
}
