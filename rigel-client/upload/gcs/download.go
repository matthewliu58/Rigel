package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

type Download struct {
	localBaseDir string
	bucketName   string
	credFile     string
}

func NewDownload(
	localBaseDir, bucketName, credFile string, pre string, logger *slog.Logger,
) *Download {
	d := &Download{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		credFile:     credFile,
	}
	logger.Info("NewDownload", slog.String("pre", pre), slog.Any("Download", *d))
	return d
}

func (d *Download) DownloadFile(
	ctx context.Context,
	filename string,
	newFilename string,
	start int64,
	length int64,
	bs string,
	inMemory bool, // 新增：true=不落盘返回Reader，false=落盘
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) { // 返回io.ReadCloser（兼容两种模式）

	objectName := filename

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("DownloadFromGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// 日志区分模式+完整/分片读取
	if inMemory {
		if length <= 0 {
			logger.Info("Reading full file from GCS (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName))
		} else {
			logger.Info("Reading file range from GCS (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	} else {
		if length <= 0 {
			logger.Info("Downloading full file from GCS (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("LocalBaseDir", d.localBaseDir))
		} else {
			logger.Info("Downloading file range from GCS (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("newFileName", newFilename),
				slog.String("LocalBaseDir", d.localBaseDir),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	}

	// 设置GCS凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", d.credFile)

	// 创建GCS客户端

	// 创建带超时的上下文
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
	//defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create storage client failed: %w", err)
	}

	// 获取Bucket和Object
	bucket := client.Bucket(d.bucketName)
	obj := bucket.Object(objectName)

	// 创建Reader（完整/分片读取）
	var rc *storage.Reader
	if length <= 0 {
		rc, err = obj.NewReader(ctx)
	} else {
		rc, err = obj.NewRangeReader(ctx, start, length)
	}
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create object reader failed: %w", err)
	}

	// 模式1：inMemory=true → 返回流式Reader（不落盘）
	if inMemory {
		return &gcsReaderWrapper{
			Reader: rc,
			//cancel: cancel,
			client: client,
		}, nil
	}

	// 模式2：inMemory=false → 落盘到本地文件
	defer func() {
		rc.Close()
		//cancel()
		client.Close()
	}()

	// 拼接本地文件路径
	localFilePath := filepath.Join(d.localBaseDir, newFilename)
	// 创建本地目录
	if err := os.MkdirAll(d.localBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("create local dir failed: %w", err)
	}
	// 创建本地文件
	f, err := os.Create(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("create local file failed: %w", err)
	}
	defer f.Close()

	// 写入本地文件
	if _, err := io.Copy(f, rc); err != nil {
		return nil, fmt.Errorf("copy to local file failed: %w", err)
	}

	// 落盘模式返回本地文件的Reader（方便调用方后续读取）
	localFileReader, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("open local file failed: %w", err)
	}

	// 日志反馈结果
	if length <= 0 {
		logger.Info("Full file download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath))
	} else {
		logger.Info("File range download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", start),
			slog.Int64("length_byte", length))
	}

	return localFileReader, nil
}

// gcsReaderWrapper 封装 storage.Reader + 资源清理逻辑（内存模式用）
type gcsReaderWrapper struct {
	*storage.Reader
	//cancel context.CancelFunc
	client *storage.Client
}

// Close 关闭所有关联资源（调用方必须调用）
func (w *gcsReaderWrapper) Close() error {
	var errStr []string
	if err := w.Reader.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("reader close failed: %v", err))
	}
	//w.cancel()
	if err := w.client.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("client close failed: %v", err))
	}
	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
