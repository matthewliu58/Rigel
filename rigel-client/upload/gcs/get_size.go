package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

type GetSize struct {
	bucketName string
	credFile   string
}

func NewGetSize(
	bucketName, credFile string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *GetSize {
	gs := &GetSize{
		bucketName: bucketName,
		credFile:   credFile,
	}
	// 和NewDownload/NewUpload完全一致的日志打印逻辑
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {

	objectName := filename

	// 2. 监听ctx取消信号，提前终止操作
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get csg file size canceled: %w", ctx.Err())
		logger.Error("GetGCSObjectSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 2. 设置 GCS 凭证环境变量
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", g.credFile)

	// 3. 创建 GCS 客户端（带超时控制）
	ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute) // 避免卡住
	defer cancel()

	client, err := storage.NewClient(ctx_)
	if err != nil {
		logger.Error("创建 GCS 客户端失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return 0, fmt.Errorf("storage.NewClient failed: %w", err)
	}
	defer client.Close() // 确保客户端关闭，释放资源

	// 4. 获取 Bucket 和 Object 实例
	bucket := client.Bucket(g.bucketName)
	obj := bucket.Object(objectName)

	// 5. 获取 Object 元数据（核心：从 Attrs 中读取 Size）
	attrs, err := obj.Attrs(ctx_)
	if err != nil {
		logger.Error("获取 GCS Object 元数据失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		// 区分常见错误类型，返回更友好的提示
		if errors.Is(err, storage.ErrObjectNotExist) {
			return 0, fmt.Errorf("object %s/%s 不存在: %w", g.bucketName, objectName, err)
		}
		return 0, fmt.Errorf("obj.Attrs failed: %w", err)
	}

	// 6. 日志记录结果
	logger.Info("成功获取 GCS Object 大小", slog.String("pre", pre),
		slog.String("bucketName", g.bucketName),
		slog.String("objectName", objectName),
		slog.Int64("file_size_bytes", attrs.Size),
		slog.String("file_size_human", formatBytes(attrs.Size))) // 可选：格式化易读大小

	// 7. 返回文件大小（字节）
	return attrs.Size, nil
}

// formatBytes 将字节数转换为易读的字符串（如 1024 → 1KB，1048576 → 1MB）
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(bytes)/float64(div),
		"KMGTPE"[exp])
}
