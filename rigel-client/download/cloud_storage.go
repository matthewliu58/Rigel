package download

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
	"sync"
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

// 默认分片大小（可根据网络/机器调整）
//const defaultChunkSize = 100 * 1024 * 1024 // 100MB

// DownloadFromGCSConcurrent 并发从 GCS 下载文件（支持指定范围 + 范围内分片）
// 参数说明：
//
//	localFilePath: 本地文件保存路径
//	bucketName: GCS bucket 名称
//	objectName: GCS 对象名称
//	credFile: GCS 凭证文件路径
//	start: 读取起始字节（从0开始，完整下载传0）
//	length: 读取字节长度（完整下载传-1，指定范围传具体值）
//	chunkSize: 每个分片的大小（字节，传0则使用默认100MB）
//	concurrency: 并发数（传0则使用默认8）
//	pre: 日志前缀
//	logger: 日志对象
func DownloadFromGCSConcurrent(ctx context.Context, LocalBaseDir, bucketName, objectName, newFilename, credFile string,
	start, length, chunkSize int64, concurrency int, pre string, logger *slog.Logger) (string, error) {

	logger.Info("Downloading file from GCS bucket using concurrent chunks", slog.String("pre", pre),
		slog.String("objectName", objectName), slog.String("LocalBaseDir", LocalBaseDir))

	// 初始化默认值
	chunkSize = util.AutoSelectChunkSize(length)

	if concurrency <= 0 {
		concurrency = 8
	}

	//split := false

	// 日志区分完整下载/分片读取
	if length <= 0 {
		logger.Info("Downloading full file from GCS bucket using client library", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("LocalBaseDir", LocalBaseDir))
	} else {
		logger.Info("Downloading file range from GCS bucket using client library", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("LocalBaseDir", LocalBaseDir),
			slog.Int64("start_byte", start), slog.Int64("length_byte", length))
		//split = true
	}

	//objectName = buildLocalFileName(objectName, start, length, split)
	localFilePath := filepath.Join(LocalBaseDir, newFilename)

	// 设置 GCS 凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建 GCS 客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		return objectName, fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	// 获取对象元数据（主要是文件总大小）
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return objectName, fmt.Errorf("failed to get object attributes: %w", err)
	}
	fileTotalSize := attrs.Size

	// 计算实际下载的范围
	var (
		downloadStart int64 // 实际下载起始位置
		downloadEnd   int64 // 实际下载结束位置（不包含）
		downloadSize  int64 // 实际下载总大小
	)

	if length <= 0 {
		// 完整下载：范围是 0 ~ 文件总大小
		downloadStart = 0
		downloadEnd = fileTotalSize
		downloadSize = fileTotalSize
		logger.Info("Downloading full file with concurrent chunks",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("total_file_size", fileTotalSize),
			slog.Int64("chunk_size", chunkSize),
			slog.Int("concurrency", concurrency))
	} else {
		// 指定范围下载：校验范围合法性
		downloadStart = start
		downloadEnd = start + length

		// 防止结束位置超出文件总大小
		if downloadEnd > fileTotalSize {
			downloadEnd = fileTotalSize
			logger.Warn("Download range exceeds file size, adjust end position",
				slog.String("pre", pre),
				slog.Int64("requested_end", start+length),
				slog.Int64("actual_end", fileTotalSize))
		}
		// 防止起始位置超出文件总大小
		if downloadStart >= fileTotalSize {
			return objectName,
				fmt.Errorf("start position %d exceeds file total size %d", downloadStart, fileTotalSize)
		}

		downloadSize = downloadEnd - downloadStart
		logger.Info("Downloading specified range with concurrent chunks",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("requested_start", start),
			slog.Int64("requested_length", length),
			slog.Int64("actual_start", downloadStart),
			slog.Int64("actual_end", downloadEnd),
			slog.Int64("actual_size", downloadSize),
			slog.Int64("chunk_size", chunkSize),
			slog.Int("concurrency", concurrency))
	}

	// 如果下载大小小于等于分片大小，直接单块下载
	if downloadSize <= chunkSize {
		logger.Info("Download size smaller than chunk size, use single range download", slog.String("pre", pre))
		return DownloadFromGCSbyClient(ctx, localFilePath, bucketName, objectName, newFilename, credFile,
			downloadStart, downloadSize, pre, logger)
	}

	// 创建本地文件（预分配空间）
	localFile, err := os.Create(localFilePath)
	if err != nil {
		return objectName, fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	// 预分配文件空间（如果是完整下载，分配全量大小；如果是范围下载，分配范围大小）
	if err := localFile.Truncate(downloadSize); err != nil {
		return objectName, fmt.Errorf("failed to truncate local file: %w", err)
	}

	// 计算在指定范围内的总分片数
	totalChunks := (downloadSize + chunkSize - 1) / chunkSize // 向上取整
	logger.Info("Split download range into chunks",
		slog.String("pre", pre),
		slog.Int64("total_chunks", totalChunks),
		slog.Int64("chunk_size", chunkSize))

	// 使用 errgroup 管理并发 goroutine（支持错误传播）
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency) // 限制并发数

	// 进度统计
	var (
		completedChunks int64
		mu              sync.Mutex
	)

	// 遍历所有分片，启动并发下载
	for i := int64(0); i < totalChunks; i++ {
		chunkIndex := i // 捕获循环变量

		eg.Go(func() error {
			// 计算当前分片在「全局文件」中的起始/结束位置
			chunkGlobalStart := downloadStart + chunkIndex*chunkSize
			chunkGlobalEnd := chunkGlobalStart + chunkSize

			// 最后一个分片可能不足 chunkSize，修正结束位置
			if chunkGlobalEnd > downloadEnd {
				chunkGlobalEnd = downloadEnd
			}
			chunkLength := chunkGlobalEnd - chunkGlobalStart

			// 计算当前分片在「本地文件」中的写入偏移（本地文件从0开始）
			chunkLocalOffset := chunkGlobalStart - downloadStart

			logger.Debug("Downloading chunk",
				slog.String("pre", pre),
				slog.Int64("chunk_index", chunkIndex),
				slog.Int64("global_start", chunkGlobalStart),
				slog.Int64("global_end", chunkGlobalEnd),
				slog.Int64("chunk_length", chunkLength),
				slog.Int64("local_offset", chunkLocalOffset))

			// 创建分片 reader（读取GCS上的指定范围）
			rc, err := obj.NewRangeReader(ctx, chunkGlobalStart, chunkLength)
			if err != nil {
				return fmt.Errorf("chunk %d: failed to create range reader: %w", chunkIndex, err)
			}
			defer rc.Close()

			// 定位到本地文件的指定偏移位置写入
			_, err = localFile.Seek(chunkLocalOffset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("chunk %d: failed to seek file: %w", chunkIndex, err)
			}

			// 写入分片数据
			written, err := io.Copy(localFile, rc)
			if err != nil {
				return fmt.Errorf("chunk %d: failed to write data: %w", chunkIndex, err)
			}

			if written != chunkLength {
				return fmt.Errorf("chunk %d: written size mismatch (expected %d, got %d)", chunkIndex, chunkLength, written)
			}

			// 更新进度
			mu.Lock()
			completedChunks++
			progress := float64(completedChunks) / float64(totalChunks) * 100
			mu.Unlock()

			logger.Debug("Chunk download completed",
				slog.String("pre", pre),
				slog.Int64("chunk_index", chunkIndex),
				slog.Float64("progress", progress))

			return nil
		})
	}

	// 等待所有 goroutine 完成
	if err := eg.Wait(); err != nil {
		return objectName, fmt.Errorf("concurrent download failed: %w", err)
	}

	// 日志反馈结果
	if length <= 0 {
		logger.Info("Full file concurrent download success",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("total_file_size", downloadSize))
	} else {
		logger.Info("Range concurrent download success",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", downloadStart),
			slog.Int64("end_byte", downloadEnd),
			slog.Int64("total_size", downloadSize))
	}

	return objectName, nil
}
