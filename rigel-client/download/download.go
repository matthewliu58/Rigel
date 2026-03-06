package download

import (
	"bufio"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DownloadFromGCSbyClient 从 GCS bucket 下载文件（支持完整下载/分片读取）
// 参数说明：
//
//	start: 读取起始字节（从0开始，完整下载传0）
//	length: 读取字节长度（完整下载传-1，分片读取传具体值如10*1024*1024*1024）
func DownloadFromGCSbyClient(ctx context.Context, localFilePath, bucketName, objectName, credFile string,
	start, length int64, pre string, logger *slog.Logger) error {

	// 日志区分完整下载/分片读取
	if length <= 0 {
		logger.Info("Downloading full file from GCS bucket using client library", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("localFilePath", localFilePath))
	} else {
		logger.Info("Downloading file range from GCS bucket using client library", slog.String("pre", pre),
			slog.String("objectName", objectName), slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", start), slog.Int64("length_byte", length))
	}

	// 设置凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
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
		return fmt.Errorf("failed to create object reader: %w", err)
	}
	defer rc.Close()

	// 创建本地文件（分片读取建议文件名带范围，如 bigfile_0_10GB.bin）
	f, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer f.Close()

	// 写入本地文件
	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("failed to copy object to local file: %w", err)
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

	return nil
}

// 默认分片大小（可根据网络/机器调整）
const defaultChunkSize = 100 * 1024 * 1024 // 100MB

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
func DownloadFromGCSConcurrent(ctx context.Context, localFilePath, bucketName, objectName, credFile string,
	start, length, chunkSize int64, concurrency int, pre string, logger *slog.Logger) error {

	// 初始化默认值
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	if concurrency <= 0 {
		concurrency = 8
	}

	// 设置 GCS 凭证
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 创建 GCS 客户端
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	// 获取对象元数据（主要是文件总大小）
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
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
			return fmt.Errorf("start position %d exceeds file total size %d", downloadStart, fileTotalSize)
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
		return DownloadFromGCSbyClient(ctx, localFilePath, bucketName, objectName, credFile,
			downloadStart, downloadSize, pre, logger)
	}

	// 创建本地文件（预分配空间）
	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	// 预分配文件空间（如果是完整下载，分配全量大小；如果是范围下载，分配范围大小）
	if err := localFile.Truncate(downloadSize); err != nil {
		return fmt.Errorf("failed to truncate local file: %w", err)
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
		return fmt.Errorf("concurrent download failed: %w", err)
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

	return nil
}

// SSHConfig 定义SSH连接配置
type SSHConfig struct {
	User     string // 用户名
	Host     string // 主机IP:端口（如192.168.1.20:22）
	Password string // 密码（或用密钥认证）
}

// SSHDDReadRangeChunk 下载指定范围的单个分片（核心函数）
// 参数说明：
//
//	rangeStart: 目标范围的起始字节（比如20GB）
//	rangeLength: 目标范围的总长度（比如10GB）
//	chunkIndex: 分片索引（用于命名和日志）
func SSHDDReadRangeChunk(cfg SSHConfig, remoteFile, localDir string, bs string,
	rangeStart, rangeLength int64, chunkIndex, chunksPerGoroutine int) (string, error) {

	// 1. 先解析bs对应的字节数（比如1G=1073741824字节）
	bsBytes, err := getBsInBytes(cfg, bs)
	if err != nil {
		return "", fmt.Errorf("分片%d：解析块大小失败：%v", chunkIndex, err)
	}

	// 2. 计算当前分片在「目标范围」内的偏移
	// 比如：目标范围是20GB~30GB（10GB），bs=1G，chunkIndex=2 → 偏移2GB → 对应远端skip=22
	chunkOffsetInRange := int64(chunkIndex*chunksPerGoroutine) * bsBytes
	if chunkOffsetInRange >= rangeLength {
		return "", nil // 超出目标范围，无需处理
	}

	// 3. 计算当前分片的实际读取长度（最后一个分片可能不足）
	chunkLength := int64(chunksPerGoroutine) * bsBytes
	if chunkOffsetInRange+chunkLength > rangeLength {
		chunkLength = rangeLength - chunkOffsetInRange
	}

	// 4. 转换为dd的skip和count参数
	// skip = 目标范围起始位置 / bs字节数 + 分片在范围内的偏移 / bs字节数
	skip := (rangeStart / bsBytes) + (chunkOffsetInRange / bsBytes)
	// count = 分片长度 / bs字节数（向上取整）
	count := (chunkLength + bsBytes - 1) / bsBytes

	if count <= 0 {
		return "", nil
	}

	// 5. 构造分片文件名
	localFile := filepath.Join(localDir, fmt.Sprintf("range_chunk_%d.bin", chunkIndex))
	log.Printf("分片%d：目标范围[%d, %d) → 远端skip=%d, count=%d, bs=%s",
		chunkIndex, rangeStart+chunkOffsetInRange, rangeStart+chunkOffsetInRange+chunkLength,
		skip, count, bs)

	// 6. 配置SSH客户端
	sshConfig := &ssh.ClientConfig{
		User: cfg.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 测试用，生产需替换
		Timeout:         30 * time.Second,
	}

	// 7. 建立SSH连接
	client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
	if err != nil {
		return "", fmt.Errorf("分片%d：SSH连接失败：%v", chunkIndex, err)
	}
	defer client.Close()

	// 8. 创建会话+执行dd命令
	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("分片%d：创建会话失败：%v", chunkIndex, err)
	}
	defer session.Close()

	ddCmd := fmt.Sprintf("dd if=%s bs=%s count=%d skip=%d", remoteFile, bs, count, skip)
	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("分片%d：获取stdout失败：%v", chunkIndex, err)
	}

	// 9. 启动dd命令
	if err := session.Start(ddCmd); err != nil {
		return "", fmt.Errorf("分片%d：启动dd失败：%v", chunkIndex, err)
	}

	// 10. 确保本地目录存在
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return "", fmt.Errorf("分片%d：创建目录失败：%v", chunkIndex, err)
	}

	// 11. 写入本地分片文件
	localFd, err := os.Create(localFile)
	if err != nil {
		return "", fmt.Errorf("分片%d：创建文件失败：%v", chunkIndex, err)
	}
	defer localFd.Close()

	bufWriter := bufio.NewWriter(localFd)
	defer bufWriter.Flush()
	_, err = io.Copy(bufWriter, stdout)
	if err != nil {
		return "", fmt.Errorf("分片%d：写入文件失败：%v", chunkIndex, err)
	}

	// 12. 等待命令完成
	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("分片%d：dd执行失败：%v", chunkIndex, err)
	}

	log.Printf("分片%d：下载完成 → %s", chunkIndex, localFile)
	return localFile, nil
}

// SSHDDReadRangeConcurrent 并发下载指定start/length范围的文件
// 参数说明：
//
//	start: 读取起始字节（比如20*1024*1024*1024 → 20GB）
//	length: 读取长度（比如10*1024*1024*1024 → 10GB）
//	bs: 基础块大小（如1G、500M）
//	chunksPerGoroutine: 每个协程处理的块数（如bs=1G，该值=5 → 每个协程下载5GB）
//	concurrency: 并发数
func SSHDDReadRangeConcurrent(cfg SSHConfig, remoteFile, localDir, outputFile string,
	start, length int64, bs string, chunksPerGoroutine, concurrency int) error {

	// 1. 校验参数
	if length <= 0 {
		return fmt.Errorf("length必须大于0（当前=%d）", length)
	}
	if start < 0 {
		return fmt.Errorf("start不能小于0（当前=%d）", start)
	}
	if chunksPerGoroutine <= 0 {
		chunksPerGoroutine = 5 // 默认每个协程处理5个块
	}
	if concurrency <= 0 {
		concurrency = 4 // 默认4个协程
	}

	log.Printf("开始并发下载指定范围：start=%d, length=%d（%.2fGB）, bs=%s",
		start, length, float64(length)/(1024*1024*1024), bs)

	// 2. 解析bs字节数
	bsBytes, err := getBsInBytes(cfg, bs)
	if err != nil {
		return fmt.Errorf("解析块大小失败：%v", err)
	}

	// 3. 计算需要的总分片数
	totalChunks := (length + int64(chunksPerGoroutine)*bsBytes - 1) / (int64(chunksPerGoroutine) * bsBytes)
	log.Printf("目标范围将拆分为%d个分片，每个协程处理%d个块（%s）",
		totalChunks, chunksPerGoroutine, bs)

	// 4. 并发下载分片
	ctx := context.Background()
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)

	var (
		chunkFiles = make([]string, 0, totalChunks)
		mu         sync.Mutex
	)

	for i := 0; i < int(totalChunks); i++ {
		chunkIndex := i
		eg.Go(func() error {
			chunkFile, err := SSHDDReadRangeChunk(
				cfg, remoteFile, localDir, bs,
				start, length, chunkIndex, chunksPerGoroutine,
			)
			if err != nil {
				return err
			}
			if chunkFile != "" {
				mu.Lock()
				chunkFiles = append(chunkFiles, chunkFile)
				mu.Unlock()
			}
			return nil
		})
	}

	// 5. 等待所有协程完成
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("并发下载失败：%v", err)
	}

	// 6. 合并分片为最终文件
	if outputFile != "" && len(chunkFiles) > 0 {
		if err := MergeChunks(chunkFiles, outputFile); err != nil {
			return fmt.Errorf("合并分片失败：%v", err)
		}
		log.Printf("指定范围[%d, %d)下载完成 → %s", start, start+length, outputFile)
	}

	return nil
}

// ------------------- 以下是通用工具函数 -------------------
// getBsInBytes 解析bs单位为字节数（如1G→1073741824）
func getBsInBytes(cfg SSHConfig, bs string) (int64, error) {
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}

	client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()

	// 用dd命令获取bs对应的字节数
	cmd := fmt.Sprintf("dd if=/dev/null bs=%s count=0 2>&1 | grep -oP '(?<=bs=)\\d+'", bs)
	output, err := session.CombinedOutput(cmd)
	if err != nil {
		return 0, fmt.Errorf("执行dd失败：%v，输出：%s", err, string(output))
	}

	bsStr := strings.TrimSpace(string(output))
	bsBytes, err := strconv.ParseInt(bsStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("转换失败：%v，原始输出：%s", err, bsStr)
	}

	return bsBytes, nil
}

// MergeChunks 合并分片为完整文件
func MergeChunks(chunkFiles []string, outputFile string) error {
	outputFd, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("创建合并文件失败：%v", err)
	}
	defer outputFd.Close()

	bufWriter := bufio.NewWriter(outputFd)
	defer bufWriter.Flush()

	for i, chunkFile := range chunkFiles {
		chunkFd, err := os.Open(chunkFile)
		if err != nil {
			return fmt.Errorf("打开分片%d失败：%v", i, err)
		}
		_, err = io.Copy(bufWriter, chunkFd)
		chunkFd.Close()
		if err != nil {
			return fmt.Errorf("合并分片%d失败：%v", i, err)
		}
	}

	return nil
}

// ------------------- 测试示例 -------------------
//func main() {
//	// 1. 配置SSH
//	cfg := SSHConfig{
//		User:     "root",
//		Host:     "192.168.1.20:22",
//		Password: "your-password", // 替换为实际密码
//	}
//
//	// 2. 定义目标范围：start=20GB，length=10GB
//	const (
//		GB          = 1024 * 1024 * 1024
//		start       = 20 * GB // 起始位置20GB
//		length      = 10 * GB // 读取10GB
//		bs          = "1G"    // 基础块大小1GB
//		chunksPerGo = 2       // 每个协程处理2个块（2GB）
//		concurrency = 3       // 3个协程并发
//	)
//
//	// 3. 执行并发下载
//	err := SSHDDReadRangeConcurrent(
//		cfg,
//		"/mnt/remote-data/bigfile.bin", // 远端大文件
//		"/mnt/local-data/tmp_chunks",   // 临时分片目录
//		"/mnt/local-data/range_20_30GB.bin", // 最终合并后的10GB文件
//		start,
//		length,
//		bs,
//		chunksPerGo,
//		concurrency,
//	)
//
//	if err != nil {
//		log.Fatalf("下载失败：%v", err)
//	}
//	log.Println("指定范围并发下载完成！")
//}
