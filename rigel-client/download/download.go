package download

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
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

// SSHDDReadRangeChunk 读取指定范围/全部文件的核心函数
// 核心规则：
//  1. length ≤ 0 → 读取整个文件（忽略start，从0开始读全部）
//  2. length > 0  → 读取 [start, start+length) 范围的内容
//
// 参数说明：
//
//	ctx: 上下文
//	cfg: SSH连接配置
//	remoteDir: 远端文件所在目录（如/mnt/remote-data）
//	filename: 要读取的文件名（如bigfile.bin）
//	localDir: 本地存储目录（如/home/matth/upload/）
//	start: 读取起始位置（字节）；length≤0时该参数无效
//	length: 读取长度（字节）；≤0时读取全部文件
//	bs: dd命令块大小（建议传空字符串，函数自动适配）
//	pre: 日志前缀
//	logger: 日志对象
//
// 返回值：
//
//	本地文件路径 / 错误信息
func SSHDDReadRangeChunk(ctx context.Context, cfg util.SSHConfig, remoteDir, filename, localDir string,
	start, length int64, bs string, pre string, logger *slog.Logger) (string, error) {

	// 1. 拼接远端文件完整路径
	remoteFile := filepath.Join(remoteDir, filename)

	split := false

	// 2. 处理length ≤ 0的情况（读取全部文件）
	var actualStart, actualLength int64
	if length <= 0 {
		// 获取远端文件总大小
		fileSize, err := getRemoteFileSize(ctx, cfg, remoteDir, filename, pre, logger)
		if err != nil {
			return "", fmt.Errorf("获取文件大小失败：%w", err)
		}
		actualStart = 0         // 从文件开头读取
		actualLength = fileSize // 读取完整文件大小
		logger.Info("读取整个文件",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile),
			slog.Int64("文件总大小(字节)", actualLength))
	} else {
		// 验证start合法性（length>0时start不能为负）
		if start < 0 {
			return "", fmt.Errorf("start不能小于0（length>0时）")
		}
		split = true
		actualStart = start
		actualLength = length
		logger.Info("读取指定范围文件",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile),
			slog.Int64("起始位置(字节)", actualStart),
			slog.Int64("读取长度(字节)", actualLength))
	}

	// 3. 自动选择最优块大小（如果bs为空）
	if strings.TrimSpace(bs) == "" {
		bs = autoSelectBs(actualLength)
		logger.Info("自动选择块大小",
			slog.String("pre", pre),
			slog.String("块大小", bs))
	}

	// 4. 解析块大小为字节数
	bsBytes, err := parseBsToBytes(bs)
	if err != nil {
		return "", fmt.Errorf("解析块大小失败：%w", err)
	}

	// 5. 构造本地输出文件名
	localFileName := buildLocalFileName(filename, actualStart, actualLength, split)
	localFilePath := filepath.Join(localDir, localFileName)

	// 6. 计算dd命令参数（skip=跳过的块数，count=读取的块数）
	skip := actualStart / bsBytes                   // 定位到起始位置需要跳过的块数
	count := (actualLength + bsBytes - 1) / bsBytes // 需要读取的块数（向上取整）
	ddCmd := fmt.Sprintf("dd if=%s bs=%s skip=%d count=%d", remoteFile, bs, skip, count)

	// 7. 初始化SSH客户端配置
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 生产环境建议替换为合法的密钥验证
		Timeout:         5 * time.Minute,             // 增大超时适配大文件读取
	}

	// 8. 建立SSH连接
	client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
	if err != nil {
		return "", fmt.Errorf("SSH连接失败：%w", err)
	}
	defer client.Close()

	// 9. 创建SSH会话
	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("创建SSH会话失败：%w", err)
	}
	defer session.Close()

	// 10. 确保本地目录存在
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return "", fmt.Errorf("创建本地目录失败：%w", err)
	}

	// 11. 创建本地文件（覆盖已有文件）
	localFd, err := os.Create(localFilePath)
	if err != nil {
		return "", fmt.Errorf("创建本地文件失败：%w", err)
	}
	defer localFd.Close()

	// 12. 执行dd命令并将输出写入本地文件
	logger.Info("执行dd命令",
		slog.String("pre", pre),
		slog.String("命令", ddCmd),
		slog.String("输出文件", localFilePath))

	// 获取dd命令的标准输出管道
	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("获取stdout管道失败：%w", err)
	}

	// 启动dd命令
	if err := session.Start(ddCmd); err != nil {
		return "", fmt.Errorf("启动dd命令失败：%w，命令：%s", err, ddCmd)
	}

	// 一次性将dd输出拷贝到本地文件（核心读取逻辑）
	_, err = io.Copy(localFd, stdout)
	if err != nil {
		return "", fmt.Errorf("写入本地文件失败：%w", err)
	}

	// 等待dd命令执行完成
	if err := session.Wait(); err != nil {
		// dd返回非0不一定是失败（如最后一块不足bs），仅打警告日志
		logger.Warn("dd命令执行完成但返回非0状态",
			slog.String("pre", pre),
			slog.String("错误", err.Error()))
	}

	// 验证本地文件大小（可选，增强健壮性）
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		logger.Warn("无法获取本地文件信息",
			slog.String("pre", pre),
			slog.String("文件", localFilePath),
			slog.String("错误", err.Error()))
	} else {
		logger.Info("文件读取完成",
			slog.String("pre", pre),
			slog.String("本地文件", localFilePath),
			slog.Int64("文件大小(字节)", fileInfo.Size()),
			slog.String("预期大小(字节)", fmt.Sprintf("%d", actualLength)))
	}

	return localFileName, nil
}

// SSHDDReadRangeConcurrent 并发读取远端文件（指定范围/全量）
// 核心逻辑：
//  1. 将读取范围拆分为多个子分片，并发执行dd命令读取
//  2. 每个子分片读取后写入本地文件的指定偏移位置
//  3. 支持自定义分片大小和并发数
//
// 参数说明：
//
//	chunkSize: 每个子分片的大小（字节，传0使用默认100MB）
//	concurrency: 并发数（传0使用默认8）
func SSHDDReadRangeConcurrent(ctx context.Context, cfg util.SSHConfig, remoteDir, filename, localDir string,
	start, length, chunkSize int64, concurrency int, bs string, pre string, logger *slog.Logger) (string, error) {

	// 1. 初始化默认值
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize // 复用GCS的默认分片大小（100MB）
	}
	if concurrency <= 0 {
		concurrency = 8
	}

	// 2. 先获取远端文件总大小 + 计算实际读取范围（复用原有逻辑）
	remoteFile := filepath.Join(remoteDir, filename)
	var (
		actualStart   int64
		actualLength  int64
		totalFileSize int64
		split         bool
		err           error
	)

	// 获取远端文件总大小
	totalFileSize, err = getRemoteFileSize(ctx, cfg, remoteDir, filename, pre, logger)
	if err != nil {
		return "", fmt.Errorf("获取远端文件大小失败：%w", err)
	}

	// 计算实际读取范围
	if length <= 0 {
		// 全量读取
		actualStart = 0
		actualLength = totalFileSize
		split = false
		logger.Info("并发读取整个文件",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile),
			slog.Int64("文件总大小", totalFileSize),
			slog.Int64("分片大小", chunkSize),
			slog.Int("并发数", concurrency))
	} else {
		// 指定范围读取
		if start < 0 {
			return "", fmt.Errorf("start不能小于0（length>0时）")
		}
		actualStart = start
		actualLength = length
		split = true

		// 校验范围合法性
		if actualStart >= totalFileSize {
			return "", fmt.Errorf("起始位置%d超出文件总大小%d", actualStart, totalFileSize)
		}
		// 修正结束位置（防止超出文件大小）
		endPos := actualStart + actualLength
		if endPos > totalFileSize {
			actualLength = totalFileSize - actualStart
			logger.Warn("读取范围超出文件大小，自动调整长度",
				slog.String("pre", pre),
				slog.Int64("原长度", length),
				slog.Int64("调整后长度", actualLength))
		}

		logger.Info("并发读取指定范围文件",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile),
			slog.Int64("起始位置", actualStart),
			slog.Int64("读取长度", actualLength),
			slog.Int64("分片大小", chunkSize),
			slog.Int("并发数", concurrency))
	}

	// 3. 如果读取长度≤分片大小，直接使用单分片读取（复用原有函数）
	if actualLength <= chunkSize {
		logger.Info("读取长度小于等于分片大小，使用单分片读取", slog.String("pre", pre))
		return SSHDDReadRangeChunk(ctx, cfg, remoteDir, filename, localDir,
			actualStart, actualLength, bs, pre, logger)
	}

	// 4. 构造本地文件名（复用原有逻辑）
	localFileName := buildLocalFileName(filename, actualStart, actualLength, split)
	localFilePath := filepath.Join(localDir, localFileName)

	// 5. 创建本地文件并预分配空间
	localFile, err := os.Create(localFilePath)
	if err != nil {
		return "", fmt.Errorf("创建本地文件失败：%w", err)
	}
	defer localFile.Close()

	// 预分配文件空间（避免频繁扩容）
	if err := localFile.Truncate(actualLength); err != nil {
		return "", fmt.Errorf("预分配文件空间失败：%w", err)
	}

	// 6. 计算总分片数（向上取整）
	totalChunks := (actualLength + chunkSize - 1) / chunkSize
	logger.Info("拆分读取范围为子分片",
		slog.String("pre", pre),
		slog.Int64("总分片数", totalChunks),
		slog.Int64("每个分片大小", chunkSize))

	// 7. 使用errgroup管理并发goroutine（支持错误传播+并发限制）
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)

	// 8. 进度统计
	var (
		completedChunks int64
		mu              sync.Mutex
	)

	// 9. 遍历所有子分片，启动并发读取
	for i := int64(0); i < totalChunks; i++ {
		chunkIndex := i // 捕获循环变量

		eg.Go(func() error {
			// 计算当前子分片的全局范围（远端文件的绝对位置）
			chunkGlobalStart := actualStart + chunkIndex*chunkSize
			chunkGlobalEnd := chunkGlobalStart + chunkSize

			// 最后一个分片修正结束位置
			if chunkGlobalEnd > actualStart+actualLength {
				chunkGlobalEnd = actualStart + actualLength
			}
			chunkLength := chunkGlobalEnd - chunkGlobalStart

			// 计算当前子分片在本地文件中的写入偏移（本地文件从0开始）
			chunkLocalOffset := chunkGlobalStart - actualStart

			logger.Debug("开始读取子分片",
				slog.String("pre", pre),
				slog.Int64("分片索引", chunkIndex),
				slog.Int64("全局起始位置", chunkGlobalStart),
				slog.Int64("全局结束位置", chunkGlobalEnd),
				slog.Int64("分片长度", chunkLength),
				slog.Int64("本地写入偏移", chunkLocalOffset))

			// 自动选择块大小（复用原有逻辑）
			chunkBs := bs
			if strings.TrimSpace(chunkBs) == "" {
				chunkBs = autoSelectBs(chunkLength)
			}

			// 解析块大小为字节数（用于计算dd的skip和count）
			bsBytes, err := parseBsToBytes(chunkBs)
			if err != nil {
				return fmt.Errorf("分片%d：解析块大小失败：%w", chunkIndex, err)
			}

			// 计算dd命令的skip和count
			skip := chunkGlobalStart / bsBytes             // 跳过的块数
			count := (chunkLength + bsBytes - 1) / bsBytes // 读取的块数（向上取整）

			// 构造dd命令（读取指定范围）
			ddCmd := fmt.Sprintf("dd if=%s bs=%s skip=%d count=%d",
				remoteFile, chunkBs, skip, count)

			// 初始化SSH客户端（每个分片独立创建连接，避免连接复用冲突）
			sshConfig := &ssh.ClientConfig{
				User:            cfg.User,
				Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
				Timeout:         5 * time.Minute,
			}

			// 建立SSH连接
			client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
			if err != nil {
				return fmt.Errorf("分片%d：SSH连接失败：%w", chunkIndex, err)
			}
			defer client.Close()

			// 创建SSH会话
			session, err := client.NewSession()
			if err != nil {
				return fmt.Errorf("分片%d：创建SSH会话失败：%w", chunkIndex, err)
			}
			defer session.Close()

			// 获取dd命令的标准输出管道
			stdout, err := session.StdoutPipe()
			if err != nil {
				return fmt.Errorf("分片%d：获取stdout管道失败：%w", chunkIndex, err)
			}

			// 启动dd命令
			if err := session.Start(ddCmd); err != nil {
				return fmt.Errorf("分片%d：启动dd命令失败：%w，命令：%s", chunkIndex, err, ddCmd)
			}

			// 定位到本地文件的指定偏移位置
			_, err = localFile.Seek(chunkLocalOffset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("分片%d：定位文件偏移失败：%w", chunkIndex, err)
			}

			// 将dd输出写入本地文件
			written, err := io.Copy(localFile, stdout)
			if err != nil {
				return fmt.Errorf("分片%d：写入本地文件失败：%w", chunkIndex, err)
			}

			// 等待dd命令执行完成
			if err := session.Wait(); err != nil {
				logger.Warn("分片%d：dd命令返回非0状态（可能是正常情况）",
					slog.String("pre", pre),
					slog.Int64("分片索引", chunkIndex),
					slog.String("错误", err.Error()))
			}

			// 验证写入大小（允许最后一个分片略小，因为dd可能返回不足bs的块）
			if written < chunkLength && !(chunkIndex == totalChunks-1) {
				return fmt.Errorf("分片%d：写入大小不匹配（预期%d，实际%d）",
					chunkIndex, chunkLength, written)
			}

			// 更新进度
			mu.Lock()
			completedChunks++
			progress := float64(completedChunks) / float64(totalChunks) * 100
			mu.Unlock()

			logger.Debug("子分片读取完成",
				slog.String("pre", pre),
				slog.Int64("分片索引", chunkIndex),
				slog.Float64("进度(%)", progress),
				slog.Int64("写入字节数", written))

			return nil
		})
	}

	// 10. 等待所有并发分片读取完成
	if err := eg.Wait(); err != nil {
		return "", fmt.Errorf("并发读取失败：%w", err)
	}

	// 11. 验证最终文件大小
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		logger.Warn("无法验证本地文件大小",
			slog.String("pre", pre),
			slog.String("本地文件", localFilePath),
			slog.String("错误", err.Error()))
	} else {
		logger.Info("并发读取完成",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile),
			slog.String("本地文件", localFilePath),
			slog.Int64("实际读取大小", fileInfo.Size()),
			slog.Int64("预期读取大小", actualLength))
	}

	return localFileName, nil
}

// ------------------- 内部工具函数（依赖） -------------------

// getRemoteFileSize 获取远端文件总大小（字节）
func getRemoteFileSize(ctx context.Context, cfg util.SSHConfig, remoteDir, filename string, pre string, logger *slog.Logger) (int64, error) {
	remoteFile := filepath.Join(remoteDir, filename)

	// 初始化SSH配置
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}

	// 建立SSH连接
	client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
	if err != nil {
		return 0, fmt.Errorf("SSH连接失败：%w", err)
	}
	defer client.Close()

	// 创建会话
	session, err := client.NewSession()
	if err != nil {
		return 0, fmt.Errorf("创建SSH会话失败：%w", err)
	}
	defer session.Close()

	// 兼容Linux/macOS的stat命令
	cmd := fmt.Sprintf("stat -c %%s '%s'", remoteFile)
	output, err := session.CombinedOutput(cmd)
	if err != nil {
		cmd = fmt.Sprintf("stat -f %%z '%s'", remoteFile)
		output, err = session.CombinedOutput(cmd)
		if err != nil {
			return 0, fmt.Errorf("执行stat命令失败：%w，输出：%s", err, string(output))
		}
	}

	// 解析文件大小
	sizeStr := strings.TrimSpace(string(output))
	fileSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析文件大小失败：%w，原始输出：%s", err, sizeStr)
	}

	return fileSize, nil
}

// autoSelectBs 根据读取的总大小自动选择最优块大小
func autoSelectBs(totalSize int64) string {
	const (
		_100MB = 100 * 1024 * 1024
		_1GB   = 1024 * 1024 * 1024
		_10GB  = 10 * _1GB
		_100GB = 100 * _1GB
	)

	switch {
	case totalSize < _100MB:
		return "1M"
	case totalSize < _1GB:
		return "64M"
	case totalSize < _10GB:
		return "512M"
	case totalSize < _100GB:
		return "1G"
	default:
		return "2G"
	}
}

// parseBsToBytes 解析bs字符串为字节数（如1G→1073741824）
func parseBsToBytes(bs string) (int64, error) {
	bs = strings.TrimSpace(strings.ToLower(bs))
	if bs == "" {
		return 0, fmt.Errorf("bs不能为空")
	}

	var numStr, unit string
	for i, c := range bs {
		if (c >= '0' && c <= '9') || c == '.' {
			numStr += string(c)
		} else {
			unit = bs[i:]
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("无法解析bs的数字部分：%s", bs)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("解析bs数字失败：%w，输入：%s", err, numStr)
	}

	var bytes float64
	switch unit {
	case "k", "kb":
		bytes = num * 1024
	case "m", "mb":
		bytes = num * 1024 * 1024
	case "g", "gb":
		bytes = num * 1024 * 1024 * 1024
	case "t", "tb":
		bytes = num * 1024 * 1024 * 1024 * 1024
	default:
		bytes = num // 无单位则为字节
	}

	return int64(bytes), nil
}

// buildLocalFileName 构造本地文件名
func buildLocalFileName(filename string, start, length int64, split bool) string {

	if !split {
		return filename
	}

	basename := filename
	ext := ""
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		basename = filename[:idx]
		ext = filename[idx:]
	}

	// 全量读取（length≤0）
	//if length <= 0 {
	//	return fmt.Sprintf("%s_full%s", basename, ext)
	//}

	// 指定范围读取
	start_ := fmt.Sprintf("%.1f", start)
	end_ := fmt.Sprintf("%.1f", start+length)
	return fmt.Sprintf("%s_%s-%s%s", basename, start_, end_, ext)
}

// ------------------- 测试调用示例 -------------------
//func main() {
//	// 1. 初始化配置
//	ctx := context.Background()
//	logger := slog.Default()
//	sshCfg := SSHConfig{
//		User:     "root",
//		Host:     "192.168.1.20:22",
//		Password: "your-password",
//	}
//
//	// 示例1：读取全部文件（length≤0）
//	localFile1, err := SSHDDReadRangeChunk(
//		ctx, sshCfg,
//		"/mnt/remote-data", "bigfile.bin", // 远端文件路径
//		"/home/matth/upload/",            // 本地存储目录
//		0, -1,                            // start=任意值, length=-1（读全部）
//		"", "SSH_READ", logger,           // bs传空（自动适配）
//	)
//	if err != nil {
//		logger.Error("读取全部文件失败", slog.String("错误", err.Error()))
//		return
//	}
//
//	// 示例2：读取指定范围（20GB开始，读10GB）
//	// localFile2, err := SSHDDReadRangeChunk(
//	// 	ctx, sshCfg,
//	// 	"/mnt/remote-data", "bigfile.bin",
//	// 	"/home/matth/upload/",
//	// 	20*1024*1024*1024, 10*1024*1024*1024, // start=20GB, length=10GB
//	// 	"", "SSH_READ", logger,
//	// )
//
//	logger.Info("操作完成", slog.String("本地文件", localFile1))
//}
