package download

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
	"strings"
	"sync"
	"time"
)

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
func SSHDDReadRangeChunk(ctx context.Context, cfg util.SSHConfig, remoteDir, filename, newFilename, localDir string,
	start, length int64, bs string, pre string, logger *slog.Logger) (string, error) {

	// 1. 拼接远端文件完整路径
	remoteFile := filepath.Join(remoteDir, filename)

	//split := false

	// 2. 处理length ≤ 0的情况（读取全部文件）
	var actualStart, actualLength int64
	if length <= 0 {
		// 获取远端文件总大小
		fileSize, err := util.GetRemoteFileSize(ctx, cfg, remoteDir, filename, pre, logger)
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
		//split = true
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
		bs = util.AutoSelectBs(actualLength)
		logger.Info("自动选择块大小",
			slog.String("pre", pre),
			slog.String("块大小", bs))
	}

	// 4. 解析块大小为字节数
	bsBytes, err := util.ParseBsToBytes(bs)
	if err != nil {
		return "", fmt.Errorf("解析块大小失败：%w", err)
	}

	// 5. 构造本地输出文件名
	//localFileName := buildLocalFileName(filename, actualStart, actualLength, split)
	localFilePath := filepath.Join(localDir, newFilename)

	localFileName := newFilename

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
func SSHDDReadRangeConcurrent(ctx context.Context, cfg util.SSHConfig, remoteDir, filename, newFilename, localDir string,
	start, length, chunkSize int64, concurrency int, bs string, pre string, logger *slog.Logger) (string, error) {

	// 1. 初始化默认值
	chunkSize = util.AutoSelectChunkSize(length)

	if concurrency <= 0 {
		concurrency = 8
	}

	// 2. 先获取远端文件总大小 + 计算实际读取范围（复用原有逻辑）
	remoteFile := filepath.Join(remoteDir, filename)
	var (
		actualStart   int64
		actualLength  int64
		totalFileSize int64
		//split         bool
		err error
	)

	// 获取远端文件总大小
	totalFileSize, err = util.GetRemoteFileSize(ctx, cfg, remoteDir, filename, pre, logger)
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
		return SSHDDReadRangeChunk(ctx, cfg, remoteDir, filename, newFilename, localDir,
			actualStart, actualLength, bs, pre, logger)
	}

	// 4. 构造本地文件名（复用原有逻辑）
	//localFileName := buildLocalFileName(filename, actualStart, actualLength, split)
	localFilePath := filepath.Join(localDir, newFilename)

	localFileName := newFilename

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
				chunkBs = util.AutoSelectBs(chunkLength)
			}

			// 解析块大小为字节数（用于计算dd的skip和count）
			bsBytes, err := util.ParseBsToBytes(chunkBs)
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
