package download

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
	"strings"
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
