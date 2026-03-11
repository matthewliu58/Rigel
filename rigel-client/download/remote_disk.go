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

// SSHDDReadRangeChunk 读取指定范围/全部文件（保留原参数，新增inMemory支持双模式）
// 核心规则：
//  1. length ≤ 0 → 读取整个文件（忽略start，从0开始读全部）
//  2. length > 0  → 读取 [start, start+length) 范围的内容
//  3. inMemory=true → 内存流式（不落盘），false → 本地落盘（原逻辑）
//
// 参数说明（完全保留原参数顺序，仅最后新增inMemory）：
//
//	ctx: 上下文
//	cfg: SSH连接配置
//	remoteDir: 远端文件所在目录（如/mnt/remote-data）
//	filename: 要读取的文件名（如bigfile.bin）
//	newFilename: 本地落盘时的新文件名（inMemory=true时传空）
//	localDir: 本地存储目录（inMemory=true时传空）
//	start: 读取起始位置（字节）；length≤0时该参数无效
//	length: 读取长度（字节）；≤0时读取全部文件
//	bs: dd命令块大小（建议传空字符串，函数自动适配）
//	pre: 日志前缀
//	logger: 日志对象
//	inMemory: 新增！true=内存流式（不落盘），false=本地落盘（默认兼容原逻辑）
//
// 返回值：
//
//	io.ReadCloser: 流式Reader（落盘模式返回本地文件Reader，内存模式返回SSH流式Reader）
//	string: 本地文件路径（仅inMemory=false时有效，兼容原返回值）
//	error: 错误信息
func SSHDDReadRangeChunk(
	ctx context.Context,
	cfg util.SSHConfig,
	remoteDir, filename, newFilename, localDir string,
	start, length int64,
	bs string,
	inMemory bool, // 新增参数放在最后，不影响原有调用
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, string, error) {

	// 1. 拼接远端文件完整路径
	remoteFile := filepath.Join(remoteDir, filename)

	// 2. 处理length ≤ 0的情况（读取全部文件）
	var actualStart, actualLength int64
	if length <= 0 {
		// 获取远端文件总大小
		fileSize, err := GetRemoteFileSize(ctx, cfg, remoteDir, filename, pre, logger)
		if err != nil {
			return nil, "", fmt.Errorf("获取文件大小失败：%w", err)
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
			return nil, "", fmt.Errorf("start不能小于0（length>0时）")
		}
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
		return nil, "", fmt.Errorf("解析块大小失败：%w", err)
	}

	// 5. 计算dd命令参数（skip=跳过的块数，count=读取的块数）
	skip := actualStart / bsBytes                   // 定位到起始位置需要跳过的块数
	count := (actualLength + bsBytes - 1) / bsBytes // 需要读取的块数（向上取整）
	ddCmd := fmt.Sprintf("dd if=%s bs=%s skip=%d count=%d", remoteFile, bs, skip, count)

	// 6. 初始化SSH客户端配置
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 生产环境建议替换为合法的密钥验证
		Timeout:         5 * time.Minute,             // 增大超时适配大文件读取
	}

	// 7. 建立SSH连接
	client, err := ssh.Dial("tcp", cfg.Host, sshConfig)
	if err != nil {
		return nil, "", fmt.Errorf("SSH连接失败：%w", err)
	}

	// 8. 创建SSH会话
	session, err := client.NewSession()
	if err != nil {
		client.Close()
		return nil, "", fmt.Errorf("创建SSH会话失败：%w", err)
	}

	// 9. 获取dd命令的标准输出管道
	stdout, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		client.Close()
		return nil, "", fmt.Errorf("获取stdout管道失败：%w", err)
	}

	// 10. 启动dd命令
	logger.Info(fmt.Sprintf("执行dd命令（%s模式）", map[bool]string{true: "内存流式", false: "本地落盘"}[inMemory]),
		slog.String("pre", pre),
		slog.String("命令", ddCmd))
	if err := session.Start(ddCmd); err != nil {
		session.Close()
		client.Close()
		return nil, "", fmt.Errorf("启动dd命令失败：%w，命令：%s", err, ddCmd)
	}

	// ==================== 模式1：inMemory=true → 内存流式（不落盘） ====================
	if inMemory {
		logger.Info("读取远端文件（内存流式模式，不落盘）",
			slog.String("pre", pre),
			slog.String("远端文件", remoteFile))

		// 封装Reader和SSH资源（调用方Close释放）
		readerWrapper := &sshReaderWrapper{
			reader:       stdout,
			session:      session,
			client:       client,
			pre:          pre,
			logger:       logger,
			ddCmd:        ddCmd,
			actualLength: actualLength,
			readDone:     false,
		}
		return readerWrapper, "", nil
	}

	// ==================== 模式2：inMemory=false → 本地落盘（完全兼容原逻辑） ====================
	// 11. 确保本地目录存在
	if err := os.MkdirAll(localDir, 0755); err != nil {
		session.Close()
		client.Close()
		return nil, "", fmt.Errorf("创建本地目录失败：%w", err)
	}

	// 12. 创建本地文件（覆盖已有文件）
	localFilePath := filepath.Join(localDir, newFilename)
	localFd, err := os.Create(localFilePath)
	if err != nil {
		session.Close()
		client.Close()
		return nil, "", fmt.Errorf("创建本地文件失败：%w", err)
	}
	defer localFd.Close()

	// 13. 执行dd命令并将输出写入本地文件
	logger.Info("将dd输出写入本地文件",
		slog.String("pre", pre),
		slog.String("输出文件", localFilePath))

	// 一次性将dd输出拷贝到本地文件（原逻辑）
	_, err = io.Copy(localFd, stdout)
	if err != nil {
		session.Close()
		client.Close()
		return nil, "", fmt.Errorf("写入本地文件失败：%w", err)
	}

	// 等待dd命令执行完成
	if err := session.Wait(); err != nil {
		logger.Warn("dd命令执行完成但返回非0状态",
			slog.String("pre", pre),
			slog.String("错误", err.Error()))
	}

	// 关闭SSH资源（落盘模式下资源已用完）
	session.Close()
	client.Close()

	// 验证本地文件大小（原逻辑）
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

	// 打开本地文件返回Reader（方便调用方后续读取）
	localFileReader, err := os.Open(localFilePath)
	if err != nil {
		return nil, localFilePath, fmt.Errorf("打开本地文件失败：%w", err)
	}

	// 兼容原返回值：返回本地文件名（newFilename）和路径
	return localFileReader, newFilename, nil
}

// sshReaderWrapper 封装stdout Reader + SSH资源清理逻辑（内存模式用）
type sshReaderWrapper struct {
	reader       io.Reader    // dd命令的stdout管道（无Close）
	session      *ssh.Session // SSH会话
	client       *ssh.Client  // SSH客户端
	pre          string       // 日志前缀
	logger       *slog.Logger // 日志对象
	ddCmd        string       // dd命令
	actualLength int64        // 预期读取长度
	readDone     bool         // 是否已读取完成
}

// Read 实现io.Reader接口
func (w *sshReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = w.reader.Read(p)
	if err == io.EOF {
		w.readDone = true
	}
	return n, err
}

// Close 实现io.ReadCloser接口（释放SSH资源）
func (w *sshReaderWrapper) Close() error {
	var errStr []string

	// 等待dd命令执行完成
	if !w.readDone {
		if err := w.session.Wait(); err != nil {
			w.logger.Warn("dd命令执行完成但返回非0状态",
				slog.String("pre", w.pre),
				slog.String("命令", w.ddCmd),
				slog.String("错误", err.Error()))
		}
	}

	// 关闭SSH会话
	if err := w.session.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("SSH会话关闭失败：%v", err))
	}

	// 关闭SSH客户端
	if err := w.client.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("SSH客户端关闭失败：%v", err))
	}

	w.logger.Info("SSH流式读取资源已释放",
		slog.String("pre", w.pre),
		slog.String("dd命令", w.ddCmd))

	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
