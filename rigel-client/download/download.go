package download

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
	"strconv"
	"strings"
	"time"
)

const (
	GCPCLoud   = "gcp-cloud"
	RemoteDisk = "remote-disk"
	LocalDisk  = "local-disk"
)

// GetLocalFileSize 简化版：获取本地文件大小
// dir: 文件所在目录
// fileName: 文件名
// pre: 日志前缀
// logger: 日志实例
// 返回：文件大小（字节）、错误（文件不存在/非文件/其他异常）
func GetLocalFileSize(ctx context.Context, dir, fileName, pre string, logger *slog.Logger) (int64, error) {
	// 1. 拼接文件完整路径
	filePath := filepath.Join(dir, fileName)

	// 2. 获取文件信息
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		// 记录错误日志，直接返回原错误
		logger.Error("GetLocalFileSize failed",
			slog.String("pre", pre),
			slog.String("filePath", filePath),
			slog.String("error", err.Error()))
		return 0, err
	}

	// 3. 校验是否为文件（排除目录）
	if fileInfo.IsDir() {
		err = os.ErrInvalid // 标记为无效文件（目录）
		logger.Error("GetLocalFileSize failed: path is directory",
			slog.String("pre", pre),
			slog.String("filePath", filePath))
		return 0, err
	}

	// 4. 成功返回文件大小
	logger.Info("GetLocalFileSize success",
		slog.String("pre", pre),
		slog.String("filePath", filePath),
		slog.Int64("fileSize", fileInfo.Size()))
	return fileInfo.Size(), nil
}

// getRemoteFileSize 获取远端文件总大小（字节）
func GetRemoteFileSize(ctx context.Context, cfg util.SSHConfig, remoteDir, filename string, pre string, logger *slog.Logger) (int64, error) {
	remoteFile := filepath.Join(remoteDir, filename)

	// 初始化SSH配置
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}

	// 建立SSH连接
	client, err := ssh.Dial("tcp", cfg.HostPort, sshConfig)
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

// GetGCSObjectSize 获取 GCS 上指定 Object 的文件大小（字节）
// 参数说明：
//
//	ctx: 上下文（用于控制超时/取消）
//	bucketName: GCS Bucket 名称
//	objectName: GCS Object 名称（文件路径）
//	credFile: GCS 凭证文件路径（如 /path/to/cred.json）
//	pre: 日志前缀（用于追踪请求）
//	logger: 日志对象
//
// 返回值：
//
//	int64: Object 大小（字节）
//	error: 错误信息（获取失败时返回）
func GetGCSObjectSize(ctx context.Context, bucketName, objectName, credFile, pre string, logger *slog.Logger) (int64, error) {
	// 1. 入参校验
	if bucketName == "" {
		return 0, fmt.Errorf("bucketName 不能为空")
	}
	if objectName == "" {
		return 0, fmt.Errorf("objectName 不能为空")
	}
	if credFile == "" {
		return 0, fmt.Errorf("credFile 不能为空")
	}

	// 2. 设置 GCS 凭证环境变量
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	// 3. 创建 GCS 客户端（带超时控制）
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // 10秒超时，避免卡住
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("创建 GCS 客户端失败", slog.String("pre", pre),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return 0, fmt.Errorf("storage.NewClient failed: %w", err)
	}
	defer client.Close() // 确保客户端关闭，释放资源

	// 4. 获取 Bucket 和 Object 实例
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	// 5. 获取 Object 元数据（核心：从 Attrs 中读取 Size）
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		logger.Error("获取 GCS Object 元数据失败", slog.String("pre", pre),
			slog.String("bucketName", bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		// 区分常见错误类型，返回更友好的提示
		if err == storage.ErrObjectNotExist {
			return 0, fmt.Errorf("object %s/%s 不存在: %w", bucketName, objectName, err)
		}
		return 0, fmt.Errorf("obj.Attrs failed: %w", err)
	}

	// 6. 日志记录结果
	logger.Info("成功获取 GCS Object 大小", slog.String("pre", pre),
		slog.String("bucketName", bucketName),
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
