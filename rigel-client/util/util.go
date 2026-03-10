package util

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func GenerateRandomLetters(length int) string {
	rand.Seed(time.Now().UnixNano())                                  // 使用当前时间戳作为随机数种子
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // 字母范围（大小写）
	var result string
	for i := 0; i < length; i++ {
		result += string(letters[rand.Intn(len(letters))]) // 随机选择一个字母
	}
	return result
}

type UserRouteRequest struct {
	FileName   string `json:"fileName"`        // 文件名
	Priority   int    `json:"priority"`        // 文件优先级
	ClientCont string `json:"clientContinent"` // 客户端大区
	ServerIP   string `json:"serverIP"`        // 目标服务器 IP 或域名
	//ServerCont     string `json:"serverContinent"` // 目标服务器大区
	Username      string `json:"username"`      // 客户端用户名
	CloudProvider string `json:"cloudProvider"` // 云服务提供商，例如 AWS, GCP, DO
	CloudRegion   string `json:"cloudRegion"`   // 云服务所在区域，例如 us-east-1
	CloudCity     string `json:"cloudCity"`     // 云服务所在城市，例如 Ashburn
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}

// SSHConfig 定义SSH连接配置
type SSHConfig struct {
	User     string // 用户名
	Host     string // 主机IP:端口（如192.168.1.20:22）
	Password string // 密码（或用密钥认证）
}

// AutoSelectChunkSize 根据文件大小自动选择最优分片大小（直接返回字节数，无字符串解析）
// 参数：totalSize - 文件总大小（字节）
// 返回：最优分片大小（字节，如 1048576 对应1M，67108864对应64M）
func AutoSelectChunkSize(totalSize int64) int64 {
	const (
		// 基础单位常量（字节）
		_1M   = 1 * 1024 * 1024
		_64M  = 64 * 1024 * 1024
		_512M = 512 * 1024 * 1024
		_1G   = 1 * 1024 * 1024 * 1024
		_2G   = 2 * 1024 * 1024 * 1024

		// 阈值常量
		_100MB = 100 * 1024 * 1024
		_1GB   = 1 * 1024 * 1024 * 1024
		_10GB  = 10 * 1024 * 1024 * 1024
		_100GB = 100 * 1024 * 1024 * 1024
	)

	switch {
	case totalSize < _100MB:
		return _1M // 1*1024*1024
	case totalSize < _1GB:
		return _64M // 64*1024*1024
	case totalSize < _10GB:
		return _512M // 512*1024*1024
	case totalSize < _100GB:
		return _1G // 1*1024*1024*1024
	default:
		return _2G // 2*1024*1024*1024
	}
}

// ------------------- 内部工具函数（依赖） -------------------

// getRemoteFileSize 获取远端文件总大小（字节）
func GetRemoteFileSize(ctx context.Context, cfg SSHConfig, remoteDir, filename string, pre string, logger *slog.Logger) (int64, error) {
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

// ------------------------------ 辅助函数：字节数格式化（可选） ------------------------------
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

// autoSelectBs 根据读取的总大小自动选择最优块大小
func AutoSelectBs(totalSize int64) string {
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
func ParseBsToBytes(bs string) (int64, error) {
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
