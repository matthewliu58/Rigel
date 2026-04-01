package util

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/oauth2/google"
	"log/slog"
	"math/bits"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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

// AutoSelectChunkSize 自动最优分片：10片左右 + 对齐到2的次方 + 最小64M / 最大2G
func AutoSelectChunkSize(totalSize int64) int64 {
	if totalSize <= 0 {
		return 64 << 20
	}

	// 1. 目标 10 片，四舍五入计算
	const targetParts = 10
	chunkSize := (totalSize + targetParts/2) / targetParts

	// 2. 核心：先对齐到最近的 2 的次方（你要求放这里！）
	if chunkSize > 0 {
		chunkSize = 1 << bits.Len64(uint64(chunkSize)-1)
	}

	// 3. 最后限制范围：最小64M，最大2G
	const minChunk = 64 << 20 // 64MB
	const maxChunk = 2 << 30  // 2GB

	if chunkSize < minChunk {
		return minChunk
	}
	if chunkSize > maxChunk {
		return maxChunk
	}
	return chunkSize
}

// AutoSelectBs 严格匹配，返回标准大写格式
func AutoSelectBs(totalSize int64) string {
	chunkSize := AutoSelectChunkSize(totalSize)

	switch chunkSize {
	case 2 << 30:
		return "2G"
	case 1 << 30:
		return "1G"
	case 512 << 20:
		return "512M"
	case 256 << 20:
		return "256M"
	case 128 << 20:
		return "128M"
	case 64 << 20:
		return "64M"
	default:
		return "64M"
	}
}

// ParseBsToBytes 兼容大小写解析
func ParseBsToBytes(bs string) (int64, error) {
	bs = strings.TrimSpace(strings.ToLower(bs))
	if bs == "" {
		return 0, errors.New("bs 不能为空")
	}

	var numPart string
	var unitPart string
	for i, ch := range bs {
		if ch >= '0' && ch <= '9' {
			numPart += string(ch)
		} else {
			unitPart = bs[i:]
			break
		}
	}

	if numPart == "" {
		return 0, fmt.Errorf("无效的 bs 格式: %s", bs)
	}

	num, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return 0, err
	}

	switch unitPart {
	case "k", "kb":
		return num * 1024, nil
	case "m", "mb":
		return num << 20, nil
	case "g", "gb":
		return num << 30, nil
	case "t", "tb":
		return num << 40, nil
	default:
		return num, nil
	}
}

// SortPartStrings 按part后的数字升序排序part字符串（如aa.part.4 → 按数字4排序）
func SortPartStrings(parts []string) []string {
	// 复制原切片，避免修改原数据
	sortedParts := make([]string, len(parts))
	copy(sortedParts, parts)

	// 自定义排序规则
	sort.Slice(sortedParts, func(i, j int) bool {
		// 提取第i个字符串的数字部分
		numI := extractPartNumber(sortedParts[i])
		// 提取第j个字符串的数字部分
		numJ := extractPartNumber(sortedParts[j])
		// 按数字升序排列
		return numI < numJ
	})

	return sortedParts
}

// extractPartNumber 从part字符串中提取数字（如"aa.part.4" → 4）
func extractPartNumber(s string) int {
	// 按"."分割字符串
	parts := strings.Split(s, ".")
	// 取最后一个部分作为数字（兼容"aa.part.10"这类多位数）
	if len(parts) < 3 {
		return 0 // 格式异常时返回0，确保排序不报错
	}
	numStr := parts[len(parts)-1]
	// 转换为数字，失败则返回0
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0
	}
	return num
}

func DeleteFilesInDir(dir string, fileNames []string, pre string, logger *slog.Logger) error {
	// 1. 基础参数校验
	if dir == "" {
		err := errors.New("目录路径不能为空")
		logger.Error("DeleteFilesInDir invalid param", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if len(fileNames) == 0 {
		err := errors.New("文件名数组不能为空")
		logger.Error("DeleteFilesInDir invalid param", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if logger == nil {
		err := errors.New("logger实例不能为空")
		fmt.Printf("错误【%s】: %v\n", pre, err) // 兜底日志
		return err
	}

	// 2. 检查目录是否存在
	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("目录不存在: %s", dir)
			logger.Error("DeleteFilesInDir dir check failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		err = fmt.Errorf("获取目录信息失败: %w", err)
		logger.Error("DeleteFilesInDir dir stat failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if !dirInfo.IsDir() {
		err = fmt.Errorf("指定路径不是目录: %s", dir)
		logger.Error("DeleteFilesInDir not a directory", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// 3. 遍历删除每个文件
	var failedFiles []string
	for _, fileName := range fileNames {
		// 过滤空文件名
		if strings.TrimSpace(fileName) == "" {
			logger.Warn("DeleteFilesInDir skip empty filename", slog.String("pre", pre))
			continue
		}

		// 拼接完整文件路径（自动处理路径分隔符，跨平台兼容）
		filePath := filepath.Join(dir, fileName)

		// 删除文件
		err := os.Remove(filePath)
		if err != nil {
			// 记录删除失败的文件，但不中断流程
			failedMsg := fmt.Sprintf("%s: %v", filePath, err)
			failedFiles = append(failedFiles, failedMsg)
			logger.Warn("DeleteFilesInDir delete file failed",
				slog.String("pre", pre),
				slog.String("filePath", filePath),
				slog.Any("err", err))
			continue
		}

		logger.Info("DeleteFilesInDir delete file success",
			slog.String("pre", pre),
			slog.String("filePath", filePath))
	}

	// 4. 处理删除失败的文件（如有）
	if len(failedFiles) > 0 {
		err = fmt.Errorf("部分文件删除失败: %s", strings.Join(failedFiles, "; "))
		logger.Error("DeleteFilesInDir partial delete failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return err
	}

	logger.Info("DeleteFilesInDir all files deleted success", slog.String("pre", pre))
	return nil
}

// ReplaceUploadURLHost 替换Upload URL中的IP:Port为first hop的值
// 入参：
//   - originalUploadURL: 原始Upload URL（如 "http://127.0.0.1:8081/api/v1/chunk/upload"）
//   - firstHop: 目标IP:Port（如 "192.168.1.100:8082"）
//
// 出参：
//   - 替换后的URL / 错误信息
func ReplaceUploadURLHost(originalUploadURL, firstHop string) (string, error) {
	// 1. 校验入参合法性
	if originalUploadURL == "" {
		return "", fmt.Errorf("original upload URL is empty")
	}
	if firstHop == "" {
		return "", fmt.Errorf("first hop is empty")
	}

	// 2. 解析原始URL（兼容带/不带协议的情况）
	var parsedURL *url.URL
	var err error
	if !strings.Contains(originalUploadURL, "://") {
		// 无协议的URL，自动补全http协议（避免解析失败）
		parsedURL, err = url.Parse("http://" + originalUploadURL)
	} else {
		parsedURL, err = url.Parse(originalUploadURL)
	}
	if err != nil {
		return "", fmt.Errorf("parse original URL failed: %w", err)
	}

	// 3. 替换Host为first hop
	parsedURL.Host = firstHop

	// 4. 还原URL（如果原始URL无协议，移除补全的http://）
	resultURL := parsedURL.String()
	if !strings.Contains(originalUploadURL, "://") {
		resultURL = strings.TrimPrefix(resultURL, "http://")
	}

	return resultURL, nil
}

func GetPublicIP() (string, error) {
	resp, err := http.Get("https://icanhazip.com")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf := make([]byte, 1024)
	n, err := resp.Body.Read(buf)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buf[:n])), nil
}

func GetGCPShortToken_(ctx context.Context, credFile, pre string, logger *slog.Logger) (string, error) {

	jsonBytes, err := os.ReadFile(credFile)
	if err != nil {
		logger.Error("read cred file failed",
			slog.String("pre", pre),
			slog.String("credFile", credFile),
			slog.Any("err", err))
		return "", fmt.Errorf("read cred file: %w", err)
	}

	reds, err := google.CredentialsFromJSON(ctx, jsonBytes,
		"https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		logger.Error("Parse GCP credentials failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return "", fmt.Errorf("parse credentials: %w", err)
	}

	token, err := reds.TokenSource.Token()
	if err != nil {
		logger.Error("Get GCP token failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return "", fmt.Errorf("get token: %w", err)
	}

	return token.AccessToken, nil
}

func GetGCPShortToken(ctx context.Context, credFile, pre string, logger *slog.Logger) (string, error) {
	jsonBytes, err := os.ReadFile(credFile)
	if err != nil {
		logger.Error("read cred file failed",
			slog.String("pre", pre),
			slog.String("credFile", credFile),
			slog.Any("err", err))
		return "", fmt.Errorf("read cred file: %w", err)
	}

	// 1. 先解析 JWT 配置
	cfg, err := google.JWTConfigFromJSON(jsonBytes, "https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		logger.Error("parse JWT config failed", slog.String("pre", pre), slog.Any("err", err))
		return "", fmt.Errorf("jwt config: %w", err)
	}

	cfg.Expires = 1 * time.Minute

	// 2. 获取 token
	token, err := cfg.TokenSource(ctx).Token()
	if err != nil {
		logger.Error("get GCP token failed", slog.String("pre", pre), slog.Any("err", err))
		return "", fmt.Errorf("get token: %w", err)
	}

	return token.AccessToken, nil
}
