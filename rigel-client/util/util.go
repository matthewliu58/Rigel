package util

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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
	case totalSize <= _100MB:
		return _1M // 1*1024*1024
	case totalSize <= _1GB:
		return _64M // 64*1024*1024
	case totalSize <= _10GB:
		return _512M // 512*1024*1024
	case totalSize <= _100GB:
		return _1G // 1*1024*1024*1024
	default:
		return _2G // 2*1024*1024*1024
	}
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
	case totalSize <= _100MB:
		return "1M"
	case totalSize <= _1GB:
		return "64M"
	case totalSize <= _10GB:
		return "512M"
	case totalSize <= _100GB:
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

// DeleteFilesInDir 删除指定目录下的指定文件
// 参数说明：
//
//	dir: 目标目录路径（绝对路径/相对路径均可）
//	fileNames: 需要删除的文件名数组（仅需文件名，无需路径）
//	pre: 日志前缀（用于追踪请求/业务标识）
//	logger: slog日志实例（用于标准化日志输出）
//
// 返回值：
//
//	error: 整体错误（目录不存在/权限问题等），单个文件删除失败会记录警告但不中断整体流程
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
