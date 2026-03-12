package util

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ApiResponse struct {
	Code int         `json:"code"` // 200=成功，400=参数错误，500=服务端错误
	Msg  string      `json:"msg"`  // 提示信息
	Data interface{} `json:"data"` // 业务数据
}

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
	Username      string `json:"username"`        // 客户端用户名
	FileName      string `json:"fileName"`        // 文件名
	Priority      int    `json:"priority"`        // 文件优先级
	ClientIP      string `json:"clientIP"`        // 目标服务器 IP 或域名
	ClientCont    string `json:"clientContinent"` // 客户端大区
	ServerIP      string `json:"serverIP"`        // 目标服务器 IP 或域名
	CloudProvider string `json:"cloudProvider"`   // 云服务提供商，例如 AWS, GCP, DO ////ServerCont
	CloudRegion   string `json:"cloudRegion"`     // 云服务所在区域，例如 us-east-1
	CloudCity     string `json:"cloudCity"`       // 云服务所在城市，例如 Ashburn
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

type FileSys struct {
	Upload string // 上传接口地址 // ``
	Merge  string // 合并接口地址
	Dir    string
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
//
// 返回值：
//
//	error: 整体错误（目录不存在/权限问题等），单个文件删除失败会记录警告但不中断整体流程
func DeleteFilesInDir(dir string, fileNames []string) error {
	// 1. 基础参数校验
	if dir == "" {
		return errors.New("目录路径不能为空")
	}
	if len(fileNames) == 0 {
		return errors.New("文件名数组不能为空")
	}

	// 2. 检查目录是否存在
	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("目录不存在: %s", dir)
		}
		return fmt.Errorf("获取目录信息失败: %w", err)
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("指定路径不是目录: %s", dir)
	}

	// 3. 遍历删除每个文件
	var failedFiles []string
	for _, fileName := range fileNames {
		// 过滤空文件名
		if strings.TrimSpace(fileName) == "" {
			fmt.Printf("警告：跳过空文件名\n")
			continue
		}

		// 拼接完整文件路径（自动处理路径分隔符，跨平台兼容）
		filePath := filepath.Join(dir, fileName)

		// 删除文件
		err := os.Remove(filePath)
		if err != nil {
			// 记录删除失败的文件，但不中断流程
			failedFiles = append(failedFiles, fmt.Sprintf("%s: %v", filePath, err))
			fmt.Printf("警告：删除文件失败 %s: %v\n", filePath, err)
			continue
		}

		fmt.Printf("成功删除文件: %s\n", filePath)
	}

	// 4. 处理删除失败的文件（如有）
	if len(failedFiles) > 0 {
		return fmt.Errorf("部分文件删除失败: %s", strings.Join(failedFiles, "; "))
	}

	return nil
}
