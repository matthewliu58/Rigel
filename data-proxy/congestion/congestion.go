package congestion

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync/atomic"
)

const (
	BufferSize            = 64
	WarningLevelforBuffer = 0.6
	//CriticalLevelforBuffer = 0.8
)

// 当前正在“真实转发数据”的请求数
var (
	ActiveTransfers int64
)

type ProxyStatus struct {
	ActiveConnections int64   `json:"active_connections"` // 当前活跃连接数
	TotalMem          int64   `json:"total_mem"`          // 机器总内存（字节）
	ProcessMem        int64   `json:"process_mem"`        // 当前进程使用内存（字节）
	AvgCachePerConn   float64 `json:"avg_cache_per_conn"` // 平均每连接缓存大小（字节）
	CacheUsageRatio   float64 `json:"cache_usage_ratio"`  // 缓存使用比例 [0,1]
}

func CheckCongestion(allBufferSize int, logger *slog.Logger) ProxyStatus {

	s := ProxyStatus{}
	// 获取系统总内存大小
	totalMem, err := getTotalMem(logger)
	if err != nil {
		logger.Error("Failed to get total memory:", err)
		return s
	}
	s.TotalMem = totalMem

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	proxyMem := int64(m.Sys) // RSS 等价内存
	s.ProcessMem = proxyMem

	usageRatio := float64(proxyMem) / float64(totalMem)
	logger.Info(fmt.Sprintf(
		"Proxy memory: %v MiB, Total memory: %v MiB, Ratio: %.2f%%",
		proxyMem/1024/1024,
		totalMem/1024/1024,
		usageRatio*100,
	))

	//if usageRatio > WarningLevelforBuffer {
	perConnCache := allBufferSize * 1024 // 每连接 128 KB
	active := atomic.LoadInt64(&ActiveTransfers)
	if active <= 0 {
		return s
	}
	s.ActiveConnections = active

	avgCache := float64(proxyMem) / float64(active)
	logger.Info(fmt.Sprintf(
		"Active connections: %d, Average per-connection memory: %.2f KB",
		active,
		avgCache/1024,
	))

	s.AvgCachePerConn = avgCache
	s.CacheUsageRatio = avgCache / float64(perConnCache)

	if s.CacheUsageRatio > WarningLevelforBuffer {
		logger.Warn("Potential congestion: average per-connection buffer near 128KB")
	}

	logger.Info(fmt.Sprintf("Proxy status: %+v", s))
	return s
}

func getTotalMem(logger *slog.Logger) (int64, error) {

	currentLimit := debug.SetMemoryLimit(-1)
	return currentLimit, nil

	//file, err := os.Open("/proc/meminfo")
	//if err != nil {
	//	return 0, err
	//}
	//defer file.Close()
	//
	//scanner := bufio.NewScanner(file)
	//for scanner.Scan() {
	//	line := scanner.Text()
	//	// 找到 MemTotal 那一行
	//	if strings.HasPrefix(line, "MemTotal:") {
	//		parts := strings.Fields(line)
	//		if len(parts) < 3 {
	//			return 0, strconv.ErrSyntax
	//		}
	//		kb, err := strconv.ParseInt(parts[1], 10, 64)
	//		if err != nil {
	//			return 0, err
	//		}
	//		return kb * 1024, nil // 转成字节
	//	}
	//}
	//
	//return 0, os.ErrNotExist
}

// GetCongestionInfo 获取拥堵状态 /getCongestionInfo
func GetCongestionInfo(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		info := CheckCongestion(2*BufferSize, logger)
		c.JSON(http.StatusOK, info)
	}
}
