package congestion

import (
	"data-proxy/util"
	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/process"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
)

const (
	BufferSize            = 64
	WarningLevelforBuffer = 0.6
	//CriticalLevelforBuffer = 0.8
)

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

func CheckCongestion(allBufferSize int, pre string, logger *slog.Logger) ProxyStatus {

	s := ProxyStatus{}

	p, _ := process.NewProcess(int32(os.Getpid()))
	memInfo, _ := p.MemoryInfo()
	proxyMem := int64(memInfo.RSS)
	s.ProcessMem = proxyMem

	perConnCache := allBufferSize * 1024 // 每连接 128 KB
	active := atomic.LoadInt64(&ActiveTransfers)
	if active <= 0 {
		return s
	}
	s.ActiveConnections = active

	avgCache := float64(proxyMem) / float64(active)
	logger.Info("Proxy avg cache", slog.String("pre", pre), slog.Int64("proxyMem", proxyMem),
		slog.Int64("active", active), slog.Float64("avgCache", avgCache))

	s.AvgCachePerConn = avgCache
	s.CacheUsageRatio = avgCache / float64(perConnCache)

	if s.CacheUsageRatio > WarningLevelforBuffer {
		logger.Warn("Potential congestion: average per-connection buffer near 128KB",
			slog.String("pre", pre), slog.Int64("WarningLevelforBuffer", WarningLevelforBuffer))
	}

	logger.Info("Proxy status", slog.String("pre", pre), slog.Any("ProxyStatus", s))
	return s
}

// GetCongestionInfo 获取拥堵状态 /getCongestionInfo
func GetCongestionInfo(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		info := CheckCongestion(2*BufferSize, pre, logger)
		c.JSON(http.StatusOK, info)
	}
}
