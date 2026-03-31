package health

import (
	"data-proxy/util"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
	"sync"
)

var (
	// 锁和状态变量
	statusLock sync.Mutex
	status     string = "on" // 默认状态为 "on"
)

// HealthStateChange 修改健康状态开关 /healthStateChange?set=on/off
func HealthStateChange(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("healthStateChange", slog.String("pre", pre))

		// 获取参数
		set := c.DefaultQuery("set", "on")
		logger.Info("get switch val", slog.String("pre", pre), slog.String("set", set))

		// 加锁修改状态
		statusLock.Lock()
		defer statusLock.Unlock()

		if set == "off" {
			status = "off"
		} else {
			status = "on"
		}

		c.JSON(http.StatusOK, "success")
	}
}

// Health 健康检查 /health
func Health(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)

		// 读锁也可以，我保持你原来的写法不动
		statusLock.Lock()
		defer statusLock.Unlock()

		logger.Info("health", slog.String("pre", pre), slog.String("status", status))

		if status == "off" {
			c.JSON(http.StatusInternalServerError, "error")
			return
		}
		c.JSON(http.StatusOK, "success")
	}
}
