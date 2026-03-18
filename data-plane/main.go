package main

import (
	"context"
	"data-plane/pkg/envoy_manager"
	"data-plane/pkg/local_info_report/reporter"
	"data-plane/probing"
	"data-plane/util"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func InitEnvoy(logger, logger1 *slog.Logger) {
	// 创建启动器
	starter := envoy_manager.NewEnvoyStarter()

	// 启动Envoy
	pid, err := starter.StartEnvoy(logger, logger1)
	if err != nil {
		logger.Error("Envoy启动失败: %v", err)
	}
	logger.Info("Envoy启动成功，PID: %d", pid)
}

var (
	// 锁和状态变量
	statusLock sync.Mutex
	status     string = "on" // 默认状态为 "on"
)

func main() {

	// 创建 log 目录（与 pkg 同级）
	logDir := filepath.Join(".", "log")
	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		panic("无法创建日志目录: " + err.Error())
	}
	logFilePath := filepath.Join(logDir, "app.log")
	//logFilePath1 := filepath.Join(logDir, "envoy.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("无法打开日志文件: " + err.Error())
	}
	//logFile1, err := os.OpenFile(logFilePath1, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	panic("无法打开日志文件: " + err.Error())
	//}

	// 初始化日志，输出到 log/app.log
	logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	//logger1 := slog.New(slog.NewTextHandler(logFile1, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))

	logPre := "init"

	util.Config_, err = util.ReadYamlConfig(logger)
	if err != nil {
		logger.Error("read config failed", slog.String("pre", logPre), slog.Any("err", err))
		return
	} else {
		b, _ := json.Marshal(util.Config_)
		logger.Info("读取配置文件成功", slog.String("pre", logPre),
			slog.String("config", string(b)))
	}
	//envoy_manager.EnvoyPath = util.Config_.EnvoyPath
	//envoy_manager.DefaultConfig = util.Config_.DefaultConfig
	//envoy_manager.EnvoyLog = util.Config_.EnvoyLog

	// 2. 初始化Gin路由
	router := gin.Default()

	router.GET("/healthStateChange", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		logger.Info("healthStateChange", slog.String("pre", pre))

		// 获取查询参数 "set"
		set := c.DefaultQuery("set", "on") // 默认值为 "on"，即默认返回 200

		logger.Info("get switch val", slog.String("pre", pre))

		// 锁住状态修改操作，保证并发安全
		statusLock.Lock()
		defer statusLock.Unlock()

		// 根据 set 参数来决定状态值和返回的状态码
		if set == "off" {
			// set 为 "off"，修改状态为 "off"，并返回 500
			status = "off"
		} else {
			// 默认情况或 set 为 "on" 时，修改状态为 "on"，并返回 200
			status = "on"
		}
		c.JSON(http.StatusOK, "success")
	})

	router.GET("/health", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		// 锁住状态修改操作，保证并发安全
		statusLock.Lock()
		defer statusLock.Unlock()

		logger.Info("health", slog.String("pre", pre), slog.String("status", status))

		if status == "off" {
			c.JSON(http.StatusInternalServerError, "error")
			return
		}
		c.JSON(http.StatusOK, "success")
	})

	// 3. 初始化上报器
	go reporter.ReportCycle(util.Config_.ControlHost, logPre, logger)

	//启动探测逻辑
	cfg := probing.Config{
		Concurrency: 4,
		Timeout:     2 * time.Second,
		Interval:    5 * time.Second,
		Attempts:    5, // 每轮尝试次数
	}
	ctx := context.Background()
	probing.StartProbePeriodically(ctx, util.Config_.ControlHost, cfg, logPre, logger)
	//logger.Info("", "probe result", probingResult)

	//InitEnvoy(logger, logger1)

	// 4. 启动API服务
	logger.Info("API端口启动", slog.String("pre", logPre), "addr", ":8082")
	if err := router.Run(":8082"); err != nil {
		logger.Error("API服务启动失败", slog.String("pre", logPre), "error", err)
		os.Exit(1)
	}
}
