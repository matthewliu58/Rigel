package main

import (
	"context"
	"data-plane/pkg/envoy-manager"
	"data-plane/pkg/report-info/reporter"
	"data-plane/probing"
	"data-plane/util"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// 自定义Handler：修复slog.Context为context.Context，兼容所有Go 1.21+版本
type SourceHandler struct {
	handler slog.Handler
}

// Handle 核心修复：把slog.Context改为context.Context
func (h *SourceHandler) Handle(ctx context.Context, r slog.Record) error {
	// 采集调用日志的位置（跳过当前Handler的栈帧，取真实业务代码的位置）
	fs := runtime.CallersFrames([]uintptr{r.PC})
	frame, _ := fs.Next()

	// 只保留文件名（去掉全路径）
	fileName := filepath.Base(frame.File)

	// 向日志记录中添加源位置字段
	r.AddAttrs(
		slog.String("file", fileName),          // 文件名
		slog.Int("line", frame.Line),           // 行号
		slog.String("func", frame.Func.Name()), // 函数名（可选）
	)

	// 交给底层TextHandler输出
	return h.handler.Handle(ctx, r)
}

// 以下是slog.Handler接口的默认实现（全部修正为context.Context）
func (h *SourceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *SourceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SourceHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *SourceHandler) WithGroup(name string) slog.Handler {
	return &SourceHandler{handler: h.handler.WithGroup(name)}
}

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
	//logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))
	//logger1 := slog.New(slog.NewTextHandler(logFile1, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))

	// 2. 配置基础TextHandler（保留原有Level等配置）
	baseHandler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo, // 日志级别
		AddSource: true,           // 必须开启！否则无法获取文件名/行号
	})

	// 3. 包装成自定义SourceHandler（添加文件名、行号、函数名）
	logger := slog.New(&SourceHandler{handler: baseHandler})

	// 4. 设置为全局logger（可选，整个项目都能生效）
	slog.SetDefault(logger)

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
	logger.Info("API端口启动", slog.String("pre", logPre), slog.String("addr", ":8082"))
	if err := router.Run(":8082"); err != nil {
		logger.Error("API服务启动失败", slog.String("pre", logPre), slog.Any("err", err))
		return
	}
}
