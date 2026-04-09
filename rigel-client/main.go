package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/api"
	"rigel-client/config"
	"rigel-client/util"
	"runtime"
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

func AccessLogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		latency := time.Since(start)

		logger.Info("access",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", latency.Milliseconds(),
			"remote", c.ClientIP(),
			"content_length", c.Request.ContentLength,
			"user_agent", c.Request.UserAgent(),
		)
	}
}

func main() {

	logDir := "log"
	_ = os.MkdirAll(logDir, 0755)
	logFile, err := os.OpenFile(logDir+"/app.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	// 2. 配置基础TextHandler（保留原有Level等配置）
	baseHandler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo, // 日志级别
		AddSource: true,           // 必须开启！否则无法获取文件名/行号
	})

	// 3. 包装成自定义SourceHandler（添加文件名、行号、函数名）
	logger := slog.New(&SourceHandler{handler: baseHandler})

	// 4. 设置为全局logger（可选，整个项目都能生效）
	slog.SetDefault(logger)

	pre := util.GenerateRandomLetters(5)
	logger.Info("rigel client", slog.String("pre", pre))

	config.Config_, err = config.ReadYamlConfig(logger)
	if err != nil {
		logger.Error("read config error", slog.String("pre", pre), slog.Any("error", err))
		return
	}

	api.LocalBaseDir = config.Config_.LocalBaseDir

	logger.Info("config data", slog.String("pre", pre), slog.Any("data", config.Config_))

	router := gin.New() // 不用 Default，这样我们自定义中间件
	router.Use(AccessLogMiddleware(logger))
	router.Use(gin.Recovery())

	//proxy
	router.POST("/api/v1/proxy/upload", api.V1ProxyUploadHandler(logger))
	router.POST("/api/v1/proxy/large/upload", api.V1ProxyLargeUploadHandler(logger))

	//client
	router.POST("/api/v1/client/upload", api.V1ClientUploadHandler(logger))
	router.POST("/api/v1/client/large/upload", api.V1ClientLargeUploadHandler(logger))

	//chunk
	router.POST("/api/v1/chunk/upload", api.ChunkUploadHandler(logger)) // 分片上传（自定义名）
	router.POST("/api/v1/chunk/merge", api.ChunkMergeHandler(logger))   // 分片合并（指定顺序）

	port := "8080"
	logger.Info("Gin Run success", slog.String("pre", pre), slog.String("port", port))
	if err := router.Run(":" + port); err != nil {
		logger.Error("Gin Run failed", slog.String("pre", pre), slog.Any("err", err))
		return
	}
}
