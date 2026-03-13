package main

import (
	"github.com/gin-gonic/gin"
	"log/slog"
	"os"
	"rigel-client/api"
	"rigel-client/config"
	"rigel-client/util"
	"time"
)

func main() {

	logDir := "log"
	_ = os.MkdirAll(logDir, 0755)
	logFile, err := os.OpenFile(logDir+"/client.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

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

	//api.InitProxyUserRoutingRouter(router, RoutingURL, logger)
	router.POST("/api/v1/proxy/upload", api.V1ProxyUploadHandler(logger))

	// 上传接口
	//router.POST("/api/v1/client/upload", api.V1ClientUploadHandler(logger))
	router.POST("/api/v2/client/upload", api.V2ClientUploadHandler(logger))

	// ========== 新增文件接收上传接口 ==========
	router.POST("/api/v1/chunk/upload", api.ChunkUploadHandler(logger)) // 分片上传（自定义名）
	router.POST("/api/v1/chunk/merge", api.ChunkMergeHandler(logger))   // 分片合并（指定顺序）

	// ========== 新增 HTTPS 直传 ==========
	//router.POST("/gcp/upload/direct", api.DirectUploadHandler(logger))
	//
	//router.POST("/gcp/upload/redirect/v1", api.RedirectV1Handler(logger))

	//router.POST("/gcp/upload/redirect/v2", api.RedirectV2Handler(logger))

	port := "8080"
	logger.Info("Starting server on port", slog.String("pre", pre), slog.String("port", port))
	router.Run(":" + port)
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
