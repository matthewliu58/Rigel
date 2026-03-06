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

//todo 1、支持断点续传
//todo 2、支持记录io时间 传输时间
//todo 3、支持从storage读取(分片)文件
//todo 4、支持用scp从local disk读取(分片)文件

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

	api.CredFileSource = config.Config_.CredFileSource
	api.BucketNameSource = config.Config_.BucketNameSource
	api.CredFile = config.Config_.CredFile
	api.BucketName = config.Config_.BucketName
	api.LocalBaseDir = config.Config_.LocalBaseDir
	api.RemoteDiskDir = config.Config_.SSH.RemoteDir
	api.RemoteDiskSSHConfig = util.SSHConfig{
		User:     config.Config_.SSH.User,
		Host:     config.Config_.SSH.Host,
		Password: config.Config_.SSH.Password,
	}

	logger.Info("config data", slog.String("pre", pre), slog.Any("data", config.Config_))

	router := gin.New() // 不用 Default，这样我们自定义中间件
	router.Use(AccessLogMiddleware(logger))
	router.Use(gin.Recovery())

	//api.InitProxyUserRoutingRouter(router, RoutingURL, logger)
	router.POST("/api/v1/upload", api.Upload(logger))

	// 上传接口
	router.POST("/gcp/upload/client", api.ClientUploadHandler(logger))

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
