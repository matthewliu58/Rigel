package api

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"rigel-client/config"
	"rigel-client/download"
	"rigel-client/upload"
	"rigel-client/util"
)

const (
	HeaderFileName = "X-File-Name" // 通过 Header 传文件名
	DataSourceType = "X-Data-Source-Type"
	//BucketName     = "rigel-data"
	//credFile       = "/home/matth/civil-honor-480405-e0-e62b994bbc27.json"
	//localBaseDir   = "/home/matth/upload/" // 本地文件目录前缀
	//HOST           = "http://127.0.0.1:8081" //可以通过geoDNS获取
	RoutingURL = "/api/v1/routing"
)

var (
	RemoteDiskSSHConfig util.SSHConfig
	RemoteDiskDir       string

	CredFileSource   string
	BucketNameSource string

	CredFile   string
	BucketName string

	LocalBaseDir string
)

type ApiResponse struct {
	Code int         `json:"code"` // 200=成功，400=参数错误，500=服务端错误
	Msg  string      `json:"msg"`  // 提示信息
	Data interface{} `json:"data"` // 业务数据
}

//type UserRouteRequest struct {
//	FileName   string `json:"fileName"` // 文件名
//	Priority   int    `json:"priority"`
//	ClientCont string `json:"clientContinent"`
//	ServerIP   string `json:"serverIP"`
//	ServerCont string `json:"serverContinent"`
//	Username   string `json:"username"`
//}

func RedirectV1Handler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		fileName := c.GetHeader(HeaderFileName)
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
			return
		}

		hops := c.GetHeader("X-Hops") // "34.69.185.247:8090,136.116.114.219:8080"
		if hops == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-Hops header"})
			return
		}

		localFilePath := LocalBaseDir + fileName

		if err := upload.UploadToGCSbyReDirectHttpsV1(localFilePath, BucketName, fileName, CredFile,
			hops, c.Request.Header, logger); err != nil {
			logger.Error("ReDirect HTTPS upload failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":   "redirect upload success",
			"file_name": fileName,
		})
	}
}

func ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("ClientUploadHandler", slog.String("pre", pre))

		// 从 Header 获取文件名
		fileName := c.GetHeader(HeaderFileName)
		sourceType := c.GetHeader(DataSourceType)
		if fileName == "" || sourceType == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name or X-Data-Source-Type header"})
			return
		}

		logger.Info("ClientUploadHandler", slog.String("pre", pre),
			slog.String("fileName", fileName), slog.String("sourceType", sourceType))

		localFilePath := LocalBaseDir + fileName
		ctx := context.Background()

		if sourceType == "gcp-cloud" {
			err := download.DownloadFromGCSbyClient(ctx, localFilePath, BucketNameSource,
				fileName, CredFileSource, 0, 0, pre, logger)
			if err != nil {
				logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else if sourceType == "remote-disk" {
			localFileName, err := download.SSHDDReadRangeChunk(ctx, RemoteDiskSSHConfig, RemoteDiskDir, fileName,
				localFilePath, 0, 0, "", pre, logger)
			if err != nil {
				logger.Error("SSHDDReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			fileName = localFileName
		}

		if err := upload.UploadToGCSbyClient(ctx, localFilePath, BucketName, fileName, CredFile, logger); err != nil {
			logger.Error("UploadToGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		logger.Info("ClientUploadHandler success", slog.String("pre", pre))

		c.JSON(http.StatusOK, gin.H{
			"message":    "upload success",
			"file_name":  fileName,
			"bucket":     BucketName,
			"objectName": fileName,
		})
	}
}

func DirectUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		fileName := c.GetHeader(HeaderFileName)
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
			return
		}

		localFilePath := LocalBaseDir + fileName

		if err := upload.UploadToGCSbyDirectHttps(localFilePath, BucketName, fileName, CredFile, logger); err != nil {
			logger.Error("Direct HTTPS upload failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":    "direct upload success",
			"file_name":  fileName,
			"bucket":     BucketName,
			"objectName": fileName,
		})
	}
}

func RedirectV2Handler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("RedirectV2Handler", slog.String("pre", pre))

		var routingInfo util.RoutingInfo
		if err := c.ShouldBindJSON(&routingInfo); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "invalid json body for routing",
				"detail": err.Error(),
			})
			return
		}

		fileName := c.GetHeader(HeaderFileName)
		sourceType := c.GetHeader(DataSourceType)
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name or X-Data-Source-Type header"})
			return
		}
		localFilePath := LocalBaseDir + fileName
		ctx := context.Background()

		if sourceType == "cloud" {
			err := download.DownloadFromGCSbyClient(ctx, localFilePath, BucketNameSource,
				fileName, CredFileSource, 0, 0, pre, logger)
			if err != nil {
				logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}

		//没有路径直传
		if len(routingInfo.Routing) == 0 {
			if err := upload.UploadToGCSbyClient(ctx, localFilePath, BucketName, fileName, CredFile, logger); err != nil {
				logger.Error("Upload failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"message":    "upload success",
				"file_name":  fileName,
				"bucket":     BucketName,
				"objectName": fileName,
			})
			return
		}

		uploadInfo := upload.UploadFileInfo{
			LocalFilePath: localFilePath,
			BucketName:    BucketName,
			FileName:      fileName,
			CredFile:      CredFile,
		}

		if err := upload.UploadToGCSbyReDirectHttpsV2(uploadInfo, routingInfo, pre, logger); err != nil {
			logger.Error("ReDirect v2 HTTPS upload failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":   "redirect v2 upload success",
			"file_name": fileName,
		})
	}
}

func Upload(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("Upload", slog.String("pre", pre))

		//读取客户端请求 body
		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "读取请求失败" + err.Error(),
			})
			return
		}

		//解析 body 用于日志
		var req util.UserRouteRequest
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "请求体解析失败" + err.Error(),
			})
			return
		}

		fileName := c.GetHeader(HeaderFileName)
		sourceType := c.GetHeader(DataSourceType)
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name or X-Data-Source-Type header"})
			return
		}

		clientIP := c.GetHeader("X-Client-IP")
		if clientIP == "" {
			clientIP = c.ClientIP()
		}
		username := c.GetHeader("X-Username")

		b, _ := json.Marshal(req)
		logger.Info("Proxy UserRoute request", slog.String("pre", pre),
			"h-clientIP", clientIP, "h-username", username,
			"h-fileName", fileName, slog.String("", string(b)))

		//构建请求转发给B
		bReq, err := http.NewRequest("POST",
			config.Config_.ControlHost+RoutingURL, bytes.NewReader(bodyBytes))
		if err != nil {
			logger.Error("http NewRequest failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		bReq.Header.Set("Content-Type", "application/json")
		bReq.Header.Set(HeaderFileName, fileName)
		bReq.Header.Set("X-Client-IP", clientIP)
		bReq.Header.Set("X-User-Name", username)

		client := &http.Client{}
		bResp, err := client.Do(bReq)
		if err != nil {
			logger.Error("http Do failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer bResp.Body.Close()

		//读取B响应 body
		bRespBody, err := io.ReadAll(bResp.Body)
		if err != nil {
			logger.Error("io ReadAll failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		//解析B的 JSON 成 ApiResponse
		var bApiResp ApiResponse
		if err := json.Unmarshal(bRespBody, &bApiResp); err != nil {
			logger.Error("json Unmarshal ApiResponse failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		reqDataBytes, _ := json.Marshal(bApiResp.Data)
		logger.Info("Proxy UserRoute response", slog.String("pre", pre),
			slog.String("reqDataBytes", string(reqDataBytes)))

		var routingInfo util.RoutingInfo
		if err := json.Unmarshal(reqDataBytes, &routingInfo); err != nil {
			logger.Error("json Unmarshal ApiResponse failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		logger.Info("Proxy UserRoute response", slog.String("pre", pre),
			slog.Any("routingInfo", routingInfo))

		if len(routingInfo.Routing) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "routing info is empty",
			})
			return
		}

		ctx := context.Background()
		localFilePath := LocalBaseDir + fileName

		if sourceType == "gcp-cloud" {
			err := download.DownloadFromGCSbyClient(ctx, localFilePath, BucketNameSource,
				fileName, CredFileSource, 0, 0, pre, logger)
			if err != nil {
				logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}

		uploadInfo := upload.UploadFileInfo{
			LocalFilePath: localFilePath,
			BucketName:    BucketName,
			FileName:      fileName,
			CredFile:      CredFile,
		}

		if err := upload.UploadToGCSbyReDirectHttpsV2(uploadInfo, routingInfo, pre, logger); err != nil {
			logger.Error("ReDirect v2 HTTPS upload failed",
				slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":   "upload success",
			"file_name": fileName,
		})

	}
}
