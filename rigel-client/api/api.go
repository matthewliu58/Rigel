package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"rigel-client/config"
	"rigel-client/download"
	"rigel-client/upload"
	"rigel-client/util"
	"strconv"
)

const (
	FileName       = "X-File-Name" // 通过 Header 传文件名
	FileStart      = "X-File-Start"
	FileLength     = "X-File-Length"
	NewFileName    = "X-New-File-Name"
	DataSourceType = "X-Data-Source-Type"
	DataDestType   = "X-Data-Dest-Type"
	ClientIP       = "X-Client-IP"
	UserName       = "X-User-Name"
	RoutingURL     = "/api/v1/routing"
)

var (
	RemoteDiskSSHConfig util.SSHConfig
	RemoteDiskDir       string

	CredFileSource   string
	BucketNameSource string

	FileSys util.FileSys

	CredFile   string
	BucketName string

	LocalBaseDir string
)

// ---------------------- 核心封装函数：Header解析+全量校验+构造UploadInfo ----------------------
// ParseHeadersAndBuildUploadInfo 一站式处理请求头解析、校验、UploadInfo构造
// 入参：Gin上下文、日志前缀、日志器
// 出参：构造好的UploadInfo / 是否已向客户端返回响应（避免重复响应）/ 错误信息
func ParseHeadersAndBuildUploadInfo(c *gin.Context, pre string, logger *slog.Logger) (upload.UploadInfo, bool, error) {

	logger.Info("Start parsing headers and building upload info", slog.String("pre", pre))

	// 1. 解析请求头
	fileName := c.GetHeader(FileName)
	fileStartStr := c.GetHeader(FileStart)
	fileLengthStr := c.GetHeader(FileLength)
	newFileName := c.GetHeader(NewFileName)
	sourceType := c.GetHeader(DataSourceType)
	destType := c.GetHeader(DataDestType)

	// 2.1 必传项校验
	requiredChecks := map[string]string{
		FileName:       fileName,
		FileStart:      fileStartStr,
		FileLength:     fileLengthStr,
		DataSourceType: sourceType,
		DataDestType:   destType,
	}
	for header, value := range requiredChecks {
		if value == "" {
			errMsg := fmt.Sprintf("Missing required header: %s", header)
			logger.Error(errMsg, slog.String("pre", pre))
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
		}
	}

	// 2.2 数值类型转换与校验
	fileStart, err := strconv.ParseInt(fileStartStr, 10, 64)
	if err != nil || fileStart < 0 {
		errMsg := fmt.Sprintf("Invalid %s: must be non-negative integer", FileStart)
		logger.Error(errMsg, slog.String("pre", pre), slog.Any("err", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
	}

	fileLength, err := strconv.ParseInt(fileLengthStr, 10, 64)
	if err != nil || fileLength <= 0 {
		errMsg := fmt.Sprintf("Invalid %s: must be positive integer", FileLength)
		logger.Error(errMsg, slog.String("pre", pre), slog.Any("err", err))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
	}

	// 2.3 源/目标类型合法性校验
	validSourceTypes := map[string]bool{download.GCPCLoud: true, download.RemoteDisk: true, download.LocalDisk: true}
	if !validSourceTypes[sourceType] {
		errMsg := fmt.Sprintf("Invalid source type: %s (supported: %v)",
			sourceType, []string{download.GCPCLoud, download.RemoteDisk, download.LocalDisk})
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
	}

	validDestTypes := map[string]bool{upload.GCPCLoud: true, upload.RemoteDisk: true}
	if !validDestTypes[destType] {
		errMsg := fmt.Sprintf("Invalid dest type: %s (supported: %v)",
			destType, []string{upload.GCPCLoud, upload.RemoteDisk})
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
	}

	logger.Info("Header parse success", slog.String("pre", pre),
		slog.String("fileName", fileName), slog.String("newFileName", newFileName),
		slog.Int64("fileStart", fileStart), slog.Int64("fileLength", fileLength),
		slog.String("sourceType", sourceType), slog.String("destType", destType))

	// 3. 构造UploadInfo
	uploadInfo := upload.UploadInfo{
		File: upload.FileInfo{
			Start:       fileStart,
			Length:      fileLength,
			FileName:    fileName,
			NewFileName: newFileName,
		},
		Source: upload.SourceInfo{
			SourceType: sourceType,
			User:       RemoteDiskSSHConfig.User,
			Host:       RemoteDiskSSHConfig.Host,
			SSHPort:    "22",
			Password:   RemoteDiskSSHConfig.Password,
			RemoteDir:  RemoteDiskDir,
			BucketName: BucketNameSource,
			CredFile:   CredFileSource,
		},
		Dest: upload.DestInfo{
			DestType: destType,
			FileSys: util.FileSys{
				Upload: FileSys.Upload,
				Merge:  FileSys.Merge,
				Dir:    FileSys.Dir,
			},
			BucketName: BucketName,
			CredFile:   CredFile,
		},
		LocalBaseDir: LocalBaseDir,
	}

	logger.Info("UploadInfo built success", slog.String("pre", pre), slog.Any("uploadInfo", uploadInfo))
	return uploadInfo, false, nil
}

func V2ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("V2ClientUploadHandler", slog.String("pre", pre))

		uploadInfo, _, err := ParseHeadersAndBuildUploadInfo(c, pre, logger)
		if err != nil {
			return
		}

		if err := upload.Upload(uploadInfo, upload.UploadDirectImp, true, pre, logger); err != nil {
			logger.Error("UploadDirectImp failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by client success",
			"file_name":  uploadInfo.File.FileName,
			"objectName": uploadInfo.File.NewFileName,
		})
	}
}

func V1ProxyUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ProxyUploadHandler", slog.String("pre", pre))

		//1、获取入参 header and form-------------------------------
		uploadInfo, _, err := ParseHeadersAndBuildUploadInfo(c, pre, logger)
		if err != nil {
			return
		}
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
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求体解析失败" + err.Error()})
			return
		}
		logger.Info("Proxy UserRoute request", slog.String("pre", pre), slog.Any("req", req))

		//2、构建请求转发给B 获取路由---------------------------------------------------------
		bReq, err := http.NewRequest("POST",
			config.Config_.ControlHost+RoutingURL, bytes.NewReader(bodyBytes))
		if err != nil {
			logger.Error("http NewRequest failed", slog.String("pre", pre),
				slog.String("err", err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		bReq.Header.Set("Content-Type", "application/json")
		bReq.Header.Set(FileName, req.FileName)
		bReq.Header.Set(ClientIP, req.ClientIP)
		bReq.Header.Set(UserName, req.Username)
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
		var bApiResp util.ApiResponse
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

		//3、上传文件到C---------------------------------------------------------
		if err := upload.Upload(uploadInfo, upload.UploadRedirectImp, true, pre, logger); err != nil {
			logger.Error("UploadRedirectImp failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		//4、返回响应---------------------------------------------------------
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by proxy success",
			"file_name":  uploadInfo.File.FileName,
			"objectName": uploadInfo.File.NewFileName,
		})

	}
}
