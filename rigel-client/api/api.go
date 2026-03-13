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

// V2ClientUploadHandler V2版本客户端直传文件处理器
// 核心流程：解析上传请求头 -> 直接调用客户端直传逻辑上传文件 -> 返回上传结果
// 区别于V1代理上传：无需调用B服务获取路由，直接完成文件上传
func V2ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 生成5位随机字符串作为请求唯一标识，用于日志追踪
		requestID := util.GenerateRandomLetters(5)
		logger.Info("V2ClientUploadHandler start", slog.String("requestID", requestID))

		// 1. 解析请求头信息，构建上传所需的基础信息（文件名、存储路径、客户端信息等）
		// 返回值说明：uploadInfo-上传核心信息；_（忽略值）-扩展字段；err-解析错误
		uploadInfo, _, err := ParseHeadersAndBuildUploadInfo(c, requestID, logger)
		if err != nil {
			return // 错误已在ParseHeadersAndBuildUploadInfo内部处理并返回响应
		}

		// 2. 调用客户端直传实现上传文件到存储服务（C服务）
		// UploadDirectImp：客户端直传实现（区别于V1的代理转发实现）
		// 参数说明：uploadInfo-上传信息；UploadDirectImp-直传实现函数；true-是否开启并发；requestID-请求标识；logger-日志实例
		if err := upload.Upload(uploadInfo, upload.UploadDirectImp, util.RoutingInfo{}, requestID, logger); err != nil {
			logger.Error("client direct upload failed", slog.String("requestID", requestID), slog.Any("err", err))
			// 返回500内部错误，携带具体错误信息
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 3. 上传成功，返回标准化响应
		logger.Info("V2ClientUploadHandler success", slog.String("requestID", requestID),
			slog.String("fileName", uploadInfo.File.FileName),
			slog.String("objectName", uploadInfo.File.NewFileName))
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by client success",  // 客户端直传成功提示
			"file_name":  uploadInfo.File.FileName,    // 原始文件名
			"objectName": uploadInfo.File.NewFileName, // 存储后的对象名（可能是重命名后的名称）
		})
	}
}

// V1ProxyUploadHandler 代理上传核心处理器
// 流程：解析请求 -> 调用B服务获取路由 -> 上传文件到C服务 -> 返回响应
func V1ProxyUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 生成请求唯一标识，用于日志追踪
		requestID := util.GenerateRandomLetters(5)
		logger.Info("V1ProxyUploadHandler start", slog.String("requestID", requestID))

		// 1. 解析请求头和请求体，构建上传基础信息
		uploadInfo, _, err := parseRequest(c, requestID, logger)
		if err != nil {
			return // 错误已在子函数中处理并返回响应
		}

		// 2. 调用B服务获取路由信息
		routingInfo, err := getRoutingInfoFromServiceB(c, requestID, logger)
		if err != nil {
			handleError(c, logger, requestID, http.StatusInternalServerError, "get routing info failed", err)
			return
		}
		if len(routingInfo.Routing) == 0 {
			handleError(c, logger, requestID, http.StatusBadRequest, "routing info is empty", nil)
			return
		}

		// 3. 上传文件到C服务
		if err := upload.Upload(uploadInfo, upload.UploadRedirectImp, routingInfo, requestID, logger); err != nil {
			handleError(c, logger, requestID, http.StatusInternalServerError, "upload to service C failed", err)
			return
		}

		// 4. 返回成功响应
		logger.Info("V1ProxyUploadHandler success", slog.String("requestID", requestID),
			slog.String("fileName", uploadInfo.File.FileName),
			slog.String("objectName", uploadInfo.File.NewFileName))
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by proxy success",
			"file_name":  uploadInfo.File.FileName,
			"objectName": uploadInfo.File.NewFileName,
		})
	}
}

// parseRequest 解析请求头、请求体，构建上传信息并记录日志
func parseRequest(c *gin.Context, requestID string, logger *slog.Logger) (upload.UploadInfo, []byte, error) {

	var uploadInfo upload.UploadInfo
	var err error

	// 解析Header构建UploadInfo
	uploadInfo, _, err = ParseHeadersAndBuildUploadInfo(c, requestID, logger)
	if err != nil {
		handleError(c, logger, requestID, http.StatusBadRequest, "parse headers failed", err)
		return uploadInfo, nil, err
	}

	// 读取并解析请求体
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		handleError(c, logger, requestID, http.StatusBadRequest, "read request body failed", err)
		return uploadInfo, nil, err
	}

	// 解析请求体为UserRouteRequest（仅用于日志）
	var req util.UserRouteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		handleError(c, logger, requestID, http.StatusBadRequest, "unmarshal request body failed", err)
		return uploadInfo, nil, err
	}
	logger.Info("parse request success", slog.String("requestID", requestID), slog.Any("userRequest", req))

	return uploadInfo, bodyBytes, nil
}

// getRoutingInfoFromServiceB 调用B服务获取路由信息
func getRoutingInfoFromServiceB(c *gin.Context, requestID string, logger *slog.Logger) (util.RoutingInfo, error) {
	// 重新读取请求体（避免流已关闭）
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return util.RoutingInfo{}, err
	}

	// 构建调用B服务的请求
	req, err := http.NewRequest("POST", config.Config_.ControlHost+RoutingURL, bytes.NewReader(bodyBytes))
	if err != nil {
		logger.Error("build service B request failed", slog.String("requestID", requestID), slog.String("err", err.Error()))
		return util.RoutingInfo{}, err
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	var userReq util.UserRouteRequest
	_ = json.Unmarshal(bodyBytes, &userReq) // 仅用于设置Header，忽略错误（已在parseRequest校验）
	req.Header.Set(FileName, userReq.FileName)
	req.Header.Set(ClientIP, userReq.ClientIP)
	req.Header.Set(UserName, userReq.Username)

	// 发送请求到B服务
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("call service B failed", slog.String("requestID", requestID), slog.String("err", err.Error()))
		return util.RoutingInfo{}, err
	}
	defer resp.Body.Close()

	// 读取B服务响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("read service B response failed", slog.String("requestID", requestID), slog.String("err", err.Error()))
		return util.RoutingInfo{}, err
	}

	// 解析B服务响应
	var apiResp util.ApiResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		logger.Error("unmarshal service B response failed", slog.String("requestID", requestID), slog.String("err", err.Error()))
		return util.RoutingInfo{}, err
	}

	// 解析路由信息
	reqDataBytes, _ := json.Marshal(apiResp.Data)
	logger.Info("get service B response", slog.String("requestID", requestID), slog.String("responseData", string(reqDataBytes)))
	var routingInfo util.RoutingInfo
	if err := json.Unmarshal(reqDataBytes, &routingInfo); err != nil {
		logger.Error("unmarshal routing info failed", slog.String("requestID", requestID), slog.String("err", err.Error()))
		return util.RoutingInfo{}, err
	}

	logger.Info("get routing info success", slog.String("requestID", requestID), slog.Any("routingInfo", routingInfo))
	return routingInfo, nil
}

// handleError 统一错误处理：记录日志并返回标准化响应
func handleError(c *gin.Context, logger *slog.Logger, requestID string, statusCode int, msg string, err error) {
	errMsg := msg
	if err != nil {
		errMsg = msg + ": " + err.Error()
	}
	logger.Error(errMsg, slog.String("requestID", requestID))
	c.JSON(statusCode, gin.H{
		"error": errMsg,
	})
}
