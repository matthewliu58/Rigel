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
)

const (
	FileName       = "X-File-Name" // 通过 Header 传文件名
	DataSourceType = "X-Data-Source-Type"
	DataDestType   = "X-Data-Dest-Type"
	ClientIP       = "X-Client-IP"
	UserName       = "X-User-Name"
	RoutingURL     = "/api/v1/routing"
)

// TransferConfig 适配Form表单传输的文件传输配置结构体
type TransferConfig struct {
	// ------------- 核心Form参数 -------------
	FileName       string `json:"file_name" form:"file_name"`               // 源文件名（如test.zip）
	FileStart      int64  `json:"file_start" form:"file_start"`             // 文件起始偏移（字节，默认0）
	FileLength     int64  `json:"file_length" form:"file_length"`           // 文件传输长度（字节，0=整个文件）
	NewFileName    string `json:"new_file_name" form:"new_file_name"`       // 目标文件名
	DataSourceType string `json:"data_source_type" form:"data_source_type"` // 源类型
	DataDestType   string `json:"data_dest_type" form:"data_dest_type"`     // 目标类型

	// ------------- 源文件存储配置 -------------
	Source struct {
		// SSH配置（仅DataSourceType=remote_ssh时生效）
		SSH struct {
			User      string `json:"ssh_user" form:"ssh_user"`             // SSH用户名
			Host      string `json:"ssh_host" form:"ssh_host"`             // SSH主机IP
			SSHPort   string `json:"ssh_port" form:"ssh_port"`             // SSH端口
			Password  string `json:"ssh_password" form:"ssh_password"`     // SSH密码
			RemoteDir string `json:"ssh_remote_dir" form:"ssh_remote_dir"` // 远端文件目录
		} `json:"ssh" form:"ssh"`

		// GCP源配置（仅DataSourceType=gcp时生效）
		CredFile   string `json:"gcp_source_cred_file" form:"gcp_source_cred_file"` // GCP凭证文件
		BucketName string `json:"gcp_source_bucket" form:"gcp_source_bucket"`       // GCP存储桶
	} `json:"source" form:"source"`

	// ------------- 目标文件存储配置 -------------
	Target struct {
		// GCP目标配置（仅DataDestType=gcp时生效）
		CredFile   string `json:"gcp_dest_cred_file" form:"gcp_dest_cred_file"` // GCP凭证文件
		BucketName string `json:"gcp_dest_bucket" form:"gcp_dest_bucket"`       // GCP存储桶

		// 文件系统接口配置（仅DataDestType=api时生效）
		FileSys struct {
			Upload string `json:"file_sys_upload" form:"file_sys_upload"` // 上传接口地址
			Merge  string `json:"file_sys_merge" form:"file_sys_merge"`   // 合并接口地址
		} `json:"file_sys" form:"file_sys"`
	} `json:"target" form:"target"`

	// ------------- 传输通用配置 -------------
	//Transfer struct {
	//	ChunkSize   int64 `json:"chunk_size" form:"chunk_size"`     // 分块大小（字节，默认512MB）
	//	Timeout     int64 `json:"timeout" form:"timeout"`           // 整体超时（秒，默认300）
	//	RateLimit   int   `json:"rate_limit" form:"rate_limit"`     // 速率限制（Mbps，0=不限）
	//	Concurrency int   `json:"concurrency" form:"concurrency"`   // 并发数（默认10）
	//} `json:"transfer" form:"transfer"`
}

var (
	LocalBaseDir string
)

// ---------------------- 核心封装函数：Header解析+全量校验+构造UploadInfo ----------------------
// ParseHeadersAndBuildUploadInfo 一站式处理请求头解析、校验、UploadInfo构造
// 入参：Gin上下文、日志前缀、日志器
// 出参：构造好的UploadInfo / 是否已向客户端返回响应（避免重复响应）/ 错误信息
func ParseHeadersAndBuildUploadInfo(c *gin.Context, pre string, logger *slog.Logger) (upload.UploadInfo, bool, error) {

	fileName := c.GetHeader(FileName)
	logger.Info("Start parsing headers and building upload info", slog.String("pre", pre),
		slog.String("fileName", fileName))

	var req TransferConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body: %v", err)
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return upload.UploadInfo{}, true, fmt.Errorf(errMsg)
	}

	logger.Info("TransferConfig", slog.String("pre", pre), slog.Any("req", req))

	// 1. 解析请求头
	fileName = req.FileName
	fileStart := req.FileStart
	fileLength := req.FileLength
	newFileName := req.NewFileName
	sourceType := req.DataSourceType
	destType := req.DataDestType

	// 2.1 必传项校验
	requiredChecks := map[string]string{
		FileName:       fileName,
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
			User:       req.Source.SSH.User,
			HostPort:   req.Source.SSH.Host + ":" + req.Source.SSH.SSHPort,
			//SSHPort:    "22",
			Password:   req.Source.SSH.Password,
			RemoteDir:  req.Source.SSH.RemoteDir,
			BucketName: req.Source.BucketName,
			CredFile:   req.Source.CredFile,
		},
		Dest: upload.DestInfo{
			DestType: destType,
			FileSys: util.FileSys{
				Upload: req.Target.FileSys.Upload,
				Merge:  req.Target.FileSys.Merge,
			},
			BucketName: req.Target.BucketName,
			CredFile:   req.Target.CredFile,
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
