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
	"rigel-client/upload"
	"rigel-client/upload/base"
	"rigel-client/util"
	"time"
)

const (
	RoutingURL = "/api/v1/routing"
)

var (
	LocalBaseDir string
)

// ---------------------- 核心封装函数：Header解析+全量校验+构造UploadInfo ----------------------
// ParseHeadersAndBuildUploadInfo 一站式处理请求头解析、校验、UploadInfo构造
// 入参：Gin上下文、日志前缀、日志器
// 出参：构造好的UploadInfo / 是否已向客户端返回响应（避免重复响应）/ 错误信息
func ParseHeadersAndBuildUploadInfo(c *gin.Context, pre string, logger *slog.Logger) (base.UploadStruct, error) {

	logger.Info("Start parsing upload info", slog.String("pre", pre))

	//todo file size 可以通过 header传入 省的计算

	var req base.UploadStruct
	if err := c.ShouldBindJSON(&req); err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body: %v", err)
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return base.UploadStruct{}, fmt.Errorf(errMsg)
	}

	if req.Source.Type == util.LocalDisk {
		req.Proxy.LocalDir = LocalBaseDir
	}

	logger.Info("TransferConfig", slog.String("pre", pre), slog.Any("req", req))
	return req, nil
}

// V2ClientUploadHandler V2版本客户端直传文件处理器
// 核心流程：解析上传请求头 -> 直接调用客户端直传逻辑上传文件 -> 返回上传结果
// 区别于V1代理上传：无需调用B服务获取路由，直接完成文件上传
func V1ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 生成5位随机字符串作为请求唯一标识，用于日志追踪
		requestID := util.GenerateRandomLetters(5)
		logger.Info("V2ClientUploadHandler start", slog.String("requestID", requestID))

		// 1. 解析请求头信息，构建上传所需的基础信息（文件名、存储路径、客户端信息等）
		// 返回值说明：uploadInfo-上传核心信息；_（忽略值）-扩展字段；err-解析错误
		uploadInfo, err := ParseHeadersAndBuildUploadInfo(c, requestID, logger)
		if err != nil {
			return // 错误已在ParseHeadersAndBuildUploadInfo内部处理并返回响应
		}

		// 2. 调用客户端直传实现上传文件到存储服务（C服务）
		// UploadDirectImp：客户端直传实现（区别于V1的代理转发实现）
		// 参数说明：uploadInfo-上传信息；UploadDirectImp-直传实现函数；true-是否开启并发；requestID-请求标识；logger-日志实例
		if err := upload.UploadFunc(true, uploadInfo, upload.DirectImp,
			upload.RoutingInfo{}, false, requestID, logger); err != nil {
			logger.Error("client direct upload failed", slog.String("requestID", requestID),
				slog.Any("err", err))
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
		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ProxyUploadHandler start", slog.String("pre", pre))

		// 1. 解析请求头和请求体，构建上传基础信息
		uploadInfo, err := ParseHeadersAndBuildUploadInfo(c, pre, logger)
		if err != nil {
			return // 错误已在ParseHeadersAndBuildUploadInfo内部处理并返回响应
		}

		// 2. 调用B服务获取路由信息
		routingInfo, err := getRoutingInfoFromServiceControlPlane(uploadInfo, pre, logger)
		if err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "get routing info failed", err)
			return
		}
		if len(routingInfo.Routing) == 0 {
			handleError(c, logger, pre, http.StatusBadRequest, "routing info is empty", nil)
			return
		}

		// 3. 上传文件到C服务
		if err := upload.UploadFunc(false, uploadInfo, upload.RedirectImp,
			routingInfo, false, pre, logger); err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "upload to service C failed", err)
			return
		}

		// 4. 返回成功响应
		logger.Info("V1ProxyUploadHandler success", slog.String("pre", pre),
			slog.String("fileName", uploadInfo.File.FileName),
			slog.String("objectName", uploadInfo.File.NewFileName))
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by proxy success",
			"file_name":  uploadInfo.File.FileName,
			"objectName": uploadInfo.File.NewFileName,
		})
	}
}

// getRoutingInfoFromServiceB 调用B服务获取路由信息
func getRoutingInfoFromServiceControlPlane(uploadInfo base.UploadStruct, pre string, logger *slog.Logger) (upload.RoutingInfo, error) {

	// 构建调用B服务的请求
	reqBodyBytes, _ := json.Marshal(uploadInfo.EndPoints)
	req, err := http.NewRequest("POST", config.Config_.ControlHost+RoutingURL, bytes.NewReader(reqBodyBytes))
	if err != nil {
		logger.Error("build service B request failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(util.HeaderFileName, uploadInfo.File.NewFileName)

	// 发送请求到B服务
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("call service B failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}
	defer resp.Body.Close()

	// 读取B服务响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("read service B response failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	type ApiResponse struct {
		Code int         `json:"code"` // 200=成功，400=参数错误，500=服务端错误
		Msg  string      `json:"msg"`  // 提示信息
		Data interface{} `json:"data"` // 业务数据
	}

	// 解析B服务响应
	var apiResp ApiResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		logger.Error("unmarshal service B response failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	// 解析路由信息
	reqDataBytes, _ := json.Marshal(apiResp.Data)
	logger.Info("get service control plane response", slog.String("pre", pre), slog.String("responseData", string(reqDataBytes)))
	var routingInfo upload.RoutingInfo
	if err := json.Unmarshal(reqDataBytes, &routingInfo); err != nil {
		logger.Error("unmarshal routing info failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	logger.Info("get routing info success", slog.String("pre", pre), slog.Any("routingInfo", routingInfo))
	return routingInfo, nil
}

// handleError 统一错误处理：记录日志并返回标准化响应
func handleError(c *gin.Context, logger *slog.Logger, pre string, statusCode int, msg string, err error) {
	errMsg := msg
	if err != nil {
		errMsg = msg + ": " + err.Error()
	}
	logger.Error(errMsg, slog.String("pre", pre))
	c.JSON(statusCode, gin.H{
		"error": errMsg,
	})
}
