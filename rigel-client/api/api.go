package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"rigel-client/config"
	"rigel-client/download"
	"rigel-client/upload"
	upload2 "rigel-client/upload/upload"
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

func V1ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("ClientUploadHandler", slog.String("pre", pre))

		// 从 Header 获取文件名
		fileName := c.GetHeader(FileName)
		//fileStart := c.GetHeader(FileStart)   // "0"
		//fileLength := c.GetHeader(FileLength) // "100"
		newFileName := c.GetHeader(NewFileName)
		sourceType := c.GetHeader(DataSourceType)
		destType := c.GetHeader(DataDestType)

		// 校验文件名 Header
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
			return
		}

		// 校验数据源类型 Header
		if sourceType == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing Data-Source-Type header"})
			return
		}

		// 校验数据目标类型 Header
		if destType == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing Data-Dest-Type header"})
			return
		}

		logger.Info("ClientUploadHandler", slog.String("pre", pre), slog.String("fileName", fileName),
			slog.String("sourceType", sourceType), slog.String("destType", destType))

		ctx := context.Background()
		if sourceType == download.GCPCLoud {
			reader, err := download.DownloadFromGCSbyClient(ctx, LocalBaseDir, BucketNameSource,
				fileName, newFileName, CredFileSource, 0, 0, false, pre, logger)
			if err != nil {
				logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			defer reader.Close()
		} else if sourceType == download.RemoteDisk {
			reader, _, err := download.SSHDDReadRangeChunk(ctx, RemoteDiskSSHConfig, RemoteDiskDir, fileName,
				newFileName, LocalBaseDir, 0, 0, "", false, pre, logger)
			if err != nil {
				logger.Error("SSHDDReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			defer reader.Close()
		}

		fileName = newFileName
		logger.Info("download objectName success", slog.String("pre", pre),
			slog.String("objectName", fileName))

		if destType == upload.GCPCLoud {
			if err := upload2.UploadToGCSbyClient(ctx, LocalBaseDir, BucketName, fileName, CredFile,
				false, nil, pre, logger); err != nil {
				logger.Error("UploadToGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else if destType == upload.RemoteDisk {
			req := upload2.ChunkUploadRequest{
				ServerURL:     FileSys.Upload,
				FinalFileName: fileName,
				ChunkName:     fileName,
				LocalBaseDir:  LocalBaseDir,
			}
			if _, err := upload2.UploadFileChunk(req, false, nil, pre, logger); err != nil {
				logger.Error("ChunkUploadHandler failed", slog.String("pre", pre), slog.Any("err", err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
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

func V2ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)
		logger.Info("ClientUploadHandler", slog.String("pre", pre))

		// 2. Header解析与严格校验
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
				return
			}
		}

		// 2.2 数值类型转换与校验
		fileStart, err := strconv.ParseInt(fileStartStr, 10, 64)
		if err != nil || fileStart < 0 {
			errMsg := fmt.Sprintf("Invalid %s: must be non-negative integer", FileStart)
			logger.Error(errMsg, slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		fileLength, err := strconv.ParseInt(fileLengthStr, 10, 64)
		if err != nil || fileLength <= 0 {
			errMsg := fmt.Sprintf("Invalid %s: must be positive integer", FileLength)
			logger.Error(errMsg, slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}

		// 2.3 源/目标类型合法性校验
		validSourceTypes := map[string]bool{download.GCPCLoud: true, download.RemoteDisk: true, download.LocalDisk: true}
		validDestTypes := map[string]bool{upload.GCPCLoud: true, upload.RemoteDisk: true}
		if !validSourceTypes[sourceType] {
			errMsg := fmt.Sprintf("Invalid source type: %s (supported: %v)",
				sourceType, []string{download.GCPCLoud, download.RemoteDisk, download.LocalDisk})
			logger.Error(errMsg, slog.String("pre", pre))
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}
		if !validDestTypes[destType] {
			errMsg := fmt.Sprintf("Invalid dest type: %s (supported: %v)",
				destType, []string{upload.GCPCLoud, upload.RemoteDisk})
			logger.Error(errMsg, slog.String("pre", pre))
			c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
			return
		}

		logger.Info("Header parse success", slog.String("pre", pre),
			slog.String("fileName", fileName), slog.String("newFileName", newFileName),
			slog.Int64("fileStart", fileStart), slog.Int64("fileLength", fileLength),
			slog.String("sourceType", sourceType), slog.String("destType", destType))

		uploadInfo := upload.UploadInfo{
			File: upload.FileInfo{
				Start:       fileStart,   // 分块起始偏移（根据实际需求填写，如0、1048576等）
				Length:      fileLength,  // 分块长度（如1MB，按需调整）
				FileName:    fileName,    // 源文件名称（后续填写，如"source.zip"）
				NewFileName: newFileName, // 目标文件名称（后续填写，如"target.zip"）
			},
			Source: upload.SourceInfo{
				SourceType: sourceType,                   // 源类型固定为disk
				User:       RemoteDiskSSHConfig.User,     // SSH用户名（后续填写，如"root"）
				Host:       RemoteDiskSSHConfig.Host,     // SSH主机IP（后续填写，如"192.168.1.100"）
				SSHPort:    "22",                         // SSH端口（默认22，可改）
				Password:   RemoteDiskSSHConfig.Password, // SSH密码（后续填写）
				RemoteDir:  RemoteDiskDir,                // 远程文件目录（后续填写，如"/data/files/"）
				// cloud类型字段无需填写，留空即可
				BucketName: BucketNameSource,
				CredFile:   CredFileSource,
			},
			Dest: upload.DestInfo{
				DestType: destType, // 目标类型固定为disk
				FileSys: util.FileSys{
					Upload: FileSys.Upload,
					Merge:  FileSys.Merge,
					Dir:    FileSys.Dir,
				}, // 本地文件系统实例（按需替换）
				// cloud类型字段无需填写，留空即可
				BucketName: BucketName,
				CredFile:   CredFile,
			},
			LocalBaseDir: LocalBaseDir, // 本地文件系统根目录（后续填写，如"/data/files/"）
		}

		logger.Info("UploadInfo", slog.String("pre", pre), slog.Any("uploadInfo", uploadInfo))

		if err := upload.Upload(uploadInfo, upload.UploadDirectImp, true, pre, logger); err != nil {
			logger.Error("UploadDirect failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":    "upload success",
			"file_name":  fileName,
			"bucket":     BucketName,
			"objectName": fileName,
		})
	}
}

func V1Upload(logger *slog.Logger) gin.HandlerFunc {
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

		fileName := c.GetHeader(FileName)
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
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
		bReq.Header.Set(FileName, fileName)
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

		//local disk
		localFilePath := LocalBaseDir + fileName
		uploadInfo := upload.UploadFileInfo{
			LocalFilePath: localFilePath,
			BucketName:    BucketName,
			FileName:      fileName,
			CredFile:      CredFile,
		}

		if err := upload.UploadToGCSbyReDirectImp(uploadInfo, routingInfo, pre, logger); err != nil {
			logger.Error("ReDirect v2 HTTPS upload failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":   "upload success",
			"file_name": fileName,
		})

	}
}

//func RedirectV1Handler(logger *slog.Logger) gin.HandlerFunc {
//	return func(c *gin.Context) {
//		fileName := c.GetHeader(HeaderFileName)
//		if fileName == "" {
//			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
//			return
//		}
//
//		hops := c.GetHeader("X-Hops") // "34.69.185.247:8090,136.116.114.219:8080"
//		if hops == "" {
//			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-Hops header"})
//			return
//		}
//
//		localFilePath := LocalBaseDir + fileName
//
//		if err := upload.UploadToGCSbyReDirectHttpsV1(localFilePath, BucketName, fileName, CredFile,
//			hops, c.Request.Header, logger); err != nil {
//			logger.Error("ReDirect HTTPS upload failed: %v", err)
//			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
//			return
//		}
//
//		c.JSON(http.StatusOK, gin.H{
//			"message":   "redirect upload success",
//			"file_name": fileName,
//		})
//	}
//}

//func DirectUploadHandler(logger *slog.Logger) gin.HandlerFunc {
//	return func(c *gin.Context) {
//		fileName := c.GetHeader(HeaderFileName)
//		if fileName == "" {
//			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name header"})
//			return
//		}
//
//		localFilePath := LocalBaseDir + fileName
//
//		if err := upload.UploadToGCSbyDirectHttps(localFilePath, BucketName, fileName, CredFile, logger); err != nil {
//			logger.Error("Direct HTTPS upload failed: %v", err)
//			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
//			return
//		}
//
//		c.JSON(http.StatusOK, gin.H{
//			"message":    "direct upload success",
//			"file_name":  fileName,
//			"bucket":     BucketName,
//			"objectName": fileName,
//		})
//	}
//}

//func RedirectV2Handler(logger *slog.Logger) gin.HandlerFunc {
//	return func(c *gin.Context) {
//
//		pre := util.GenerateRandomLetters(5)
//		logger.Info("RedirectV2Handler", slog.String("pre", pre))
//
//		var routingInfo util.RoutingInfo
//		if err := c.ShouldBindJSON(&routingInfo); err != nil {
//			c.JSON(http.StatusBadRequest, gin.H{
//				"error":  "invalid json body for routing",
//				"detail": err.Error(),
//			})
//			return
//		}
//
//		fileName := c.GetHeader(HeaderFileName)
//		sourceType := c.GetHeader(DataSourceType)
//		if fileName == "" {
//			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing X-File-Name or X-Data-Source-Type header"})
//			return
//		}
//		localFilePath := LocalBaseDir + fileName
//		ctx := context.Background()
//
//		if sourceType == "cloud" {
//			err := download.DownloadFromGCSbyClient(ctx, localFilePath, BucketNameSource,
//				fileName, CredFileSource, 0, 0, pre, logger)
//			if err != nil {
//				logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
//				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
//				return
//			}
//		}
//
//		//没有路径直传
//		if len(routingInfo.Routing) == 0 {
//			if err := upload.UploadToGCSbyClient(ctx, localFilePath, BucketName, fileName, CredFile, logger); err != nil {
//				logger.Error("Upload failed: %v", err)
//				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
//				return
//			}
//			c.JSON(http.StatusOK, gin.H{
//				"message":    "upload success",
//				"file_name":  fileName,
//				"bucket":     BucketName,
//				"objectName": fileName,
//			})
//			return
//		}
//
//		uploadInfo := upload.UploadFileInfo{
//			LocalFilePath: localFilePath,
//			BucketName:    BucketName,
//			FileName:      fileName,
//			CredFile:      CredFile,
//		}
//
//		if err := upload.UploadToGCSbyReDirectHttpsV2(uploadInfo, routingInfo, pre, logger); err != nil {
//			logger.Error("ReDirect v2 HTTPS upload failed: %v", err)
//			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
//			return
//		}
//
//		c.JSON(http.StatusOK, gin.H{
//			"message":   "redirect v2 upload success",
//			"file_name": fileName,
//		})
//	}
//}
