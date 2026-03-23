package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log/slog"
	"net/http"
	"rigel-client/upload"
	"rigel-client/upload/base"
	"rigel-client/util"
	"sync"
	"time"
)

const (
	LargeFileUploadTimeout = 5 * time.Minute
)

type VM struct {
	IP     string `json:"ip" form:"ip"`
	Weight int64  `json:"weight" form:"weight"`
}

type LargeFile struct {
	VMs    []VM              `json:"vms" form:"vms"`
	Upload base.UploadStruct `json:"upload" form:"upload"`
}

func V1ProxyLargeUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 生成请求唯一标识，用于日志追踪
		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ProxyLargeUploadHandler start", slog.String("pre", pre))

		processUploadLogic(c, false, pre, logger)

		logger.Info("V1ProxyLargeUploadHandler end", slog.String("pre", pre))
	}
}

func V1ClientLargeUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 生成请求唯一标识，用于日志追踪
		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ClientLargeUploadHandler start", slog.String("pre", pre))

		processUploadLogic(c, true, pre, logger)

		logger.Info("V1ClientLargeUploadHandler end", slog.String("pre", pre))
	}
}

func processUploadLogic(c *gin.Context, cleintB bool, pre string, logger *slog.Logger) {
	// 1. 解析请求头和请求体，构建上传基础信息
	largeFile, err := ParseHeadersAndBuildUploadInfo_(c, pre, logger)
	if err != nil {
		return // 错误已在ParseHeadersAndBuildUploadInfo内部处理并返回响应
	}
	fo := upload.UploadFunc_(cleintB, largeFile.Upload, pre, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		cancel() // 无论正常/异常退出，都取消上下文
		logger.Info("Large upload context canceled", slog.String("pre", pre))
	}()

	up := largeFile.Upload
	if fo.GetFileSize.GetFileSize == nil {
		err := fmt.Errorf("GetFileSize is nil")
		logger.Error("GetFileSize is nil", slog.String("pre", pre), slog.String("error", err.Error()))
		handleError(c, logger, pre, http.StatusInternalServerError, "GetFileSize is nil", err)
		return // 补充return，原代码此处遗漏return会继续执行后续逻辑
	}
	l, err := fo.GetFileSize.GetFileSize(ctx, up.File.FileName, pre, logger)
	if err != nil {
		handleError(c, logger, pre, http.StatusInternalServerError, "GetFileSize failed", err)
		return
	}
	list, err := ProcessLargeFileUpload(cleintB, largeFile, l, pre, logger)
	if err != nil {
		handleError(c, logger, pre, http.StatusInternalServerError, "ProcessLargeFileUpload failed", err)
		return
	}
	if len(list) > 1 {
		list = util.SortPartStrings(list)
		if fo.ComposeFile.ComposeFile == nil {
			err := fmt.Errorf("ComposeFile is nil")
			logger.Error("ComposeFile is nil", slog.String("pre", pre), slog.String("error", err.Error()))
			handleError(c, logger, pre, http.StatusInternalServerError, "ComposeFile is nil", err)
			return // 补充return，原代码此处遗漏return会继续执行后续逻辑
		}
		err := fo.ComposeFile.ComposeFile(ctx, up.File.FileName, list, pre, logger)
		if err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "ComposeFile failed", err)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": list})
}

func ParseHeadersAndBuildUploadInfo_(c *gin.Context, pre string, logger *slog.Logger) (LargeFile, error) {

	logger.Info("Start parsing upload info", slog.String("pre", pre))

	var req LargeFile
	if err := c.ShouldBindJSON(&req); err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body: %v", err)
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return LargeFile{}, fmt.Errorf(errMsg)
	}

	logger.Info("ParseHeadersAndBuildUploadInfo_", slog.String("pre", pre), slog.Any("req", req))
	return req, nil
}

// 函数返回值：[]string（拆分后的文件名列表）, error
func ProcessLargeFileUpload(cleintB bool, largeFile LargeFile, size int64, pre string, logger *slog.Logger) ([]string, error) {
	// 存储拆分后的文件名列表
	var splitFileNames []string

	// 1. 基础参数校验
	logger.Info("开始处理大文件分片上传",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
	)

	if len(largeFile.VMs) == 0 {
		err := fmt.Errorf("没有可用的VM节点")
		logger.Error("参数校验失败",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	file := largeFile.Upload.File
	if file.FileLength == 0 {
		file.FileLength = size
		logger.Info("文件长度为0，使用传入的size赋值",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Int64("file_length", file.FileLength),
		)
	}

	// 2. 计算总权重和每个VM应分配的字节数
	totalWeight := int64(0)
	for _, vm := range largeFile.VMs {
		totalWeight += vm.Weight
	}
	logger.Info("计算VM总权重完成",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.Int64("total_weight", totalWeight),
	)

	if totalWeight == 0 {
		err := fmt.Errorf("所有VM的权重总和不能为0")
		logger.Error("权重校验失败",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	// 3. 计算分片（四舍五入确保总长度一致）
	var offset int64 = file.FileStart
	vmTasks := make(map[string]base.UploadStruct) // 存储每个VM对应的上传任务

	for i, vm := range largeFile.VMs {
		// 计算当前VM应分配的字节数（四舍五入）
		allocatedLength := (file.FileLength*vm.Weight + totalWeight/2) / totalWeight

		// 最后一个VM处理剩余字节（避免四舍五入导致总长度不一致）
		if i == len(largeFile.VMs)-1 {
			allocatedLength = file.FileStart + file.FileLength - offset
		}

		// 构建当前VM的上传结构体
		uploadTask := largeFile.Upload
		uploadTask.File.FileStart = offset
		uploadTask.File.FileLength = allocatedLength

		// 生成分片文件名
		splitFileName := fmt.Sprintf("%s_%d", file.NewFileName, i)
		uploadTask.File.NewFileName = splitFileName
		// 将分片文件名加入列表
		splitFileNames = append(splitFileNames, splitFileName)

		vmTasks[vm.IP] = uploadTask
		logger.Info("VM分片计算完成",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("vm_ip", vm.IP),
			slog.String("new_file_name", splitFileName),
		)

		offset += allocatedLength
	}

	// 4. 并发发送请求并等待所有结果
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := true
	var finalErr error

	logger.Info("开始并发发送上传请求",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.Int("task_count", len(vmTasks)),
		slog.Duration("timeout", LargeFileUploadTimeout),
	)

	for ip, task := range vmTasks {
		wg.Add(1)
		go func(ip string, task base.UploadStruct) {
			defer wg.Done()

			// 构造请求URL
			url := fmt.Sprintf("http://%s:8080/api/v1/proxy/upload", ip)
			if cleintB {
				url = fmt.Sprintf("http://%s:8080/api/v1/client/upload", ip)
			}

			// 序列化上传结构体为JSON
			jsonData, err := json.Marshal(task)
			if err != nil {
				mu.Lock()
				success = false
				finalErr = fmt.Errorf("序列化任务失败（%s）：%v", ip, err)
				mu.Unlock()
				logger.Error("任务序列化失败",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			// 发送POST请求
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				mu.Lock()
				success = false
				finalErr = fmt.Errorf("请求发送失败（%s）：%v", ip, err)
				mu.Unlock()
				logger.Error("HTTP请求发送失败",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}
			defer resp.Body.Close()

			// 核心校验：HTTP状态码是否为200（http.StatusOK）
			if resp.StatusCode != http.StatusOK {
				mu.Lock()
				success = false
				finalErr = fmt.Errorf("节点响应状态码异常（%s）：%d", ip, resp.StatusCode)
				mu.Unlock()
				logger.Error("VM响应状态码异常",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.Int("response_status", resp.StatusCode),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			// 读取响应体（仅处理读取失败场景，不打印内容）
			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				mu.Lock()
				success = false
				finalErr = fmt.Errorf("读取响应失败（%s）：%v", ip, err)
				mu.Unlock()
				logger.Error("响应体读取失败",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			logger.Info("VM上传请求处理成功",
				slog.String("pre", pre),
				slog.String("func", "ProcessLargeFileUpload"),
				slog.String("vm_ip", ip),
			)
		}(ip, task)
	}

	// 带超时的等待逻辑
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有协程正常完成
	case <-time.After(LargeFileUploadTimeout):
		// 超时触发
		finalErr = fmt.Errorf("上传任务超时（超时时间：%v），部分VM可能未完成处理", LargeFileUploadTimeout)
		logger.Error("上传任务超时",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Duration("timeout", LargeFileUploadTimeout),
			slog.String("error", finalErr.Error()),
		)
		return nil, finalErr
	}

	if success {
		logger.Info("所有VM上传请求处理完成，全部成功",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Any("split_file_names", splitFileNames),
		)
		return splitFileNames, nil
	}

	logger.Error("部分VM上传请求处理失败",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.String("error", finalErr.Error()),
		slog.Any("split_file_names", splitFileNames),
	)
	return nil, finalErr
}
