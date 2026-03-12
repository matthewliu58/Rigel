package upload

import (
	"bytes"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/limit_rate"
	"rigel-client/util"
	"strings"
	"time"
)

const (
	HeaderFileName  = "X-File-Name"  // 最终合并后的文件名
	HeaderChunkName = "X-Chunk-Name" // 单个分片的自定义名称
)

// ChunkUploadRequest 分片上传请求参数
type ChunkUploadRequest struct {
	ServerURL     string // 接口地址（如 http://127.0.0.1:8080/api/v1/chunk/upload）
	FinalFileName string // 最终合并后的文件名（对应 X-File-Name）
	ChunkName     string // 当前分片名称（对应 X-Chunk-Name）
	LocalBaseDir  string // 分片文件的目录
}

// UploadFileChunk 向 ChunkUploadHandler 接口上传单个分片文件（支持本地文件/内存流式双模式）
// 核心改动：新增inMemory和dataReader参数，其余参数/逻辑完全保留
// 参数说明（原参数顺序不变，最后新增2个参数）：
//
//	req: 分片上传请求参数
//	pre: 日志前缀
//	logger: 日志对象
//	inMemory: 新增！true=内存流式上传（从dataReader读取），false=本地文件上传（原逻辑）
//	dataReader: 新增！inMemory=true时传入的数据源Reader（如SSH/GC SReader）
func UploadFileChunk(
	req ChunkUploadRequest,
	inMemory bool,        // 新增：内存模式开关
	dataReader io.Reader, // 新增：内存模式的数据源Reader
	pre string,
	logger *slog.Logger,
) (*http.Response, error) {

	logger.Info("UploadFileChunk", "req", req, slog.String("pre", pre), slog.Bool("inMemory", inMemory))

	// 1. 基础入参校验（完全保留原逻辑）
	if req.ServerURL == "" {
		return nil, fmt.Errorf("server URL 不能为空")
	}
	if req.FinalFileName == "" {
		return nil, fmt.Errorf("最终文件名（FinalFileName）不能为空")
	}
	if req.ChunkName == "" {
		return nil, fmt.Errorf("分片名称（ChunkName）不能为空")
	}

	// 2. 模式判断：inMemory=true → 内存流式上传；false → 本地文件上传（原逻辑）
	var fileReader io.Reader
	var err error

	if inMemory {
		// 内存模式：直接使用传入的reader
		if dataReader == nil {
			return nil, fmt.Errorf("内存模式下dataReader不能为空")
		}
		fileReader = dataReader
		logger.Info("使用内存流式上传分片", slog.String("pre", pre), slog.String("ChunkName", req.ChunkName))
	} else {
		// 本地文件模式：完全保留原逻辑
		// 2.1 拼接分片文件完整路径（处理 Linux 路径分隔符）
		chunkFilePath := filepath.Join(req.LocalBaseDir, req.ChunkName)
		chunkFilePath = filepath.Clean(chunkFilePath)

		// 2.2 校验 LocalBaseDir 目录（不存在则自动创建）
		if err := os.MkdirAll(req.LocalBaseDir, 0755); err != nil {
			return nil, fmt.Errorf("创建分片存储目录 %s 失败: %w", req.LocalBaseDir, err)
		}

		// 2.3 校验分片文件是否存在且可读
		fileInfo, err := os.Stat(chunkFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("分片文件不存在: %s", chunkFilePath)
			}
			return nil, fmt.Errorf("获取分片文件信息失败: %w", err)
		}
		if fileInfo.IsDir() {
			return nil, fmt.Errorf("%s 是目录，不是分片文件", chunkFilePath)
		}
		if fileInfo.Size() == 0 {
			return nil, fmt.Errorf("分片文件 %s 为空", chunkFilePath)
		}

		// 2.4 打开本地分片文件（只读模式）
		file, err := os.Open(chunkFilePath)
		if err != nil {
			return nil, fmt.Errorf("打开分片文件 %s 失败（请检查文件权限）: %w", chunkFilePath, err)
		}
		defer file.Close() // 本地文件模式下延迟关闭
		fileReader = file

		logger.Info("使用本地文件上传分片", slog.String("pre", pre), slog.String("chunkFilePath", chunkFilePath))
	}

	// 3. 构建 multipart/form-data 请求体（适配两种模式）
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	// 3.1 添加文件字段（字段名必须为 "file"，和服务端匹配）
	fileWriter, err := bodyWriter.CreateFormFile("file", req.ChunkName)
	if err != nil {
		return nil, fmt.Errorf("创建文件表单字段失败: %w", err)
	}

	// 3.2 流式写入文件内容（两种模式共用此逻辑，避免加载大文件到内存）
	if _, err := io.Copy(fileWriter, fileReader); err != nil {
		return nil, fmt.Errorf("写入文件内容失败: %w", err)
	}

	// 3.3 关闭 multipart writer（生成结束边界）
	if err := bodyWriter.Close(); err != nil {
		return nil, fmt.Errorf("关闭请求体 writer 失败: %w", err)
	}

	// 4. 构建 HTTP POST 请求（完全保留原逻辑）
	httpReq, err := http.NewRequest("POST", req.ServerURL, bodyBuf)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}

	// 5. 设置请求头（严格匹配服务端要求，完全保留原逻辑）
	httpReq.Header.Set("Content-Type", bodyWriter.FormDataContentType())
	httpReq.Header.Set(HeaderFileName, req.FinalFileName)
	httpReq.Header.Set(HeaderChunkName, req.ChunkName)

	// 6. 配置 HTTP 客户端（适配 Linux 长连接/超时，完全保留原逻辑）
	client := &http.Client{
		// 可选：设置超时（避免大文件上传卡住）
		// Timeout: 5 * time.Minute,
	}

	// 7. 发送请求（完全保留原逻辑）
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("发送请求到 %s 失败: %w", req.ServerURL, err)
	}

	// 8. 校验响应状态码（完全保留原逻辑，增强错误排查）
	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("无法读取错误响应")
		}
		resp.Body.Close() // 必须关闭响应体
		return resp, fmt.Errorf("接口返回错误: 状态码 %d, 内容: %s", resp.StatusCode, string(respBody))
	}

	return resp, nil
}

// UploadToGCSbyProxy 通过代理向GCS上传文件/分片（修复后）
// 核心功能：支持内存流式上传（inMemory=true）和本地文件上传（inMemory=false），带限流和GCP鉴权
func UploadFileChunkbyProxy(
	task ChunkTask_,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) error {
	logger.Info("UploadFileChunkbyProxy start",
		slog.String("pre", pre),
		slog.Any("task", task),
		slog.String("hops", hops),
		slog.Bool("inMemory", inMemory))

	// ---------------------- 1. 基础参数校验（防空指针） ----------------------
	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	ctx := task.Ctx
	dest := task.Dest
	var proxyReader io.ReadCloser = reader

	// 定义资源关闭defer（统一释放所有Reader）
	defer func() {
		if proxyReader != nil && proxyReader != reader {
			_ = proxyReader.Close() // 关闭本地文件Reader
		}
		// 外部传入的reader由调用方负责关闭，此处不主动关闭（避免重复关闭）
	}()

	// ---------------------- 2. 选择上传源：内存流 / 本地文件 ----------------------
	if !inMemory {
		// 模式1：inMemory=false → 从本地文件读取
		localFilePath := filepath.Join(task.LocalBaseDir, task.ObjectName)
		localFilePath = filepath.Clean(localFilePath) // 标准化路径（防多斜杠）

		logger.Info("prepare to read local file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))

		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		proxyReader = f // 替换为本地文件Reader
		logger.Info("local file opened successfully",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))
	} else {
		// 模式2：inMemory=true → 使用外部传入的内存Reader
		if proxyReader == nil {
			err := fmt.Errorf("inMemory=true but reader is nil")
			logger.Error("invalid reader", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		logger.Info("use in-memory reader for upload", slog.String("pre", pre))
	}

	// ---------------------- 3. 限流包装Reader ----------------------
	rateLimitedBody := limit_rate.NewRateLimitedReader(ctx, proxyReader, rateLimiter)
	logger.Info("rate limiter applied to reader", slog.String("pre", pre))

	// ---------------------- 4. 解析hops并构造URL ----------------------
	hopList := strings.Split(hops, ",")
	if len(hopList) == 0 {
		err := fmt.Errorf("invalid X-Hops: %s (split empty)", hops)
		logger.Error("parse hops failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	firstHop := hopList[0]

	url, _ := util.ReplaceUploadURLHost(dest.FileSys.Upload, firstHop)
	logger.Info("construct upload URL",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("firstHop", firstHop))

	// ---------------------- 6. 构造并发送HTTP请求 ----------------------
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Hops", hops)
	req.Header.Set("X-Chunk-Index", "1")
	req.Header.Set("X-Rate-Limit-Enable", "true")
	req.Header.Set("X-Source-Type", task.Source.SourceType)
	req.Header.Set(HeaderFileName, task.ObjectName)
	req.Header.Set(HeaderChunkName, task.ObjectName)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	logger.Info("send HTTP request to proxy",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("timeout", "5m"))

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("HTTP request failed",
			slog.String("pre", pre),
			slog.String("url", url),
			slog.Any("err", err))
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close() // 确保响应体关闭

	// ---------------------- 7. 校验响应状态 ----------------------
	if resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("failed to read response body")
			logger.Error("read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("upload to chunk by proxy failed",
			slog.String("pre", pre),
			slog.Int("statusCode", resp.StatusCode),
			slog.String("response", string(respBody)))
		return err
	}

	logger.Info("UploadFileChunkbyProxy success",
		slog.String("pre", pre),
		slog.String("url", url))

	return nil
}
