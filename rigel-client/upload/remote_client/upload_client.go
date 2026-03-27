package remote_client

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/util"
	"time"
)

type Upload struct {
	serverURL    string // 接口地址（如 http://127.0.0.1:8080/api/v1/chunk/upload）
	localBaseDir string // 分片文件的目录
}

// NewUpload 仿照统一风格初始化新版Upload结构体
func NewUpload(
	serverURL, localBaseDir string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Upload {
	u := &Upload{
		serverURL:    serverURL,
		localBaseDir: localBaseDir,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
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
func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool, // 新增：内存模式开关
	pre string,
	logger *slog.Logger,
) error {

	logger.Info("UploadFileChunk", slog.String("pre", pre), slog.Bool("inMemory", inMemory))

	finalFileName := objectName // 最终合并后的文件名（对应 X-File-Name）
	chunkName := objectName     // 当前分片名称（对应 X-Chunk-Name）
	dataReader := reader

	// 仅在「上传开始前」检查ctx是否已取消（避免启动无效上传）
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 2. 模式判断：inMemory=true → 内存流式上传；false → 本地文件上传（原逻辑）
	var fileReader io.Reader
	var err error

	if inMemory {
		// 内存模式：直接使用传入的reader
		if dataReader == nil {
			return fmt.Errorf("内存模式下dataReader不能为空")
		}
		fileReader = dataReader
		logger.Info("使用内存流式上传分片", slog.String("pre", pre), slog.String("ChunkName", chunkName))
	} else {
		// 本地文件模式：完全保留原逻辑
		// 2.1 拼接分片文件完整路径（处理 Linux 路径分隔符）
		chunkFilePath := filepath.Join(u.localBaseDir, chunkName)
		chunkFilePath = filepath.Clean(chunkFilePath)

		// 2.2 校验 LocalBaseDir 目录（不存在则自动创建）
		if err := os.MkdirAll(u.localBaseDir, 0755); err != nil {
			return fmt.Errorf("创建分片存储目录 %s 失败: %w", u.localBaseDir, err)
		}

		// 2.3 校验分片文件是否存在且可读
		fileInfo, err := os.Stat(chunkFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("分片文件不存在: %s", chunkFilePath)
			}
			return fmt.Errorf("获取分片文件信息失败: %w", err)
		}
		if fileInfo.IsDir() {
			return fmt.Errorf("%s 是目录，不是分片文件", chunkFilePath)
		}
		if fileInfo.Size() == 0 {
			return fmt.Errorf("分片文件 %s 为空", chunkFilePath)
		}

		// 2.4 打开本地分片文件（只读模式）
		file, err := os.Open(chunkFilePath)
		if err != nil {
			return fmt.Errorf("打开分片文件 %s 失败（请检查文件权限）: %w", chunkFilePath, err)
		}
		defer file.Close() // 本地文件模式下延迟关闭
		fileReader = file

		logger.Info("使用本地文件上传分片", slog.String("pre", pre), slog.String("chunkFilePath", chunkFilePath))
	}

	// 3. 构建 multipart/form-data 请求体（适配两种模式）
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	// 3.1 添加文件字段（字段名必须为 "file"，和服务端匹配）
	fileWriter, err := bodyWriter.CreateFormFile("file", chunkName)
	if err != nil {
		return fmt.Errorf("创建文件表单字段失败: %w", err)
	}

	// 3.2 流式写入文件内容（两种模式共用此逻辑，避免加载大文件到内存）
	if _, err := io.Copy(fileWriter, fileReader); err != nil {
		return fmt.Errorf("写入文件内容失败: %w", err)
	}

	// 3.3 关闭 multipart writer（生成结束边界）
	if err := bodyWriter.Close(); err != nil {
		return fmt.Errorf("关闭请求体 writer 失败: %w", err)
	}

	// 4. 构建 HTTP POST 请求（完全保留原逻辑）
	httpReq, err := http.NewRequest("POST", u.serverURL, bodyBuf)
	if err != nil {
		return fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}

	// 5. 设置请求头（严格匹配服务端要求，完全保留原逻辑）
	httpReq.Header.Set("Content-Type", bodyWriter.FormDataContentType())
	httpReq.Header.Set(util.HeaderFileName, finalFileName)
	httpReq.Header.Set(util.HeaderChunkName, chunkName)

	// 6. 配置 HTTP 客户端（适配 Linux 长连接/超时，完全保留原逻辑）
	client := &http.Client{Timeout: 1 * time.Minute}

	// 7. 发送请求（完全保留原逻辑）
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("发送请求到 %s 失败: %w", u.serverURL, err)
	}

	// 8. 校验响应状态码（完全保留原逻辑，增强错误排查）
	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("无法读取错误响应")
		}
		resp.Body.Close() // 必须关闭响应体
		return fmt.Errorf("接口返回错误: 状态码 %d, 内容: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
