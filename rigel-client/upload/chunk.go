package upload

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
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

// UploadFileChunk 向 ChunkUploadHandler 接口上传单个分片文件
// 自动从 /home/matth/upload/ 目录读取分片文件
func UploadFileChunk(req ChunkUploadRequest, pre string, logger *slog.Logger) (*http.Response, error) {

	logger.Info("UploadFileChunk", "req", req, slog.String("pre", pre))

	// 1. 基础入参校验
	if req.ServerURL == "" {
		return nil, fmt.Errorf("server URL 不能为空")
	}
	if req.FinalFileName == "" {
		return nil, fmt.Errorf("最终文件名（FinalFileName）不能为空")
	}
	if req.ChunkName == "" {
		return nil, fmt.Errorf("分片名称（ChunkName）不能为空")
	}

	// 2. 拼接分片文件完整路径（处理 Linux 路径分隔符）
	// 确保路径拼接后无重复 "/"（如 /home/matth/upload/subdir/chunk1.bin）
	chunkFilePath := filepath.Join(req.LocalBaseDir, req.ChunkName)
	// 标准化路径（解决多斜杠、相对路径问题）
	chunkFilePath = filepath.Clean(chunkFilePath)

	// 3. 校验 LocalBaseDir 目录（不存在则自动创建，带 Linux 权限）
	if err := os.MkdirAll(req.LocalBaseDir, 0755); err != nil { // 0755 是 Linux 常用目录权限
		return nil, fmt.Errorf("创建分片存储目录 %s 失败: %w", req.LocalBaseDir, err)
	}

	// 4. 校验分片文件是否存在且可读
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

	// 5. 打开本地分片文件（只读模式）
	file, err := os.Open(chunkFilePath)
	if err != nil {
		return nil, fmt.Errorf("打开分片文件 %s 失败（请检查文件权限）: %w", chunkFilePath, err)
	}
	defer file.Close()

	// 6. 构建 multipart/form-data 请求体（适配 Linux 大文件）
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	// 6.1 添加文件字段（字段名必须为 "file"，和服务端匹配）
	fileWriter, err := bodyWriter.CreateFormFile("file", filepath.Base(chunkFilePath))
	if err != nil {
		return nil, fmt.Errorf("创建文件表单字段失败: %w", err)
	}

	// 6.2 流式写入文件内容（避免加载大文件到内存，适配 Linux 大分片）
	if _, err := io.Copy(fileWriter, file); err != nil {
		return nil, fmt.Errorf("写入文件内容失败: %w", err)
	}

	// 6.3 关闭 multipart writer（生成结束边界）
	if err := bodyWriter.Close(); err != nil {
		return nil, fmt.Errorf("关闭请求体 writer 失败: %w", err)
	}

	// 7. 构建 HTTP POST 请求
	httpReq, err := http.NewRequest("POST", req.ServerURL, bodyBuf)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}

	// 8. 设置请求头（严格匹配服务端要求）
	httpReq.Header.Set("Content-Type", bodyWriter.FormDataContentType()) // 必须设置
	httpReq.Header.Set(HeaderFileName, req.FinalFileName)                // 最终文件名
	httpReq.Header.Set(HeaderChunkName, req.ChunkName)                   // 当前分片名

	// 9. 配置 HTTP 客户端（适配 Linux 长连接/超时）
	client := &http.Client{
		// 可选：设置超时（避免大文件上传卡住）
		// Timeout: 5 * time.Minute,
	}

	// 10. 发送请求
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("发送请求到 %s 失败: %w", req.ServerURL, err)
	}

	// 11. 校验响应状态码
	if resp.StatusCode != http.StatusOK {
		// 读取错误响应内容（便于排查服务端问题）
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("无法读取错误响应")
		}
		resp.Body.Close() // 必须关闭响应体
		return resp, fmt.Errorf("接口返回错误: 状态码 %d, 内容: %s", resp.StatusCode, string(respBody))
	}

	return resp, nil
}
