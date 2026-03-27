package remote

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/limit_rate"
	"rigel-client/util"
	"strings"
	"time"
)

type Upload struct {
	localBaseDir string
	uploadURL    string
}

func NewUpload(
	localBaseDir, uploadURL string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		uploadURL:    uploadURL,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

// UploadToGCSbyProxy 通过代理向GCS上传文件/分片（修复后）
// 核心功能：支持内存流式上传（inMemory=true）和本地文件上传（inMemory=false），带限流和GCP鉴权
func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) error {

	logger.Info("UploadFile start", slog.String("pre", pre), slog.Bool("inMemory", inMemory))

	// ---------------------- 1. 基础参数校验（防空指针） ----------------------
	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("Invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadFileChunkbyProxy canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

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
		localFilePath := filepath.Join(u.localBaseDir, objectName)
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

	url, _ := util.ReplaceUploadURLHost(u.uploadURL, firstHop)
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
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderDestType, util.RemoteDisk)
	req.Header.Set(util.HeaderFileName, objectName)
	req.Header.Set(util.HeaderChunkName, objectName)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{Timeout: 1 * time.Minute}
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
