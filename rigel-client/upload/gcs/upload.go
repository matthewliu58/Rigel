package gcs

import (
	"context"
	"fmt"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	cfg "rigel-client/config"
	"rigel-client/limit_rate"
	"rigel-client/util"
	"strings"
	"time"
)

const (
	gcpScopes = "https://www.googleapis.com/auth/devstorage.full_control"
)

type Upload struct {
	localBaseDir string
	bucketName   string
	credFile     string
}

func NewUpload(
	localBaseDir, bucketName, credFile string,
	pre string, // 日志前缀
	logger *slog.Logger,
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		credFile:     credFile,
	}
	// 和NewDownload一致的日志打印逻辑
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

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

	logger.Info("UploadToGCSbyProxy start", slog.String("pre", pre))

	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyProxy canceled", slog.String("pre", pre), slog.Any("err", err))
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

	//如果first为本机ip 直接改成127.0.0.1不走public
	firstHop := hopList[0]
	if firstHop == cfg.PublicIp {
		firstHop = "127.0.0.1"
	}

	url := fmt.Sprintf(
		"http://%s/%s/%s",
		firstHop,
		u.bucketName,
		objectName,
	)
	logger.Info("construct upload URL",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("firstHop", firstHop))

	// ---------------------- 5. 生成GCP Access Token ----------------------
	logger.Info("start to generate GCP access token", slog.String("pre", pre))
	jsonBytes, err := os.ReadFile(u.credFile)
	if err != nil {
		logger.Error("read cred file failed",
			slog.String("pre", pre),
			slog.String("credFile", u.credFile),
			slog.Any("err", err))
		return fmt.Errorf("read cred file: %w", err)
	}

	reds, err := google.CredentialsFromJSON(ctx, jsonBytes, gcpScopes)
	if err != nil {
		logger.Error("Parse GCP credentials failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("parse credentials: %w", err)
	}

	token, err := reds.TokenSource.Token()
	if err != nil {
		logger.Error("Get GCP token failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("get token: %w", err)
	}
	logger.Info("GCP access token generated successfully", slog.String("pre", pre))

	// ---------------------- 6. 构造并发送HTTP请求 ----------------------
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("Create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderDestType, util.GCPCLoud)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{Timeout: 1 * time.Minute}
	logger.Info("Send HTTP request to proxy",
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
			logger.Error("Read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("Upload to GCS by proxy failed",
			slog.String("pre", pre),
			slog.Int("statusCode", resp.StatusCode),
			slog.String("response", string(respBody)))
		return err
	}

	logger.Info("UploadToGCSbyProxy success",
		slog.String("pre", pre),
		slog.String("objectName", objectName),
		slog.String("url", url))

	return nil
}
