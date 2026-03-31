package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/limit_rate"
	"rigel-client/util"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	awsService     = "s3"
	awsAlgorithm   = "AWS4-HMAC-SHA256"
	awsContentType = "application/octet-stream"
)

// =====================
// 核心结构体（对齐 GCP Upload）
// =====================
type Upload struct {
	localBaseDir string // 本地基础目录（文件模式用）
	bucketName   string // S3 存储桶名称
	region       string // AWS 区域
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // 留空 = AWS 官方
	usePathStyle bool
}

// NewUpload 初始化 AWS S3 Upload 实例（对齐 GCP NewUpload）
func NewUpload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string, // 日志前缀
	logger *slog.Logger,
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	// 和 GCP 完全一致的日志打印逻辑
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
	logger.Info("UploadToS3byProxy start", slog.String("pre", pre))

	// 校验 hops 非空
	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// 监听 ctx 取消信号
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToS3byProxy canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	var proxyReader io.ReadCloser = reader

	// 统一释放资源（对齐 GCP 逻辑）
	defer func() {
		if proxyReader != nil && proxyReader != reader {
			_ = proxyReader.Close() // 关闭本地文件Reader
		}
		// 外部传入的 reader 由调用方关闭
	}()

	//选择上传源：内存流 / 本地文件
	if !inMemory {
		// 模式1：inMemory=false → 从本地文件读取
		localFilePath := filepath.Join(u.localBaseDir, objectName)
		localFilePath = filepath.Clean(localFilePath) // 标准化路径

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
		proxyReader = f
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

	//限流包装Reader
	rateLimitedBody := limit_rate.NewRateLimitedReader(ctx, proxyReader, rateLimiter)
	logger.Info("rate limiter applied to reader", slog.String("pre", pre))

	// 解析hops并构造URL
	hopList := strings.Split(hops, ",")
	if len(hopList) == 0 {
		err := fmt.Errorf("invalid X-Hops: %s (split empty)", hops)
		logger.Error("parse hops failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	firstHop := hopList[0]

	// 构造 S3 Proxy 上传 URL（对齐 GCP 格式）
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

	// 生成 AWS 签名（替代 GCP Token）
	logger.Info("start to generate AWS signature", slog.String("pre", pre))
	// 获取当前时间（AWS 签名需要）
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	// 生成签名密钥
	signingKey := getSignatureKey(u.secretKey, dateStamp, u.region, awsService)

	// 构造待签名字符串（简化版，适配 Proxy 场景）
	canonicalRequest := fmt.Sprintf("POST\n/%s/%s\n\nhost:%s\nx-amz-date:%s\n\nhost;x-amz-date\nUNSIGNED-PAYLOAD",
		u.bucketName, objectName, firstHop, amzDate)
	stringToSign := fmt.Sprintf("%s\n%s\n%s/%s/%s/aws4_request\n%s",
		awsAlgorithm, amzDate, dateStamp, u.region, awsService,
		sha256Hex(canonicalRequest))

	// 计算签名
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	// 构造 Authorization 头
	authHeader := fmt.Sprintf("%s Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=host;x-amz-date, Signature=%s",
		awsAlgorithm, u.accessKey, dateStamp, u.region, awsService, signature)
	logger.Info("AWS signature generated successfully", slog.String("pre", pre))

	// 构造并发送HTTP请求
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("Create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头（对齐 GCP 逻辑，替换为 AWS 相关头）
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Content-Type", awsContentType)
	req.Header.Set("X-Amz-Date", amzDate) // AWS 必需头
	// 保留和 GCP 一致的业务头
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderDestType, util.AWSCloud) // 替换为 AWS 类型
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求（对齐 GCP 超时配置）
	client := &http.Client{Timeout: 5 * time.Minute}
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

	// 校验响应状态
	if resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("failed to read response body")
			logger.Error("Read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("Upload to S3 by proxy failed",
			slog.String("pre", pre),
			slog.Int("statusCode", resp.StatusCode),
			slog.String("response", string(respBody)))
		return err
	}

	logger.Info("UploadToS3byProxy success",
		slog.String("pre", pre),
		slog.String("objectName", objectName),
		slog.String("url", url))

	return nil
}

// getSignatureKey 生成 AWS 签名密钥（AWS4 规范）
func getSignatureKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// hmacSHA256 计算 HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// sha256Hex 计算 SHA256 并返回十六进制字符串
func sha256Hex(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}
