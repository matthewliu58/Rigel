package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type Download struct {
	localBaseDir string // 本地基础目录（落盘模式用）
	bucketName   string // S3 存储桶名称
	region       string // AWS 区域
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // 留空 = AWS 官方
	usePathStyle bool
}

// NewDownload 初始化 AWS S3 Download 实例
func NewDownload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *Download {
	d := &Download{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	logger.Info("NewDownload", slog.String("pre", pre), slog.Any("Download", *d))
	return d
}

// DownloadFile AWS S3 文件下载
func (d *Download) DownloadFile(
	ctx context.Context,
	filename string,    // S3 对象名
	newFilename string, // 本地文件名（落盘模式用）
	start int64,        // 分片起始字节
	length int64,       // 分片长度（<=0 表示完整下载）
	bs string,          // 兼容 GCP 入参（预留）
	inMemory bool,      // true=内存模式，false=落盘模式
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) { // 返回 io.ReadCloser（兼容两种模式）
	objectName := filename

	// 上下文超时检查
	select {
	case <-ctx.Done():
		err := fmt.Errorf("download canceled before start: %w", ctx.Err())
		logger.Error("DownloadFromS3 canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// 日志区分模式+完整/分片读取
	if inMemory {
		if length <= 0 {
			logger.Info("Reading full file from S3 (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName))
		} else {
			logger.Info("Reading file range from S3 (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	} else {
		if length <= 0 {
			logger.Info("Downloading full file from S3 (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("LocalBaseDir", d.localBaseDir))
		} else {
			logger.Info("Downloading file range from S3 (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("newFileName", newFilename),
				slog.String("LocalBaseDir", d.localBaseDir),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	}

	// 初始化 S3 客户端
	s3Client, err := d.initS3Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("create s3 client failed: %w", err)
	}

	// 构建下载请求（完整/分片）
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(objectName),
	}
	// 分片下载：设置 Range 请求头
	if length > 0 {
		endByte := start + length - 1
		getInput.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, endByte))
	}

	// 发送下载请求
	resp, err := s3Client.GetObject(ctx, getInput)
	if err != nil {
		// 处理 S3 错误（如文件不存在、权限不足）
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			logger.Error("S3 GetObject API error",
				slog.String("pre", pre),
				slog.String("code", apiErr.ErrorCode()),
				slog.String("message", apiErr.ErrorMessage()))
		}
		return nil, fmt.Errorf("s3 get object failed: %w", err)
	}

	// 模式1：inMemory=true → 返回流式 Reader（不落盘）
	if inMemory {
		return &s3ReaderWrapper{
			ReadCloser: resp.Body, // 修复：结构体字段是 ReadCloser，不是 Reader
			client:     s3Client,
		}, nil
	}

	// 模式2：inMemory=false → 落盘到本地文件
	defer resp.Body.Close()

	// 拼接本地文件路径
	localFilePath := filepath.Join(d.localBaseDir, newFilename)
	// 创建本地目录
	if err := os.MkdirAll(d.localBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("create local dir failed: %w", err)
	}

	// 创建本地文件
	f, err := os.Create(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("create local file failed: %w", err)
	}
	defer f.Close()

	// 写入本地文件
	if _, err := io.Copy(f, resp.Body); err != nil {
		return nil, fmt.Errorf("copy to local file failed: %w", err)
	}

	// 落盘模式返回本地文件的 Reader
	localFileReader, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("open local file failed: %w", err)
	}

	// 日志反馈结果
	if length <= 0 {
		logger.Info("Full file download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath))
	} else {
		logger.Info("File range download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", start),
			slog.Int64("length_byte", length))
	}

	return localFileReader, nil
}

// initS3Client 初始化 S3 客户端（带超时配置）
func (u *Download) initS3Client(ctx context.Context) (*s3.Client, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	// 基础配置
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(u.region),
		config.WithHTTPClient(httpClient),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     u.accessKey,
				SecretAccessKey: u.secretKey,
				Source:          "custom-config",
			}, nil
		})),
	}

	//如果有 Endpoint，就覆盖（适配所有S3兼容）
	if u.endpoint != "" {
		endpointResolver := aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           u.endpoint,
					SigningRegion: u.region,
				}, nil
			},
		)
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(endpointResolver))
	}

	// 加载配置
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}

	// 创建客户端，自动控制 PathStyle
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = u.usePathStyle
	}), nil
}

// s3ReaderWrapper 封装 S3 响应体 ReadCloser + 资源清理逻辑（内存模式用）
type s3ReaderWrapper struct {
	io.ReadCloser            // 正确：resp.Body 是 io.ReadCloser 类型
	client        *s3.Client // 保留客户端引用（如需扩展清理逻辑）
}

// Close 关闭所有关联资源（调用方必须调用）
func (w *s3ReaderWrapper) Close() error {
	var errStr []string
	if err := w.ReadCloser.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("reader close failed: %v", err))
	}
	// S3 客户端无需手动关闭（SDK 自动管理）
	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
