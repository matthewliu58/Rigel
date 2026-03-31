package s3_client

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Upload struct {
	localBaseDir string // 本地基础目录（文件模式用）
	bucketName   string // S3 存储桶名称
	region       string // AWS 区域
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // 留空 = AWS 官方
	usePathStyle bool
}

// NewUpload 初始化 AWS S3 Upload 实例（完全对齐 GCP 风格）
func NewUpload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string, // 日志前缀（和 GCP 保持一致）
	logger *slog.Logger, // 日志实例
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
	hops string, // 兼容 GCP 入参（预留，AWS 客户端模式无需使用）
	rateLimiter *rate.Limiter, // 兼容 GCP 入参（如需限流可启用）
	reader io.ReadCloser,
	inMemory bool, // true=内存模式，false=文件模式
	pre string, // 日志前缀（关键追溯字段）
	logger *slog.Logger,
) error {

	logger.Info("UploadToS3byClient", slog.String("pre", pre))

	// 上传开始前检查 ctx 是否已取消（对齐 GCP 逻辑）
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToS3byClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 日志区分上传模式（添加 pre 前缀，和 GCP 格式一致）
	if inMemory {
		logger.Info("Uploading data to S3 (in-memory mode, no local file)",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to S3 (disk mode)",
			slog.String("pre", pre),
			slog.String("LocalBaseDir", u.localBaseDir),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	s3Client, err := u.initS3Client(ctx)
	if err != nil {
		logger.Error("Failed to create S3 client",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// 定义上传体（根据模式选择）
	var uploadBody io.Reader
	//var localFile *os.File // 文件模式下的本地文件句柄

	// ---------------------- 模式1：inMemory=true → 内存流式上传 ----------------------
	if inMemory {
		if reader == nil {
			err := fmt.Errorf("in-memory mode requires non-nil dataReader")
			logger.Error("In-memory mode invalid", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		defer reader.Close() // 内存模式关闭传入的 Reader

		// 可选：启用限流（如需和 GCP 保持一致的限流逻辑）
		if rateLimiter != nil {
			//uploadBody = NewRateLimitedReader(ctx, reader, rateLimiter)
		} else {
			uploadBody = reader
		}

		// ---------------------- 模式2：inMemory=false → 本地文件上传 ----------------------
	} else {
		localFilePath := filepath.Join(u.localBaseDir, objectName)
		// 打开本地文件（对齐 GCP 的错误日志格式）
		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("Failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		//localFile = f
		defer f.Close() // 确保本地文件关闭

		// 可选：启用限流
		if rateLimiter != nil {
			//uploadBody = NewRateLimitedReader(ctx, f, rateLimiter)
		} else {
			uploadBody = f
		}
	}

	// 构建 S3 上传请求（对齐 GCP 的 StorageClass/ContentType）
	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(u.bucketName),
		Key:           aws.String(objectName),
		Body:          uploadBody,
		ContentType:   aws.String("application/octet-stream"), // 和 GCP 一致
		ContentLength: aws.Int64(contentLength),
		//StorageClass: types.StorageClassStandard,
	}

	// 执行上传（传入外层 ctx，支持中途取消）
	_, err = s3Client.PutObject(ctx, putInput)
	if err != nil {
		logger.Error("Failed to upload to S3 bucket",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return fmt.Errorf("failed to upload to S3 bucket: %w", err)
	}

	// 上传成功日志（区分模式，和 GCP 格式一致）
	if inMemory {
		logger.Info("In-memory upload success",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Local file upload success",
			slog.String("pre", pre),
			slog.String("localFilePath", filepath.Join(u.localBaseDir, objectName)),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	return nil
}

// initS3Client 初始化 S3 客户端（带超时配置）
func (u *Upload) initS3Client(ctx context.Context) (*s3.Client, error) {
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
