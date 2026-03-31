package s3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type GetSize struct {
	bucketName   string // S3 存储桶名称
	region       string // AWS 区域
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // 留空 = AWS 官方
	usePathStyle bool
}

// NewGetSize 初始化 AWS S3 GetSize 实例（对齐 GCP NewGetSize）
func NewGetSize(
	bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *GetSize {
	gs := &GetSize{
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	// 和 GCP 完全一致的日志打印逻辑
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

// GetFileSize 获取 AWS S3 对象大小（对齐 GCP GetFileSize 接口）
// 返回值：文件大小（字节）、错误
func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {
	objectName := filename

	// 1. 监听 ctx 取消信号，提前终止操作
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get s3 file size canceled: %w", ctx.Err())
		logger.Error("GetS3ObjectSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 2. 初始化 S3 客户端（带超时控制）
	s3Client, err := g.initS3Client(ctx)
	if err != nil {
		logger.Error("创建 S3 客户端失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return 0, fmt.Errorf("create s3 client failed: %w", err)
	}

	// 3. 构建 HeadObject 请求（仅获取元数据，不下载文件内容）
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(g.bucketName),
		Key:    aws.String(objectName),
	}

	// 4. 发送 HeadObject 请求获取元数据（核心：读取 ContentLength）
	headResp, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		logger.Error("获取 S3 Object 元数据失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))

		// 区分常见错误类型（对齐 GCP 的 ErrObjectNotExist）
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			// S3 对象不存在错误码
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return 0, fmt.Errorf("object %s/%s 不存在: %w", g.bucketName, objectName, err)
			}
			// 权限不足错误
			if apiErr.ErrorCode() == "AccessDenied" {
				return 0, fmt.Errorf("访问对象 %s/%s 权限不足: %w", g.bucketName, objectName, err)
			}
		}
		return 0, fmt.Errorf("s3.HeadObject failed: %w", err)
	}

	// 5. 提取文件大小（ContentLength 对应字节数）
	fileSize := headResp.ContentLength

	// 6. 日志记录结果（和 GCP 完全一致的日志字段）
	logger.Info("成功获取 S3 Object 大小", slog.String("pre", pre),
		slog.String("bucketName", g.bucketName),
		slog.String("objectName", objectName),
		slog.Int64("file_size_bytes", *fileSize),
		slog.String("file_size_human", formatBytes(*fileSize))) // 复用 GCP 的格式化函数

	// 7. 返回文件大小（字节）
	return *fileSize, nil
}

// initS3Client 初始化 S3 客户端（带超时配置）
func (u *GetSize) initS3Client(ctx context.Context) (*s3.Client, error) {
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

// formatBytes 将字节数转换为易读的字符串（完全复用 GCP 的实现）
// 如：1024 → 1KB，1048576 → 1MB
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(bytes)/float64(div),
		"KMGTPE"[exp])
}
