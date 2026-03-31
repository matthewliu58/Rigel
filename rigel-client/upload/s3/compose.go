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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// 核心结构体
type Compose struct {
	bucket       string // S3 存储桶名称
	region       string // AWS 区域
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // 留空 = AWS 官方
	usePathStyle bool
}

// NewCompose 初始化 AWS S3 Compose 实例（对齐 GCP NewCompose）
func NewCompose(
	bucket, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *Compose {
	c := &Compose{
		bucket:       bucket,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

func (c *Compose) ComposeFile(
	ctx context.Context,
	objectName string, // 最终合成的文件名
	parts []string, // 分片文件列表
	pre string,
	logger *slog.Logger,
) error {
	// 上下文超时检查
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled: %w", ctx.Err())
		logger.Error("AWS Compose canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 初始化 S3 客户端
	s3Client, err := c.initS3Client(ctx)
	if err != nil {
		logger.Error("create S3 client failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("new s3 client failed: %w", err)
	}

	// 1. 单文件场景：复制+删除源文件（对齐 GCP 逻辑）
	if len(parts) == 1 {
		partName := parts[0]
		// 同名无需操作
		if partName == objectName {
			logger.Info("single file name matches final name, skip compose",
				slog.String("pre", pre),
				slog.String("object", objectName))
			return nil
		}

		// 复制文件
		logger.Info("start copy single file to final location",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		copyInput := &s3.CopyObjectInput{
			Bucket:     aws.String(c.bucket),
			CopySource: aws.String(fmt.Sprintf("/%s/%s", c.bucket, partName)),
			Key:        aws.String(objectName),
		}
		_, err := s3Client.CopyObject(ctx, copyInput)
		if err != nil {
			logger.Error("copy single file failed",
				slog.String("pre", pre),
				slog.String("from", partName),
				slog.String("to", objectName),
				slog.Any("err", err))
			return fmt.Errorf("copy single file failed: %w", err)
		}
		logger.Info("copy single file success",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// 删除源文件（容错：失败仅告警）
		delInput := &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(partName),
		}
		if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
			var apiErr smithy.APIError
			if errors.As(delErr, &apiErr) {
				logger.Warn("delete single source file failed (copy success)",
					slog.String("pre", pre),
					slog.String("partName", partName),
					slog.String("code", apiErr.ErrorCode()),
					slog.Any("err", delErr))
			} else {
				logger.Warn("delete single source file failed (copy success)",
					slog.String("pre", pre),
					slog.String("partName", partName),
					slog.Any("err", delErr))
			}
		} else {
			logger.Info("delete single source file success",
				slog.String("pre", pre),
				slog.String("partName", partName))
		}

		logger.Info("single file process completed",
			slog.String("pre", pre),
			slog.String("finalObject", objectName))
		return nil
	}

	current := parts
	level := 0
	var tempObjects []string // 记录临时合成文件

	// 树形合成：每次合并最多1000个分片（S3 单请求最大限制）
	for len(current) > 1 {
		var next []string

		for i := 0; i < len(current); i += 1000 {
			end := i + 1000
			if end > len(current) {
				end = len(current)
			}
			group := current[i:end]
			tmpObjectName := fmt.Sprintf("%s.compose.%d.%d", objectName, level, i)

			// 合并当前分组的分片到临时文件
			if err := c.mergePartsToTempFile(ctx, s3Client, group, tmpObjectName, pre, logger); err != nil {
				logger.Error("merge temp object failed",
					slog.String("pre", pre),
					slog.String("tmpObjectName", tmpObjectName),
					slog.Int("level", level),
					slog.Any("group", group),
					slog.Any("err", err))
				return fmt.Errorf("merge temp object %s failed: %w", tmpObjectName, err)
			}

			next = append(next, tmpObjectName)
			tempObjects = append(tempObjects, tmpObjectName)
			logger.Info("merge temp object success",
				slog.String("pre", pre),
				slog.String("name", tmpObjectName),
				slog.Int("level", level),
				slog.Any("from", group))
		}

		current = next
		level++
	}

	// 3. 最终合成：临时文件→最终文件
	logger.Info("start finalize object",
		slog.String("pre", pre),
		slog.String("from", current[0]),
		slog.String("to", objectName))

	// 复制临时文件到最终位置
	finalCopyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		CopySource: aws.String(fmt.Sprintf("/%s/%s", c.bucket, current[0])),
		Key:        aws.String(objectName),
	}
	_, err = s3Client.CopyObject(ctx, finalCopyInput)
	if err != nil {
		logger.Error("finalize object copy failed",
			slog.String("pre", pre),
			slog.String("from", current[0]),
			slog.String("to", objectName),
			slog.Any("err", err))
		return fmt.Errorf("copy temp to final failed: %w", err)
	}

	// 4. 清理临时文件和分片
	// 4.1 删除中间临时文件
	for _, tmp := range tempObjects {
		if tmp != current[0] {
			delInput := &s3.DeleteObjectInput{
				Bucket: aws.String(c.bucket),
				Key:    aws.String(tmp),
			}
			if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
				var apiErr smithy.APIError
				if errors.As(delErr, &apiErr) {
					logger.Warn("delete temp object failed",
						slog.String("pre", pre),
						slog.String("tmp", tmp),
						slog.String("code", apiErr.ErrorCode()),
						slog.Any("err", delErr))
				} else {
					logger.Warn("delete temp object failed",
						slog.String("pre", pre),
						slog.String("tmp", tmp),
						slog.Any("err", delErr))
				}
			}
		}
	}

	// 4.2 删除最终临时文件
	delFinalTempInput := &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(current[0]),
	}
	if _, delErr := s3Client.DeleteObject(ctx, delFinalTempInput); delErr != nil {
		logger.Warn("delete final temp object failed",
			slog.String("pre", pre),
			slog.String("tmp", current[0]),
			slog.Any("err", delErr))
	}

	// 4.3 删除原始分片文件
	for _, p := range parts {
		delInput := &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(p),
		}
		if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
			var apiErr smithy.APIError
			if errors.As(delErr, &apiErr) {
				logger.Warn("delete part object failed",
					slog.String("pre", pre),
					slog.String("part", p),
					slog.String("code", apiErr.ErrorCode()),
					slog.Any("err", delErr))
			} else {
				logger.Warn("delete part object failed",
					slog.String("pre", pre),
					slog.String("part", p),
					slog.Any("err", delErr))
			}
		}
	}

	logger.Info("multi file compose success",
		slog.String("pre", pre),
		slog.String("finalObject", objectName))
	return nil
}

// initS3Client 初始化 S3 客户端（带超时配置）
func (u *Compose) initS3Client(ctx context.Context) (*s3.Client, error) {
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

// mergePartsToTempFile 合并分片到临时文件（S3 兼容合成逻辑）
func (c *Compose) mergePartsToTempFile(
	ctx context.Context,
	client *s3.Client,
	parts []string,
	tempName string,
	pre string,
	logger *slog.Logger,
) error {
	// 创建临时本地文件
	tempFile, err := os.CreateTemp("", "s3-compose-*")
	if err != nil {
		return fmt.Errorf("create temp local file failed: %w", err)
	}
	tempFilePath := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFilePath) // 清理本地临时文件
	}()

	// 下载并合并所有分片
	for _, part := range parts {
		select {
		case <-ctx.Done():
			return fmt.Errorf("merge canceled: %w", ctx.Err())
		default:
		}

		// 下载分片
		getInput := &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(part),
		}
		resp, err := client.GetObject(ctx, getInput)
		if err != nil {
			return fmt.Errorf("download part %s failed: %w", part, err)
		}

		// 写入临时文件
		_, err = io.Copy(tempFile, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("write part %s to temp file failed: %w", part, err)
		}

		logger.Debug("merge part to temp file",
			slog.String("pre", pre),
			slog.String("part", part),
			slog.String("temp", tempName))
	}

	// 重置文件指针到开头
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek temp file failed: %w", err)
	}

	// 上传合并后的临时文件到 S3
	putInput := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(tempName),
		Body:   tempFile,
	}
	_, err = client.PutObject(ctx, putInput)
	if err != nil {
		return fmt.Errorf("upload temp file %s to s3 failed: %w", tempName, err)
	}

	return nil
}
