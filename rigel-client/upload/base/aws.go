package base

import (
	"encoding/json"
	"log/slog"
)

type AWSConfig struct {
	BucketName   string `json:"bucketName"`   // S3 桶名
	Region       string `json:"region"`       // AWS 区域
	AccessKey    string `json:"accessKey"`    // Access Key
	SecretKey    string `json:"secretKey"`    // Secret Key
	Endpoint     string `json:"endpoint"`     // 留空 = AWS 官方
	UsePathStyle bool   `json:"usePathStyle"` // S3 兼容存储必须开
}

// ExtractAWSFromInterface 从接口中提取 AWS 配置（仿照 ExtractGCPFromInterface 实现）
func ExtractAWSFromInterface(iface interface{}, pre string, logger *slog.Logger) *AWSConfig {
	if iface == nil {
		logger.Error("AWS interface is nil", slog.String("pre", pre))
		return nil
	}

	// 1. 先序列化成 JSON
	data, err := json.Marshal(iface)
	if err != nil {
		logger.Error("marshal aws interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. 反序列化成 AWSConfig
	var awsCfg AWSConfig
	if err := json.Unmarshal(data, &awsCfg); err != nil {
		logger.Error("unmarshal to AWSConfig failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 3. 校验必填字段
	if awsCfg.BucketName == "" || awsCfg.Region == "" || awsCfg.AccessKey == "" || awsCfg.SecretKey == "" {
		logger.Error("AWS config missing required fields", slog.String("pre", pre),
			slog.String("bucket", awsCfg.BucketName),
			slog.String("region", awsCfg.Region))
		return nil
	}

	return &awsCfg
}
