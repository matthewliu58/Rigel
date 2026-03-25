package base

import (
	"encoding/json"
	"log/slog"
)

type GCP struct {
	CredFile   string `json:"cred_file" form:"cred_file"`     // GCP凭证文件
	BucketName string `json:"bucket_name" form:"bucket_name"` // GCP存储桶
}

func ExtractGCPFromInterface(obj interface{}, pre string, logger *slog.Logger) *GCP {
	// 1. 先把 obj 转成 JSON 字节
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal interface failed", slog.Any("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. 再反序列化到 GCP
	var gcp GCP
	if err := json.Unmarshal(data, &gcp); err != nil {
		logger.Error("unmarshal to GCP failed", slog.Any("pre", pre), slog.Any("err", err))
		return nil
	}

	return &gcp
}
