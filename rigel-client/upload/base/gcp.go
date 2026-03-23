package base

import "log/slog"

type GCP struct {
	CredFile   string `json:"cred_file" form:"cred_file"`     // GCP凭证文件
	BucketName string `json:"bucket_name" form:"bucket_name"` // GCP存储桶
}

func ExtractGCPFromInterface(obj interface{}, pre string, logger *slog.Logger) *GCP {
	// 类型断言：把 interface{} 转为 *GCP
	gcpObj, ok := obj.(*GCP)
	if !ok {
		// 断言失败（类型不匹配），返回 nil 或抛错（按需选择）
		logger.Error("interface 不是 GCP 类型", slog.Any("pre", pre))
		return nil
	}
	return gcpObj
}
