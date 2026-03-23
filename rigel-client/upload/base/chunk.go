package base

import "log/slog"

type Chunk struct {
	Upload string `json:"upload" form:"upload"` // 上传接口地址
	Merge  string `json:"merge" form:"merge"`   // 合并接口地址
}

func ExtractChunkFromInterface(obj interface{}, pre string, logger *slog.Logger) *Chunk {
	ckObj, ok := obj.(*Chunk)
	if !ok {
		// 断言失败（类型不匹配），返回 nil 或抛错（按需选择）
		logger.Error("interface 不是 Chunk 类型", slog.Any("pre", pre))
		return nil
	}
	return ckObj
}
