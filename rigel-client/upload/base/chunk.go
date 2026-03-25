package base

import (
	"encoding/json"
	"log/slog"
)

type Chunk struct {
	Upload string `json:"upload" form:"upload"` // 上传接口地址
	Merge  string `json:"merge" form:"merge"`   // 合并接口地址
}

func ExtractChunkFromInterface(obj interface{}, pre string, logger *slog.Logger) *Chunk {
	if obj == nil {
		logger.Error("Chunk interface is nil", slog.String("pre", pre))
		return nil
	}

	// 先转 JSON
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal chunk interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 再反序列化为 Chunk
	var ck Chunk
	if err := json.Unmarshal(data, &ck); err != nil {
		logger.Error("unmarshal to Chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	return &ck
}
