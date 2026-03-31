package base

import (
	"encoding/json"
	"log/slog"
)

type SourceDisk struct {
	User      string `json:"ssh_user" form:"ssh_user"`             // SSH用户名
	Host      string `json:"ssh_host" form:"ssh_host"`             // SSH主机IP
	Port      string `json:"ssh_port" form:"ssh_port"`             // SSH端口
	Password  string `json:"ssh_password" form:"ssh_password"`     // SSH密码
	RemoteDir string `json:"ssh_remote_dir" form:"ssh_remote_dir"` // 远端文件目录
}

func ExtractSourceDiskFromInterface(obj interface{}, pre string, logger *slog.Logger) *SourceDisk {
	if obj == nil {
		logger.Error("SourceDisk interface is nil", slog.String("pre", pre))
		return nil
	}

	// 1. 把 interface{} 序列化成 JSON
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal SourceDisk interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. 反序列化到结构体
	var sd SourceDisk
	if err := json.Unmarshal(data, &sd); err != nil {
		logger.Error("unmarshal to SourceDisk failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	return &sd
}
