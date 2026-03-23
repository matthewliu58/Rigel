package base

import "log/slog"

type SourceDisk struct {
	User      string `json:"ssh_user" form:"ssh_user"`             // SSH用户名
	Host      string `json:"ssh_host" form:"ssh_host"`             // SSH主机IP
	Port      string `json:"ssh_port" form:"ssh_port"`             // SSH端口
	Password  string `json:"ssh_password" form:"ssh_password"`     // SSH密码
	RemoteDir string `json:"ssh_remote_dir" form:"ssh_remote_dir"` // 远端文件目录
}

func ExtractSourceDiskFromInterface(obj interface{}, pre string, logger *slog.Logger) *SourceDisk {
	sdObj, ok := obj.(*SourceDisk)
	if !ok {
		// 断言失败（类型不匹配），返回 nil 或抛错（按需选择）
		logger.Error("interface 不是 SourceDisk 类型", slog.Any("pre", pre))
		return nil
	}
	return sdObj
}
