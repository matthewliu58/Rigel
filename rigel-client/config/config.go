package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log/slog"
	"os"
	"path/filepath"
)

var Config_ *Config

// Config 一级结构体，对应yaml平级配置
type Config struct {
	Port        string `yaml:"port"`         // 服务端口
	ControlHost string `yaml:"control_host"` // 控制接口地址

	// SSH相关配置（补充，用于远端文件读取）
	SSH struct {
		User      string `yaml:"user"`       // SSH用户名（如root）
		Host      string `yaml:"host"`       // SSH主机IP（如192.168.1.20）
		SSHPort   string `yaml:"ssh_port"`   // SSH端口（默认22）
		Password  string `yaml:"password"`   // SSH密码
		RemoteDir string `yaml:"remote_dir"` // 远端文件目录（如/mnt/remote-data）
	} `yaml:"ssh"`

	CredFileSource   string `yaml:"cred_file"`      // GCP / 服务凭证文件
	BucketNameSource string `yaml:"local_base_dir"` // 本地上传目录

	CredFile   string `yaml:"cred_file"` // GCP / 服务凭证文件
	BucketName string `yaml:"bucket_name"`
	
	LocalBaseDir string `yaml:"local_base_dir"` // 本地上传目录
}

// ReadYamlConfig 读取同层级的config.yaml配置
func ReadYamlConfig(logger *slog.Logger) (*Config, error) {
	// 1. 获取当前可执行文件所在目录（确保和config.yaml同层级）
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("获取程序路径失败: %w", err)
	}
	exeDir := filepath.Dir(exePath)                    // 程序所在目录
	configPath := filepath.Join(exeDir, "config.yaml") // 拼接同层级的config.yaml路径

	// 2. 校验配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("配置文件不存在: %s（请确保config.yaml和程序同目录）", configPath)
	}

	// 3. 读取配置文件内容
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 4. 解析yaml到结构体
	var config Config
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("解析yaml失败: %w", err)
	}

	return &config, nil
}
