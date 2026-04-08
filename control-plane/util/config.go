package util

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var Config_ *Config

// Config 代表整个配置文件的结构
type Config struct {
	//Name       string   `yaml:"name"`         // 当前节点名字
	EnvoyPathBase string        `yaml:"envoy_path_base"` // Envoy 可执行文件路径（基础路径）
	EnvoyPath     string        `yaml:"envoy_path"`      // Envoy 可执行文件路径
	EnvoyLog      string        `yaml:"envoy_log"`
	EnvoyConfig   string        `yaml:"envoy_config"`
	ServerList    []string      `yaml:"server_list"` // Etcd 集群所有节点的地址
	ServerIP      string        `yaml:"server_ip"`   // 当前节点 IP
	DataDir       string        `yaml:"data_dir"`    // Etcd 数据目录
	Scaling       ScalingConfig `yaml:"scaling"`
	Node          NodeConfig    `yaml:"node"`
	Proxy         ProxyConfig   `yaml:"proxy"`
}

type ScalingConfig struct {
	Username        string `yaml:"username"`
	LocalPathProxy  string `yaml:"local_path_proxy"`
	RemotePathProxy string `yaml:"remote_path_proxy"`
	BinaryProxy     string `yaml:"binary_proxy"`
	LocalPathPlane  string `yaml:"local_path_plane"`
	RemotePathPlane string `yaml:"remote_path_plane"`
	BinaryPlane     string `yaml:"binary_plane"`
	Config          string `yaml:"config"`
	PrivateKey      string `yaml:"private_key"`
	SshKey          string `yaml:"ssh_key"`
}

type ProxyConfig struct {
	IPs []string `yaml:"ips"`
}

// NodeConfig 对应 node 配置
type NodeConfig struct {
	Provider  string `yaml:"provider"`  // gcp | aws | azure | vultr | digitalocean | onprem
	Continent string `yaml:"continent"` // 仅用于 bandwidth / cost 查询
	Country   string `yaml:"country"`   // 可选，用于日志/可解释性
	City      string `yaml:"city"`      // 可选，用于日志/可解释性
	IP        NodeIP `yaml:"ip"`        // 内外网 IP
}

// NodeIP 内外网 IP
type NodeIP struct {
	Private string `yaml:"private"` // 内网 IP
	Public  string `yaml:"public"`  // 公网 IP（可选）
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
	} else {
		logger.Info("读取配置文件成功", "path", configPath)
	}
	// 4. 解析yaml到结构体
	var config Config
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("解析yaml失败: %w", err)
	}

	return &config, nil
}

func GenerateRandomLetters(length int) string {
	rand.Seed(time.Now().UnixNano())                                  // 使用当前时间戳作为随机数种子
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // 字母范围（大小写）
	var result string
	for i := 0; i < length; i++ {
		result += string(letters[rand.Intn(len(letters))]) // 随机选择一个字母
	}
	return result
}

func GenerateRandomLetters_(length int) string {
	rand.Seed(time.Now().UnixNano())                  // 使用当前时间戳作为随机数种子
	letters := "abcdefghijklmnopqrstuvwxyz0123456789" // 字母范围（大小写）
	var result string
	for i := 0; i < length; i++ {
		result += string(letters[rand.Intn(len(letters))]) // 随机选择一个字母
	}
	return result
}
