package envoy_manager

import (
	"bufio"
	"context"
	"control-plane/util"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//const EnvoyPath = "/home/matth/envoy"

var (
	EnvoyPath string
	EnvoyLog  string
)

// EnvoyOperator Envoy操作器（适配matth目录）
type EnvoyOperator struct {
	AdminAddr  string // 管理地址（固定为http://127.0.0.1:9901）
	ConfigPath string // 配置文件路径（固定为/home/matth/envoy.yaml）
	GlobalCfg  *EnvoyGlobalConfig
	//flag       bool         //flag == flase 系统不能服务 8083没有ip
	mu sync.RWMutex // 读写锁：读多写少场景更高效
}

// slogWriter 自定义Writer，解析Envoy日志真实级别后写入slog
type slogWriter struct {
	logger *slog.Logger
	stream string // 标记是stdout还是stderr（仅备用）
}

// Write 实现io.Writer接口：解析真实级别，保留原始换行符
func (w *slogWriter) Write(p []byte) (n int, err error) {
	// 1. 保留原始内容（包含换行符）
	content := string(p)
	if strings.TrimSpace(content) == "" { // 过滤空行
		return len(p), nil
	}

	// 2. 核心：解析Envoy日志的真实级别
	level, msg := parseEnvoyLogLevel(content)

	// 3. 按真实级别写入slog，保留原始content
	switch level {
	case slog.LevelDebug:
		w.logger.DebugContext(context.Background(), "envoy_log", "stream", w.stream, "content", msg)
	case slog.LevelInfo:
		w.logger.InfoContext(context.Background(), "envoy_log", "stream", w.stream, "content", msg)
	case slog.LevelWarn:
		w.logger.WarnContext(context.Background(), "envoy_log", "stream", w.stream, "content", msg)
	case slog.LevelError:
		w.logger.ErrorContext(context.Background(), "envoy_log", "stream", w.stream, "content", msg)
	default:
		// 无法识别的级别，按info输出
		w.logger.InfoContext(context.Background(), "envoy_log", "stream", w.stream, "content", msg)
	}

	return len(p), nil
}

// parseEnvoyLogLevel 解析Envoy日志的真实级别
// Envoy日志格式：[2025-12-26 16:33:41.715][267487][info][main] xxxx
func parseEnvoyLogLevel(content string) (slog.Level, string) {
	switch {
	case strings.Contains(content, "[debug]"):
		return slog.LevelDebug, content
	case strings.Contains(content, "[info]"):
		return slog.LevelInfo, content
	case strings.Contains(content, "[warn]"):
		return slog.LevelWarn, content
	case strings.Contains(content, "[error]"):
		return slog.LevelError, content
	case strings.Contains(content, "[critical]"):
		return slog.LevelError, content // critical归为error级别
	default:
		return slog.LevelInfo, content // 未知级别默认info
	}
}

// teeWriter 实现双输出：控制台 + slog（带缓冲但及时刷新）
type teeWriter struct {
	console io.Writer     // 控制台输出（os.Stdout/os.Stderr）
	slog    *bufio.Writer // slog缓冲Writer
}

// Write 实现io.Writer接口，核心：透传所有字节（含\n）+ 刷新缓冲
func (t *teeWriter) Write(p []byte) (n int, err error) {
	// 第一步：写入控制台（保留原始换行符）
	n, err = t.console.Write(p)
	if err != nil {
		return n, err
	}

	// 第二步：写入slog缓冲（包含\n）
	_, err = t.slog.Write(p)
	if err != nil {
		return n, err
	}

	// 关键点：如果包含换行符，立即刷新缓冲（避免\n被吞）
	if len(p) > 0 && p[len(p)-1] == '\n' {
		err = t.slog.Flush()
	}

	return n, err
}

// 辅助函数：创建带双输出的Writer
func NewEnvoyLogWriter(logger *slog.Logger, stream string, console io.Writer) io.Writer {
	// 创建slogWriter
	slogW := &slogWriter{
		logger: logger,
		stream: stream,
	}
	// 包装为缓冲Writer（避免频繁写入）
	bufSlogW := bufio.NewWriter(slogW)
	// 双输出：控制台 + slog
	return &teeWriter{
		console: console,
		slog:    bufSlogW,
	}
}

// NewEnvoyOperator 创建Envoy操作器实例
func NewEnvoyOperator(adminAddr, configPath string) *EnvoyOperator {
	// 标准化配置文件路径（确保是绝对路径）
	absPath, _ := filepath.Abs(configPath)

	return &EnvoyOperator{
		AdminAddr:  adminAddr,
		ConfigPath: absPath,
		//flag:       false,
		mu: sync.RWMutex{}, // 初始化锁
	}
}

// InitEnvoyGlobalConfig 初始化Envoy全局配置
func (o *EnvoyOperator) InitEnvoyGlobalConfig(uu *util.Config, adminPort int,
	pre string, logger *slog.Logger) error {

	o.mu.Lock()
	defer o.mu.Unlock()

	//8090 默认端口
	ports := make([]EnvoyPortConfig, 0)
	for i := 8090; i <= 8090; i++ {
		ports = append(ports, EnvoyPortConfig{Port: i, Enabled: true, RateLimit: PortRateLimitConfig{Bandwidth: 0}})
	}

	//数据面转发端口8083
	targetAddresses := make([]EnvoyTargetAddr, 0)
	for _, ip := range uu.Proxy.IPs {
		targetAddresses = append(targetAddresses, EnvoyTargetAddr{IP: ip, Port: 8095}) //34.174.125.203
	}

	o.GlobalCfg = &EnvoyGlobalConfig{
		AdminPort:   adminPort,
		PathBase:    uu.EnvoyPathBase,
		Ports:       ports,
		TargetAddrs: targetAddresses,
	}

	b, _ := json.Marshal(o.GlobalCfg)
	logger.Info("初始化Envoy全局配置", slog.String("pre", pre), slog.String("config", string(b)))

	return nil
}

// CreateOrUpdateEnvoyPort 新增/更新Envoy端口配置
func (o *EnvoyOperator) CreateOrUpdateEnvoyPort(req EnvoyPortCreateReq,
	pre string, logger, logger1 *slog.Logger) (EnvoyPortConfig, error) {

	o.mu.Lock()
	defer o.mu.Unlock()

	// 1. 检查端口是否已存在
	portIdx := -1
	for i, p := range o.GlobalCfg.Ports {
		if p.Port == req.Port {
			portIdx = i
			break
		}
	}

	// 2. 构造端口配置
	newPortCfg := EnvoyPortConfig{
		Port:    req.Port,
		Enabled: true,
	}

	// 3. 更新/新增端口配置
	if portIdx >= 0 {
		o.GlobalCfg.Ports[portIdx] = newPortCfg
	} else {
		o.GlobalCfg.Ports = append(o.GlobalCfg.Ports, newPortCfg)
	}

	logger.Info("CreateOrUpdateEnvoyPort", slog.String("pre", pre),
		slog.Int("port", req.Port))

	// 4. 渲染配置文件到matth目录
	if err := RenderEnvoyYamlConfig(o.GlobalCfg, o.ConfigPath); err != nil {
		return EnvoyPortConfig{}, fmt.Errorf("渲染配置失败: %w", err)
	}

	// 5. 先检查是否有运行的Envoy，没有则首次启动，有则热重启
	if !o.IsEnvoyRunning() {
		if err := o.StartFirstEnvoy(pre, logger, logger1); err != nil {
			return EnvoyPortConfig{}, fmt.Errorf("首次启动Envoy失败: %w", err)
		}
	} else {
		if err := o.HotReloadEnvoyConfig(pre, logger, logger1); err != nil {
			return EnvoyPortConfig{}, fmt.Errorf("热加载配置失败: %w", err)
		}
	}

	return newPortCfg, nil
}

// DisableEnvoyPort 禁用Envoy端口
func (o *EnvoyOperator) DisableEnvoyPort(port int, pre string, logger, logger1 *slog.Logger) error {

	o.mu.Lock()
	defer o.mu.Unlock()

	// 1. 查找端口并禁用
	portIdx := -1
	for i, p := range o.GlobalCfg.Ports {
		if p.Port == port {
			portIdx = i
			break
		}
	}
	if portIdx < 0 {
		return errors.New("端口未配置")
	}

	o.GlobalCfg.Ports[portIdx].Enabled = false
	logger.Info("禁用端口: %d", port)

	// 2. 重新渲染配置到matth目录
	if err := RenderEnvoyYamlConfig(o.GlobalCfg, o.ConfigPath); err != nil {
		return fmt.Errorf("渲染禁用端口配置失败: %w", err)
	}

	// 3. 热加载配置
	return o.HotReloadEnvoyConfig(pre, logger, logger1)
}

// UpdateGlobalTargetAddrs 更新后端地址（写锁）
func (o *EnvoyOperator) UpdateGlobalTargetAddrs(targetAddrs []EnvoyTargetAddr,
	action, pre string, logger, logger1 *slog.Logger) error {
	// 写锁：修改TargetAddrs，独占锁
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.GlobalCfg == nil {
		return errors.New("global config not initialized")
	}
	if len(targetAddrs) == 0 {
		return errors.New("target addrs cannot be empty")
	}

	logger.Info("UpdateGlobalTargetAddrs", slog.String("pre", pre), slog.Any("targetAddrs", targetAddrs))

	b, _ := json.Marshal(o.GlobalCfg)
	logger.Info("UpdateGlobalTargetAddrs, old config",
		slog.String("pre", pre), slog.String("config", string(b)))

	// 更新后端地址
	//o.GlobalCfg.TargetAddrs = append(o.GlobalCfg.TargetAddrs, targetAddrs...)

	targetAddrMap := make(map[string]EnvoyTargetAddr)
	for _, v := range o.GlobalCfg.TargetAddrs {
		targetAddrMap[v.IP] = v
	}
	if action == "add" {
		for _, v := range targetAddrs {
			if _, exist := targetAddrMap[v.IP]; !exist {
				o.GlobalCfg.TargetAddrs = append(o.GlobalCfg.TargetAddrs, v)
			}
		}
	} else if action == "del" {
		o.GlobalCfg.TargetAddrs = []EnvoyTargetAddr{}

		delAddrMap := make(map[string]EnvoyTargetAddr)
		for _, v_ := range targetAddrs {
			delAddrMap[v_.IP] = v_
		}

		for _, v := range targetAddrMap {
			if _, exist := delAddrMap[v.IP]; !exist {
				o.GlobalCfg.TargetAddrs = append(o.GlobalCfg.TargetAddrs, v)
			}
		}
	}

	b, _ = json.Marshal(o.GlobalCfg)
	logger.Info("UpdateGlobalTargetAddrs, new config",
		slog.String("pre", pre), slog.String("config", string(b)))

	// 渲染配置
	if err := RenderEnvoyYamlConfig(o.GlobalCfg, o.ConfigPath); err != nil {
		return fmt.Errorf("render target addrs failed: %w", err)
	}

	logger.Info("RenderEnvoyYamlConfig success", slog.String("pre", pre))

	err := o.HotReloadEnvoyConfig(pre, logger, logger1)
	if err != nil {
		return fmt.Errorf("hot reload envoy config failed: %w", err)
	}

	//o.flag = true
	logger.Info("UpdateGlobalTargetAddrs, flag changed to true", slog.String("pre", pre))

	return nil
}

// GetCurrentConfig 获取当前配置（读锁，不修改数据）
func (o *EnvoyOperator) GetCurrentConfig() (*EnvoyGlobalConfig, error) {
	// 读锁：仅读取配置，共享锁（多个goroutine可同时读）
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.GlobalCfg == nil {
		return nil, errors.New("global config not initialized")
	}

	// 返回拷贝：避免外部修改原指针（可选，增强安全性）
	cfgCopy := *o.GlobalCfg
	return &cfgCopy, nil
}

// StartFirstEnvoy 首次启动Envoy（epoch=0）
func (o *EnvoyOperator) StartFirstEnvoy(pre string, logger, logger1 *slog.Logger) error {

	// 4. 渲染配置文件到matth目录
	if err := RenderEnvoyYamlConfig(o.GlobalCfg, o.ConfigPath); err != nil {
		return fmt.Errorf("渲染配置失败: %w", err)
	}

	// 检查配置文件是否存在
	if _, err := os.Stat(o.ConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("配置文件不存在: %s", o.ConfigPath)
	}

	// 构造首次启动命令（epoch=0）
	cmd := exec.Command(
		EnvoyPath,
		"-c", o.ConfigPath,
		"--restart-epoch", "0",
		"--base-id", "1000",
		"--log-level", "info",
		"--log-path", EnvoyLog,
	)

	// 日志输出
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	// --------------------------
	// 核心修改：保留控制台输出 + 转发到logger1
	// --------------------------
	// 1. 创建stdout/stderr对应的slogWriter（关联logger1）
	//stdoutSlogWriter := &slogWriter{logger: logger1, stream: "stdout"}
	//stderrSlogWriter := &slogWriter{logger: logger1, stream: "stderr"}

	stdoutSlogWriter := NewEnvoyLogWriter(logger1, "stdout", os.Stdout)
	stderrSlogWriter := NewEnvoyLogWriter(logger1, "stderr", os.Stderr)

	// 2. 带缓冲避免阻塞，包装成teeWriter实现双输出
	cmd.Stdout = &teeWriter{
		console: os.Stdout,
		slog:    bufio.NewWriter(stdoutSlogWriter),
	}
	cmd.Stderr = &teeWriter{
		console: os.Stderr,
		slog:    bufio.NewWriter(stderrSlogWriter),
	}

	// 启动进程
	logger.Info("首次启动Envoy（epoch=0）", slog.String("pre", pre))
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动失败: %w", err)
	}

	// 验证进程是否存活
	time.Sleep(1 * time.Second)
	if !isProcessAlive(cmd.Process.Pid) {
		return errors.New("Envoy启动后立即退出")
	}

	// 初始化epoch文件
	if err := os.WriteFile("/tmp/envoy_epoch", []byte("0"), 0644); err != nil {
		logger.Error("写入epoch文件警告", slog.String("pre", pre), slog.Any("err", err))
	}

	// 后台等待进程（防止僵尸）
	go func() {
		if err := cmd.Wait(); err != nil {
			logger.Error("Envoy进程退出", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	logger.Info(
		"Envoy 首次启动成功", slog.String("pre", pre), slog.Int("pid", cmd.Process.Pid),
	)
	return nil
}

// HotReloadEnvoyConfig 修复后的热重启函数
func (o *EnvoyOperator) HotReloadEnvoyConfig(pre string, logger, logger1 *slog.Logger) error {
	// 前置检查：确保Envoy正在运行
	if !o.IsEnvoyRunning() {
		return errors.New("Envoy未运行，无法热重启")
	}

	// ===== 1. 读取上一次 epoch =====
	epoch := 0
	if data, err := os.ReadFile("/tmp/envoy_epoch"); err == nil {
		s := strings.TrimSpace(string(data))
		if n, err := strconv.Atoi(s); err == nil {
			epoch = n
		}
	}
	newEpoch := epoch + 1

	// ===== 2. 启动新 Envoy =====
	cmd := exec.Command(
		EnvoyPath,
		"-c", o.ConfigPath,
		"--restart-epoch", strconv.Itoa(newEpoch),
		"--base-id", "1000",
		"--log-level", "info",
		"--log-path", EnvoyLog,
	)

	// 日志输出
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	// --------------------------
	// 核心修改：保留控制台输出 + 转发到logger1
	// --------------------------
	// 1. 创建stdout/stderr对应的slogWriter（关联logger1）
	//stdoutSlogWriter := &slogWriter{logger: logger1, stream: "stdout"}
	//stderrSlogWriter := &slogWriter{logger: logger1, stream: "stderr"}

	stdoutSlogWriter := NewEnvoyLogWriter(logger1, "stdout", os.Stdout)
	stderrSlogWriter := NewEnvoyLogWriter(logger1, "stderr", os.Stderr)

	// 2. 带缓冲避免阻塞，包装成teeWriter实现双输出
	cmd.Stdout = &teeWriter{
		console: os.Stdout,
		slog:    bufio.NewWriter(stdoutSlogWriter),
	}
	cmd.Stderr = &teeWriter{
		console: os.Stderr,
		slog:    bufio.NewWriter(stderrSlogWriter),
	}

	// 启动新进程
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动新Envoy失败: %w", err)
	}

	// 验证新进程存活
	time.Sleep(2 * time.Second)
	if !isProcessAlive(cmd.Process.Pid) {
		return fmt.Errorf("新Envoy进程启动后立即退出（PID: %d）", cmd.Process.Pid)
	}

	// 后台等待新进程（防止僵尸）
	go func() {
		if err := cmd.Wait(); err != nil {
			logger.Error("新Envoy进程退出", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	// ===== 3. 更新 epoch 文件 =====
	if err := os.WriteFile(
		"/tmp/envoy_epoch",
		[]byte(strconv.Itoa(newEpoch)),
		0644,
	); err != nil {
		return fmt.Errorf("写入epoch文件失败: %w", err)
	}

	logger.Info("Envoy热重启成功", slog.String("pre", pre),
		slog.Int("旧epoch", epoch), slog.Int("新epoch", newEpoch))
	return nil
}

// IsEnvoyRunning 检查Envoy是否正在运行
func (o *EnvoyOperator) IsEnvoyRunning() bool {
	//cmd := exec.Command("pgrep", "-u", "matth", "envoy")
	cmd := exec.Command("pgrep", "envoy")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

// isProcessAlive 检查进程是否存活
func isProcessAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// 发送空信号检查进程是否存在
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}
