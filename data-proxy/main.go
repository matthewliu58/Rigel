package main

import (
	"context"
	"crypto/tls"
	"data-proxy/config"
	"data-proxy/util"
	"data-proxy/virtual_queue"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"
)

const (
	HeaderHops       = "x-hops"
	HeaderIndex      = "x-index"
	HeaderSourceTyep = "X-Source-Type"
	RemoteDisk       = "remote-disk"
	//HeaderHost      = "Host"
	DefaultIndex    = "1"
	ServerErrorCode = 503
	BufferSize      = 64
)

// 自定义Handler：修复slog.Context为context.Context，兼容所有Go 1.21+版本
type SourceHandler struct {
	handler slog.Handler
}

// Handle 核心修复：把slog.Context改为context.Context
func (h *SourceHandler) Handle(ctx context.Context, r slog.Record) error {
	// 采集调用日志的位置（跳过当前Handler的栈帧，取真实业务代码的位置）
	fs := runtime.CallersFrames([]uintptr{r.PC})
	frame, _ := fs.Next()

	// 只保留文件名（去掉全路径）
	fileName := filepath.Base(frame.File)

	// 向日志记录中添加源位置字段
	r.AddAttrs(
		slog.String("file", fileName),          // 文件名
		slog.Int("line", frame.Line),           // 行号
		slog.String("func", frame.Func.Name()), // 函数名（可选）
	)

	// 交给底层TextHandler输出
	return h.handler.Handle(ctx, r)
}

// 以下是slog.Handler接口的默认实现（全部修正为context.Context）
func (h *SourceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *SourceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SourceHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *SourceHandler) WithGroup(name string) slog.Handler {
	return &SourceHandler{handler: h.handler.WithGroup(name)}
}

/*
 * =========================
 * 方案 B：数据路径级统计
 * =========================
 */

// 统计 reader：包在 io.Copy 的数据路径上
type countingReader struct {
	r io.Reader
}

func (c *countingReader) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

// 拆分 x-hops 字符串
func splitHops(hopsStr string) []string {
	if hopsStr == "" {
		return []string{}
	}
	parts := strings.Split(hopsStr, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// ==================== client 池（保持你原来的实现） ====================
var (
	clientMap = make(map[string]*http.Client)
	clientMu  = &sync.RWMutex{}
)

func getClient(target string, scheme string) *http.Client {
	clientMu.RLock()
	c, ok := clientMap[target]
	clientMu.RUnlock()
	if ok {
		return c
	}

	transport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     10 * time.Second,
		ReadBufferSize:      BufferSize * 1024,
		WriteBufferSize:     BufferSize * 1024,
	}

	if scheme == "https" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	c = &http.Client{Transport: transport}

	clientMu.Lock()
	clientMap[target] = c
	clientMu.Unlock()
	return c
}

// handler 返回 http.HandlerFunc
func handler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		pre := util.GenerateRandomLetters(5)

		hopsStr := r.Header.Get(HeaderHops)
		indexStr := r.Header.Get(HeaderIndex)
		if indexStr == "" {
			indexStr = DefaultIndex
		}

		hops := splitHops(hopsStr)
		currentIndex := 1
		if idx, err := strconv.Atoi(indexStr); err == nil {
			currentIndex = idx
		}
		hopsLen := len(hops)

		logger.Info("Received request", slog.String("pre", pre),
			"hops", hops, "current_index", currentIndex,
			"method", r.Method, "path", r.URL.Path,
		)

		if hopsLen == 0 {
			http.Error(w, "Missing x-hops header", http.StatusBadRequest)
			logger.Warn("Missing x-hops header", slog.String("pre", pre))
			return
		}

		newIndex := currentIndex + 1
		if newIndex > hopsLen {
			http.Error(w, "Forward index out of range", ServerErrorCode)
			logger.Warn("Forward index out of range", slog.String("pre", pre),
				"new_index", newIndex, "hops_len", hopsLen)
			return
		}

		targetHop := hops[newIndex-1]
		parts := strings.Split(targetHop, ":")
		if len(parts) != 2 {
			http.Error(w, "Invalid target hop format", http.StatusBadRequest)
			logger.Warn("Invalid target hop format", slog.String("pre", pre), "target_hop", targetHop)
			return
		}
		targetIP := parts[0]
		targetPort := parts[1]

		scheme := "http"
		method := r.Method
		//最后一跳的逻辑
		if newIndex == len(hops) {
			sourceType := r.Header.Get(HeaderSourceTyep)
			if sourceType != RemoteDisk {
				scheme = "https"
				method = "PUT"
			}
		}

		targetURL := scheme + "://" + targetIP + ":" + targetPort + r.URL.RequestURI()
		logger.Info("Forwarding to target", slog.String("pre", pre), "target_url", targetURL)

		target := targetIP + ":" + targetPort
		client := getClient(target, scheme)

		req, err := http.NewRequest(method, targetURL, r.Body)
		if err != nil {
			http.Error(w, "Failed to create request", http.StatusInternalServerError)
			logger.Error("Failed to create request", slog.String("pre", pre), "error", err)
			return
		}
		req.Header = r.Header.Clone()
		req.Header.Set(HeaderIndex, strconv.Itoa(newIndex))

		logger.Info("Forwarded request headers", slog.String("pre", pre), slog.Any("header", req.Header))

		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to forward request", ServerErrorCode)
			logger.Error("Failed to forward request", slog.String("pre", pre), slog.Any("err", err))
			return
		}
		defer resp.Body.Close()

		logger.Info("Forwarded response headers", slog.String("pre", pre), slog.Any("header", resp.Header))

		for key, values := range resp.Header {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.WriteHeader(resp.StatusCode)

		// =========================
		// 方案 B 核心：只在真正转发数据时计数
		// =========================
		atomic.AddInt64(&virtual_queue.ActiveTransfers, 1)
		_, err = io.Copy(w, &countingReader{r: resp.Body})
		atomic.AddInt64(&virtual_queue.ActiveTransfers, -1)

		if err != nil {
			logger.Error("Error copying response body", slog.String("pre", pre), slog.Any("err", err))
		}

		logger.Info("Request completed", slog.String("pre", pre),
			"target_hop", targetHop, "status", resp.StatusCode, "protocol", scheme,
			//"active_transfers", atomic.LoadInt64(&virtual_queue.ActiveTransfers),
		)
	}
}

var (
	// 锁和状态变量
	statusLock sync.Mutex
	status     string = "on" // 默认状态为 "on"
)

func main() {
	logDir := "log"
	_ = os.MkdirAll(logDir, 0755)
	logFile, err := os.OpenFile(logDir+"/proxy.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	//logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))

	// 2. 配置基础TextHandler（保留原有Level等配置）
	baseHandler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo, // 日志级别
		AddSource: true,           // 必须开启！否则无法获取文件名/行号
	})

	// 3. 包装成自定义SourceHandler（添加文件名、行号、函数名）
	logger := slog.New(&SourceHandler{handler: baseHandler})

	// 4. 设置为全局logger（可选，整个项目都能生效）
	slog.SetDefault(logger)

	pre := "init"

	config.Config_, err = config.ReadYamlConfig(logger)
	if err != nil {
		logger.Error("read config failed", slog.String("pre", pre), "error", err)
		return
	} else {
		logger.Info("print config info", slog.String("pre", pre),
			slog.Any("config", config.Config_))
	}

	router := gin.Default()

	router.GET("/healthStateChange", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		logger.Info("healthStateChange", slog.String("pre", pre))

		// 获取查询参数 "set"
		set := c.DefaultQuery("set", "on") // 默认值为 "on"，即默认返回 200

		logger.Info("get switch val", slog.String("pre", pre))

		// 锁住状态修改操作，保证并发安全
		statusLock.Lock()
		defer statusLock.Unlock()

		// 根据 set 参数来决定状态值和返回的状态码
		if set == "off" {
			// set 为 "off"，修改状态为 "off"，并返回 500
			status = "off"
		} else {
			// 默认情况或 set 为 "on" 时，修改状态为 "on"，并返回 200
			status = "on"
		}
		c.JSON(http.StatusOK, "success")
	})

	router.GET("/health", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		// 锁住状态修改操作，保证并发安全
		statusLock.Lock()
		defer statusLock.Unlock()

		logger.Info("health", slog.String("pre", pre), slog.String("status", status))

		if status == "off" {
			c.JSON(http.StatusInternalServerError, "error")
			return
		}
		c.JSON(http.StatusOK, "success")
	})

	router.GET("/getCongestionInfo", func(c *gin.Context) {
		c.JSON(http.StatusOK, virtual_queue.CheckCongestion(2*BufferSize, logger))
	})

	//router.Any("/*proxyPath", func(c *gin.Context) {
	//	handler(logger)(c.Writer, c.Request)
	//})
	router.NoRoute(func(c *gin.Context) {
		handler(logger)(c.Writer, c.Request)
	})

	port := "8095"
	port = config.Config_.Port

	logger.Info("Listening", slog.String("pre", pre), "port", port)
	if err := router.Run(":" + port); err != nil {
		logger.Error("Gin Run failed", slog.String("pre", pre), "error", err)
	}
}
