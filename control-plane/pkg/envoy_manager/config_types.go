package envoy_manager

// PortRateLimitConfig 端口维度限流配置
type PortRateLimitConfig struct {
	//Enabled bool `json:"enabled"` // 是否开启限流
	//QPS int64 `json:"qps"` // 每秒请求数限制
	//Burst int64 `json:"burst"` // 突发请求数
	//Whitelist []string `json:"whitelist"` // 白名单IP
	Bandwidth int64 `json:"bandwidth"` // 带宽限制 单位 Bytes/s, 1 Mbps = 125000 Bytes/s
}

// APICommonResp Common API response structure
type APICommonResp struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// EnvoyTargetAddr Multi target IP + Port
type EnvoyTargetAddr struct {
	IP   string `json:"ip" binding:"required,ip"`
	Port int    `json:"port" binding:"required,min=1,max=65535"`
}

// EnvoyPortConfig Envoy port config (only listen port + enable status)
type EnvoyPortConfig struct {
	Port      int                 `json:"port" binding:"required,min=1,max=65535"`
	Enabled   bool                `json:"enabled"`
	RateLimit PortRateLimitConfig `json:"rate_limit"` // 限流配置
}

// EnvoyGlobalConfig Envoy global config (global target addrs + listen ports)
type EnvoyGlobalConfig struct {
	AdminPort   int               `json:"admin_port" binding:"required,min=1,max=65535"`
	PathBase    string            `json:"path_base"`
	TargetAddrs []EnvoyTargetAddr `json:"target_addrs" binding:"required,dive"` // Global target addrs
	Ports       []EnvoyPortConfig `json:"ports"`
}

// EnvoyPortCreateReq Create Envoy port request
type EnvoyPortCreateReq struct {
	Port int `json:"port" binding:"required,min=1,max=65535"`
}

// EnvoyPortDisableReq Disable Envoy port request
type EnvoyPortDisableReq struct {
	Port int `json:"port" binding:"required,min=1,max=65535"`
}
