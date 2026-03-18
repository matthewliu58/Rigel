package util

type ProbeTask struct {
	TargetType string // "node" | "cloud_storage"
	Provider   string // node 可为空，cloud storage 用 google/aws/azure
	IP         string
	Port       int
	Region     string // cloud storage 用，node 可为空
	ID         string // cloud storage 用，node 可为空
}

type EndPoints struct {
	ClientIP       string `json:"clientIP"` // 目标服务器 IP 或域名
	ClientProvider string `json:"clientProvider"`
	ClientRegion   string `json:"clientRegion"`   // 客户端大区
	ClientID       string `json:"clientID"`       // 客户端城市
	ServerIP       string `json:"serverIP"`       // 目标服务器 IP 或域名
	ServerProvider string `json:"serverProvider"` // 云服务提供商，例如 AWS, GCP, DO ////ServerCont
	ServerRegion   string `json:"serverRegion"`   // 云服务所在区域，例如 us-east-1
	ServerID       string `json:"serverID"`       // 云服务所在城市，例如 Ashburn
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}
