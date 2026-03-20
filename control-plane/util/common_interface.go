package util

type ProbeTask struct {
	TargetType string // "node" | "cloud_storage"
	Provider   string // node 可为空，cloud storage 用 google/aws/azure
	IP         string
	Port       int
	Region     string // cloud storage 用，node 可为空
	ID         string // cloud storage 用，node 可为空
}

type EndPoint struct {
	IP       string `json:"ip"`
	Provider string `json:"provider"`
	Region   string `json:"region"`
	ID       string `json:"id"`
}

type EndPoints struct {
	Source EndPoint `json:"source"`
	Dest   EndPoint `json:"dest"`
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}
