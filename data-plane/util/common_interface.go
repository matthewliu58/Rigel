package util

type ProbeTask struct {
	TargetType string // "node" | "cloud_storage"
	Provider   string // node 可为空，cloud storage 用 google/aws/azure
	IP         string
	Port       int
	Region     string // cloud storage 用，node 可为空
	ID         string // cloud storage 用，node 可为空
}
