package util

const (
	GCPCLoud               = "gcp-cloud"
	RemoteDisk             = "remote-disk"
	LocalDisk              = "local-disk"
	HeaderFileName         = "X-File-Name"  // 最终合并后的文件名
	HeaderChunkName        = "X-Chunk-Name" // 单个分片的自定义名称
	DataSourceType         = "X-Data-Source-Type"
	DataDestType           = "X-Data-Dest-Type"
	HeaderXHops            = "X-Hops"
	HeaderXChunkIndex      = "X-Chunk-Index"
	HeaderXRateLimitEnable = "X-Rate-Limit-Enable"
	HeaderXSourceType      = "X-Source-Type"
	HeaderDestTyep         = "X-Dest-Type"
)

type ApiResponse struct {
	Code int         `json:"code"` // 200=成功，400=参数错误，500=服务端错误
	Msg  string      `json:"msg"`  // 提示信息
	Data interface{} `json:"data"` // 业务数据
}

type ChunkMergeRequest struct {
	FinalFileName string   `json:"final_file_name" binding:"required"` // 最终合并后的文件名（必填）
	ChunkNames    []string `json:"chunk_names" binding:"required"`     // 分片名列表（按顺序，必填）
	DeleteChunks  bool     `json:"delete_chunks,omitempty"`            // 是否删除分片（可选，默认true）
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}

// SSHConfig 定义SSH连接配置
type SSHConfig struct {
	User     string // 用户名
	HostPort string // 主机IP:端口（如192.168.1.20:22）
	Password string // 密码（或用密钥认证）
}

type FileSys struct {
	Upload string // 上传接口地址 // ``
	Merge  string // 合并接口地址
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

// TransferConfig 适配Form表单传输的文件传输配置结构体
type UploadConfig struct {
	User struct {
		Username string `json:"username" form:"username"` // 客户端用户名
		Priority int    `json:"priority" form:"priority"` // 优先级
	} `json:"user" form:"user"`

	// ------------- 核心Form参数 -------------
	File struct {
		FileName    string `json:"file_name" form:"file_name"`         // 源文件名（如test.zip）
		FileStart   int64  `json:"file_start" form:"file_start"`       // 文件起始偏移（字节，默认0）
		FileLength  int64  `json:"file_length" form:"file_length"`     // 文件传输长度（字节，0=整个文件）
		NewFileName string `json:"new_file_name" form:"new_file_name"` // 目标文件名
	} `json:"file" form:"file"`

	Proxy struct {
		LocalDir string `json:"local_dir" form:"local_dir"`
	} `json:"proxy" form:"proxy"`

	EndPoints EndPoints `json:"end_points"`

	// ------------- 源文件存储配置 -------------
	Source struct {
		DataSourceType string `json:"data_source_type" form:"data_source_type"` // 源类型

		// SSH配置（仅DataSourceType=remote_ssh时生效）
		SourceDisk struct {
			User      string `json:"ssh_user" form:"ssh_user"`             // SSH用户名
			Host      string `json:"ssh_host" form:"ssh_host"`             // SSH主机IP
			SSHPort   string `json:"ssh_port" form:"ssh_port"`             // SSH端口
			Password  string `json:"ssh_password" form:"ssh_password"`     // SSH密码
			RemoteDir string `json:"ssh_remote_dir" form:"ssh_remote_dir"` // 远端文件目录
		} `json:"source_disk" form:"source_disk"`

		// GCP源配置（仅DataSourceType=gcp时生效）
		SourceGCP struct {
			CredFile   string `json:"gcp_source_cred_file" form:"gcp_source_cred_file"` // GCP凭证文件
			BucketName string `json:"gcp_source_bucket" form:"gcp_source_bucket"`       // GCP存储桶
		} `json:"source_gcp" form:"source_gcp"`
	} `json:"source" form:"source"`

	// ------------- 目标文件存储配置 -------------
	Dest struct {
		DataDestType string `json:"data_dest_type" form:"data_dest_type"` // 目标类型

		// GCP目标配置（仅DataDestType=gcp时生效）
		DestGCP struct {
			CredFile   string `json:"gcp_dest_cred_file" form:"gcp_dest_cred_file"` // GCP凭证文件
			BucketName string `json:"gcp_dest_bucket" form:"gcp_dest_bucket"`       // GCP存储桶
		} `json:"dest_gcp" form:"dest_gcp"`

		// 文件系统接口配置（仅DataDestType=api时生效）
		DestDisk struct {
			Upload string `json:"file_sys_upload" form:"file_sys_upload"` // 上传接口地址
			Merge  string `json:"file_sys_merge" form:"file_sys_merge"`   // 合并接口地址
		} `json:"dest_disk" form:"dest_disk"`
	} `json:"dest" form:"dest"`

	// ------------- 传输通用配置 -------------
	//Transfer struct {
	//	ChunkSize   int64 `json:"chunk_size" form:"chunk_size"`     // 分块大小（字节，默认512MB）
	//	Timeout     int64 `json:"timeout" form:"timeout"`           // 整体超时（秒，默认300）
	//	RateLimit   int   `json:"rate_limit" form:"rate_limit"`     // 速率限制（Mbps，0=不限）
	//	Concurrency int   `json:"concurrency" form:"concurrency"`   // 并发数（默认10）
	//} `json:"transfer" form:"transfer"`
}
