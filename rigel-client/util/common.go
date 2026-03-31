package util

const (
	GCSCLoud   = "gcs-cloud"
	S3Cloud    = "s3-cloud"
	RemoteDisk = "remote-disk"
	LocalDisk  = "local-disk"
)

const (
	HeaderFileName         = "X-File-Name"  // 最终合并后的文件名
	HeaderFileSize         = "X-File-Size"  // 分片大小
	HeaderChunkName        = "X-Chunk-Name" // 单个分片的自定义名称
	HeaderXHops            = "X-Hops"
	HeaderXChunkIndex      = "X-Chunk-Index"
	HeaderXRateLimitEnable = "X-Rate-Limit-Enable"
	HeaderDestType         = "X-Dest-Type"
)

type ChunkMergeRequest struct {
	FinalFileName string   `json:"final_file_name" binding:"required"` // 最终合并后的文件名（必填）
	ChunkNames    []string `json:"chunk_names" binding:"required"`     // 分片名列表（按顺序，必填）
	DeleteChunks  bool     `json:"delete_chunks,omitempty"`            // 是否删除分片（可选，默认true）
}
