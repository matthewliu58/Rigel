package base

import (
	"context"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"rigel-client/upload/gcs"
	"rigel-client/upload/gcs_client"
	"rigel-client/upload/local"
	"rigel-client/upload/remote"
	"rigel-client/upload/remote_client"
	"rigel-client/upload/s3"
	"rigel-client/upload/s3_client"
	"rigel-client/util"
)

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

type User struct {
	Username string `json:"username" form:"username"` // 客户端用户名
	Priority int    `json:"priority" form:"priority"` // 优先级
}

type File struct {
	FileName    string `json:"file_name" form:"file_name"`         // 源文件名（如test.zip）
	FileStart   int64  `json:"file_start" form:"file_start"`       // 文件起始偏移（字节，默认0）
	FileLength  int64  `json:"file_length" form:"file_length"`     // 文件传输长度（字节，0=整个文件）
	NewFileName string `json:"new_file_name" form:"new_file_name"` // 目标文件名
}

type Proxy struct {
	LocalDir string `json:"local_dir" form:"local_dir"`
}

type End struct {
	Type      string      `json:"type" form:"type"` // 源类型
	Interface interface{} `json:"interface" form:"interface"`
}

type UploadStruct struct {
	User      User      `json:"user" form:"user"`
	File      File      `json:"file" form:"file"`
	Proxy     Proxy     `json:"proxy" form:"proxy"`
	Source    End       `json:"source" form:"source"`
	Dest      End       `json:"dest" form:"dest"`
	EndPoints EndPoints `json:"end_points" form:"end_points"`
}

type GetFileSizeInterface interface {
	GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error)
}

type ComposeFileInterface interface {
	ComposeFile(ctx context.Context, objectName string, parts []string, pre string, logger *slog.Logger) error
}

type DownloadFileInterface interface {
	DownloadFile(ctx context.Context, filename string, newFilename string, start int64,
		length int64, bs string, inMemory bool, pre string, logger *slog.Logger) (io.ReadCloser, error)
}

type UploadFileInterface interface {
	UploadFile(ctx context.Context, objectName string, contentLength int64, hops string, rateLimiter *rate.Limiter,
		reader io.ReadCloser, inMemory bool, pre string, logger *slog.Logger) error
}

type FileOperateInterfaces struct {
	GetFileSize  GetFileSizeInterface  // 获取文件大小接口
	ComposeFile  ComposeFileInterface  // 文件合并接口
	DownloadFile DownloadFileInterface // 文件下载接口
	UploadFile   UploadFileInterface   // 文件上传接口
}

// InitInterface 初始化文件操作接口
func InitInterface(clientB bool, us UploadStruct, pre string, logger *slog.Logger) FileOperateInterfaces {
	var fo FileOperateInterfaces

	// 第一步：初始化 读取/下载 相关接口（按Source.Type分支）
	switch us.Source.Type {
	case util.GCSCLoud:
		gcp_ := ExtractGCPFromInterface(us.Source.Interface, pre, logger)
		if gcp_ == nil {
			return fo
		}
		fo.GetFileSize = gcs.NewGetSize(gcp_.BucketName, gcp_.CredFile, pre, logger)
		fo.DownloadFile = gcs.NewDownload(us.Proxy.LocalDir, gcp_.BucketName, gcp_.CredFile, pre, logger)

	case util.S3Cloud:
		aws_ := ExtractAWSFromInterface(us.Source.Interface, pre, logger)
		if aws_ == nil {
			logger.Error("Extract AWS Source Interface failed", slog.String("pre", pre))
			return fo
		}
		fo.GetFileSize = s3.NewGetSize(aws_.BucketName, aws_.Region,
			aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)
		fo.DownloadFile = s3.NewDownload(us.Proxy.LocalDir, aws_.BucketName, aws_.Region,
			aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)

	case util.LocalDisk:
		fo.GetFileSize = local.NewGetSize(us.Proxy.LocalDir, pre, logger)
		fo.DownloadFile = local.NewDownload(us.Proxy.LocalDir, pre, logger)

	case util.RemoteDisk:
		sd := ExtractSourceDiskFromInterface(us.Source.Interface, pre, logger)
		if sd == nil {
			return fo
		}
		fo.GetFileSize = remote.NewGetSize(sd.User, sd.Host, sd.Password, sd.RemoteDir, pre, logger)
		fo.DownloadFile = remote.NewDownload(sd.User, sd.Host, sd.Password, sd.RemoteDir, us.Proxy.LocalDir, pre, logger)

	// 可选：添加default分支，增强容错性
	default:
		logger.Warn("Unsupported Source.Type", slog.String("pre", pre), slog.String("type", string(us.Source.Type)))
		return fo
	}

	// 第二步：初始化 上传/合并 相关接口（按Dest.Type分支）
	switch us.Dest.Type {
	case util.GCSCLoud:
		gcp_ := ExtractGCPFromInterface(us.Dest.Interface, pre, logger)
		if gcp_ == nil {
			return fo
		}
		if clientB {
			fo.UploadFile = gcs_client.NewUpload(us.Proxy.LocalDir, gcp_.BucketName, gcp_.CredFile, pre, logger)
			fo.ComposeFile = gcs.NewCompose(gcp_.BucketName, gcp_.CredFile, pre, logger)
		} else {
			fo.UploadFile = gcs.NewUpload(us.Proxy.LocalDir, gcp_.BucketName, gcp_.CredFile, pre, logger)
			fo.ComposeFile = gcs.NewCompose(gcp_.BucketName, gcp_.CredFile, pre, logger)
		}

	case util.S3Cloud:
		aws_ := ExtractAWSFromInterface(us.Dest.Interface, pre, logger)
		if aws_ == nil {
			logger.Error("Extract AWS Dest Interface failed", slog.String("pre", pre))
			return fo
		}
		if clientB {
			fo.UploadFile = s3_client.NewUpload(us.Proxy.LocalDir, aws_.BucketName, aws_.Region,
				aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)
			fo.ComposeFile = s3.NewCompose(aws_.BucketName, aws_.Region,
				aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)
		} else {
			fo.UploadFile = s3.NewUpload(us.Proxy.LocalDir, aws_.BucketName, aws_.Region,
				aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)
			fo.ComposeFile = s3.NewCompose(aws_.BucketName, aws_.Region,
				aws_.AccessKey, aws_.SecretKey, aws_.Endpoint, aws_.UsePathStyle, pre, logger)
		}

	case util.RemoteDisk:
		ck_ := ExtractChunkFromInterface(us.Dest.Interface, pre, logger)
		if ck_ == nil {
			return fo
		}
		if clientB {
			fo.UploadFile = remote_client.NewUpload(ck_.Upload, us.Proxy.LocalDir, pre, logger)
			fo.ComposeFile = remote.NewCompose(ck_.Merge, true, pre, logger)
		} else {
			fo.UploadFile = remote.NewUpload(us.Proxy.LocalDir, ck_.Upload, pre, logger)
			fo.ComposeFile = remote.NewCompose(ck_.Merge, true, pre, logger)
		}

	// 可选：添加default分支，增强容错性
	default:
		logger.Warn("Unsupported Dest.Type", slog.String("pre", pre), slog.String("type", string(us.Dest.Type)))
		// 此处不return，保留已初始化的GetFileSize/DownloadFile
	}

	return fo
}
