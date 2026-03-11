package upload

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"rigel-client/download"
	"rigel-client/upload/split"
	upload2 "rigel-client/upload/upload"
	"rigel-client/util"
	"time"
)

func UploadDirect(uploadInfo UploadInfo, pre string, logger *slog.Logger) error {

	// 3. 获取文件真实长度
	ctx := context.Background()
	var fileSize int64
	var err error
	switch uploadInfo.Source.SourceType {
	case download.GCPCLoud:
		fileSize, err = download.GetGCSObjectSize(ctx, uploadInfo.Source.BucketName,
			uploadInfo.File.FileName, uploadInfo.Source.CredFile, pre, logger)
	case download.RemoteDisk:
		RemoteDiskSSHConfig := util.SSHConfig{
			User:     uploadInfo.Source.User,
			Host:     uploadInfo.Source.Host + ":" + uploadInfo.Source.SSHPort,
			Password: uploadInfo.Source.Password,
		}
		fileSize, err = download.GetRemoteFileSize(ctx, RemoteDiskSSHConfig,
			uploadInfo.Source.RemoteDir, uploadInfo.File.FileName, pre, logger)
	case download.LocalDisk:
		fileSize, err = download.GetLocalFileSize(ctx, uploadInfo.LocalBaseDir, uploadInfo.File.FileName, pre, logger)
	}
	if err != nil {
		logger.Error("Get file size failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	logger.Info("Get file size success", slog.String("pre", pre), slog.Int64("size", fileSize))

	// 4. 文件分块
	chunks := util.NewSafeMap()

	chunkSize, err := split.SplitFilebyRange(fileSize, uploadInfo.File.Start, uploadInfo.File.Length,
		uploadInfo.File.FileName, uploadInfo.File.NewFileName, chunks, pre, logger)
	if err != nil {
		logger.Error("Split file failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	//512MB
	inMemory := false
	if chunkSize >= int64(512*1024*1024) {
		inMemory = true
	}

	//启动定时重传 & check传输完毕
	done := make(chan struct{})
	events := make(chan ChunkEvent, 100)
	interval := 10 * time.Duration(time.Second)
	expire := 120 * time.Duration(time.Second)
	StartChunkTimeoutChecker_(ctx, chunks, interval, expire, events, pre, logger)

	//启动消费者 默认一个http并发度
	workerPool := NewWorkerPool_(QueueBufferSize, util.RoutingInfo{}, UploadDirectImp, inMemory, pre, logger)

	//events 消费
	go ChunkEventLoop_(ctx, chunks, workerPool, uploadInfo, events, done, pre, logger)

	// 4. 启动分片上传
	go StartChunkSubmitLoop_(ctx, chunks, workerPool, uploadInfo,
		false, nil, pre, logger)

	newFileName := uploadInfo.File.NewFileName

	// 5分钟超时定时器
	timeout := 5 * time.Minute
	select {
	case <-done:
		logger.Info("Function 正常完成", slog.String("per", pre), slog.String("newFileName", newFileName))
	case <-time.After(timeout):
		logger.Warn("等待 5 分钟超时，退出等待", slog.String("per", pre),
			slog.String("newFileName", newFileName))
		return fmt.Errorf("等待 5 分钟超时，退出等待, newFileName: %s", newFileName)
	}

	logger.Info("主程序执行完毕", slog.String("per", pre), slog.String("newFileName", newFileName))
	return nil
}

func UploadDirectImp(task ChunkTask_, hops string, rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	logger.Info("UploadDirectImp", slog.String("pre", pre), slog.Any("task", task))

	ctx := task.Ctx
	source_ := task.Source
	file := task.File
	dest := task.Dest
	var reader io.ReadCloser = nil
	var err error
	if source_.SourceType == GCPCLoud {
		reader, err = download.DownloadFromGCSbyClient(ctx, task.LocalBaseDir, source_.BucketName,
			file.FileName, task.ObjectName, source_.CredFile, file.Start, file.Length, inMemory, pre, logger)
		if err != nil {
			logger.Error("DownloadFromGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	} else if source_.SourceType == RemoteDisk {

		RemoteDiskSSHConfig := util.SSHConfig{
			User:     task.Source.User,
			Host:     task.Source.Host + ":" + task.Source.SSHPort,
			Password: task.Source.Password,
		}

		reader, _, err = download.SSHDDReadRangeChunk(ctx, RemoteDiskSSHConfig, source_.RemoteDir, file.FileName,
			task.ObjectName, task.LocalBaseDir, file.Start, file.Length, "", inMemory, pre, logger)
		if err != nil {
			logger.Error("SSHDDReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	} else if source_.SourceType == LocalDisk {
		reader, _, err = download.LocalReadRangeChunk(ctx, task.LocalBaseDir, file.FileName,
			file.Start, file.Length, pre, logger)
		if err != nil {
			logger.Error("LocalReadRangeChunk failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	}
	defer reader.Close()

	logger.Info("download objectName success", slog.String("pre", pre),
		slog.String("objectName", task.ObjectName))

	if dest.DestType == GCPCLoud {
		if err := upload2.UploadToGCSbyClient(ctx, task.LocalBaseDir, dest.BucketName,
			task.ObjectName, dest.CredFile, inMemory, reader, pre, logger); err != nil {
			logger.Error("UploadToGCSbyClient failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	} else if dest.DestType == RemoteDisk {
		req := upload2.ChunkUploadRequest{
			ServerURL:     dest.FileSys.Upload,
			FinalFileName: task.ObjectName,
			ChunkName:     task.ObjectName,
			LocalBaseDir:  task.LocalBaseDir,
		}
		if _, err := upload2.UploadFileChunk(req, inMemory, reader, pre, logger); err != nil {
			logger.Error("ChunkUploadHandler failed", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
	}

	logger.Info("ClientUploadHandler success", slog.String("pre", pre))
	return nil
}
