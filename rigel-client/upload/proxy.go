package upload

import (
	"fmt"
	"log/slog"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"time"

	"golang.org/x/time/rate"
)

func RedirectImp(fo base.FileOperateInterfaces, task ChunkTask, hops string,
	rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	//todo 内部触发重新 routing

	logger.Info("UploadRedirectImp", slog.String("pre", pre), slog.Any("task", task))

	// 先获取当前分片的基础信息（避免空指针）
	chunkVal, ok := task.Chunks.Get(task.Index)
	if !ok {
		err := fmt.Errorf("chunk index %d not found in Chunks map", task.Index)
		logger.Error("get chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	chunk, _ := chunkVal.(*split.ChunkState)

	startTime := time.Now()
	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusTransferring) // 1=开始传输
	task.Chunks.Set(task.Index, chunk)
	logger.Info("set chunk initial state", slog.String("pre", pre), slog.String("index", task.Index),
		slog.Int("acked", 1))

	// 定义defer函数：异常时统一设置Acked=0（兜底）
	var finalErr error
	defer func() {
		if finalErr != nil {
			chunk.LastSend = startTime
			chunk.Acked = int(ChunkStatusTransferFailed)
			task.Chunks.Set(task.Index, chunk)
			logger.Error("chunk transfer failed, set acked=0", slog.String("pre", pre),
				slog.String("index", task.Index), slog.Any("err", finalErr))
		}
	}()

	//获取Reader（读取源文件）
	ctx := task.Ctx
	// 核心修改1：先检查ctx是否已取消，避免无效操作
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before get reader: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	upload := task.Upload
	start := chunk.Offset
	length := chunk.Size
	reader, err := GetTransferReader(ctx, fo, upload, start, length, task.ObjectName, inMemory, pre, logger)
	if err != nil {
		return err
	}
	defer func() {
		if reader != nil {
			_ = reader.Close() // 确保Reader关闭，无论成功/失败
		}
	}()

	logger.Info("download object success", slog.String("pre", pre),
		slog.String("objectName", task.ObjectName))

	// 上传到目标端
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before upload: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	if fo.UploadFile == nil {
		logger.Error("UploadFile is nil", slog.String("pre", pre))
		return fmt.Errorf("%w: UploadFile is nil", ErrInterfaceNotImplemented)
	}
	err = fo.UploadFile.UploadFile(ctx, task.ObjectName, length, hops, rateLimiter, reader, inMemory, pre, logger)
	if err != nil {
		logger.Error("UploadFile failed", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", err))
		return err
	} else {
		logger.Info("UploadFile success", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Time("time", time.Now()))
	}

	// 新状态前最后检查
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before update success state: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusCompleted)
	task.Chunks.Set(task.Index, chunk)
	logger.Info("chunk transfer success, set acked=2", slog.String("pre", pre),
		slog.String("index", task.Index), slog.Time("time", time.Now()))

	logger.Info("UploadRedirectImp success", slog.String("pre", pre))
	return nil
}
