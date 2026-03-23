package upload

import (
	"fmt"
	"golang.org/x/time/rate"
	"log/slog"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"time"
)

func RedirectImp(
	fo base.FileOperateInterfaces,
	task ChunkTask, hops string, rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	logger.Info("UploadRedirectImp", slog.String("pre", pre), slog.Any("task", task))

	// --------------- 第一步：初始状态设置（Acked=1）---------------
	// 先获取当前分片的基础信息（避免空指针）
	chunkVal, ok := task.Chunks.Get(task.Index)
	if !ok {
		err := fmt.Errorf("chunk index %d not found in Chunks map", task.Index)
		logger.Error("get chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	chunk, ok := chunkVal.(*split.ChunkState)
	if !ok {
		err := fmt.Errorf("chunk index %d type is not *split.ChunkState", task.Index)
		logger.Error("chunk type assert failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// 初始状态：标记为开始传输（Acked=1）
	initialChunkState := &split.ChunkState{
		Index:       chunk.Index,
		FileName:    chunk.FileName,
		NewFileName: chunk.NewFileName,
		ObjectName:  chunk.ObjectName,
		Offset:      chunk.Offset,
		Size:        chunk.Size,
		LastSend:    time.Now(),
		Acked:       int(ChunkStatusTransferring), // 1=开始传输
	}
	task.Chunks.Set(task.Index, initialChunkState)
	logger.Info("set chunk initial state", slog.String("pre", pre), slog.String("index", task.Index), slog.Int("acked", 1))

	// 定义defer函数：异常时统一设置Acked=0（兜底）
	var finalErr error
	defer func() {
		if finalErr != nil {
			// 出错时：更新状态为失败（Acked=0）
			errorChunkState := &split.ChunkState{
				Index:       chunk.Index,
				FileName:    chunk.FileName,
				NewFileName: chunk.NewFileName,
				ObjectName:  chunk.ObjectName,
				Offset:      chunk.Offset,
				Size:        chunk.Size,
				LastSend:    initialChunkState.LastSend,
				Acked:       int(ChunkStatusTransferFailed), // 0=传输失败
			}
			task.Chunks.Set(task.Index, errorChunkState)
			logger.Error("chunk transfer failed, set acked=0", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		}
	}()

	// --------------- 第二步：获取Reader（读取源文件）---------------
	ctx := task.Ctx
	// 核心修改1：先检查ctx是否已取消，避免无效操作
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before get reader: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
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

	logger.Info("download object success", slog.String("pre", pre), slog.String("objectName", task.ObjectName))

	// --------------- 第三步：上传到目标端 ---------------
	// 核心修改2：上传前检查ctx是否已取消
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before upload: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	if fo.UploadFile.UploadFile == nil {
		logger.Error("UploadFile is nil", slog.String("pre", pre))
		return fmt.Errorf("%w: UploadFile is nil", ErrInterfaceNotImplemented)
	}
	err = fo.UploadFile.UploadFile(ctx, task.ObjectName, hops, rateLimiter, reader, inMemory, pre, logger)
	if err != nil {
		logger.Error("UploadFile failed", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", err))
		return err
	}

	// --------------- 第四步：成功状态更新（Acked=2）---------------
	// 核心修改3：更新状态前最后检查ctx（防止更新过程中取消）
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before update success state: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}
	successChunkState := &split.ChunkState{
		Index:       chunk.Index,
		FileName:    chunk.FileName,
		NewFileName: chunk.NewFileName,
		ObjectName:  chunk.ObjectName,
		Offset:      chunk.Offset,
		Size:        chunk.Size,
		LastSend:    initialChunkState.LastSend, // 保留开始传输时间
		Acked:       int(ChunkStatusCompleted),  // 2=传输成功
	}
	task.Chunks.Set(task.Index, successChunkState)
	logger.Info("chunk transfer success, set acked=2", slog.String("pre", pre), slog.String("index", task.Index))

	logger.Info("UploadRedirectImp success", slog.String("pre", pre))
	return nil
}
