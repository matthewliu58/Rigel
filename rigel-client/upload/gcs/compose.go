package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

type Compose struct {
	bucket   string
	credFile string
}

func NewCompose(
	bucket, credFile string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Compose {
	c := &Compose{
		bucket:   bucket,
		credFile: credFile,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

func (c *Compose) ComposeFile(
	ctx context.Context,
	objectName string,
	parts []string,
	pre string,
	logger *slog.Logger,
) error {

	bucket := c.bucket
	//objectName := c.objectName
	credFile := c.credFile
	//parts := c.parts

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled: %w", ctx.Err())
		logger.Error("GCP ComposeTree canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 1. 单文件场景特殊处理（核心优化）
	if len(parts) == 1 {
		// 设置GCP凭证环境变量
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

		// 创建GCS客户端
		ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		client, err := storage.NewClient(ctx_)
		if err != nil {
			logger.Error("create GCS client failed", slog.String("pre", pre), slog.Any("err", err))
			return fmt.Errorf("new storage client failed: %w", err)
		}
		defer client.Close()

		bkt := client.Bucket(bucket)
		partName := parts[0]

		// 1.1 单文件同名：无需操作，直接返回
		if partName == objectName {
			logger.Info("single file name matches final name, skip compose",
				slog.String("pre", pre),
				slog.String("object", objectName))
			return nil
		}

		// 1.2 单文件不同名：复制+成功后删除源文件（带容错）
		logger.Info("start copy single file to final location",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// 执行复制操作
		_, err = bkt.Object(objectName).CopierFrom(bkt.Object(partName)).Run(ctx_)
		if err != nil {
			logger.Error("copy single file failed",
				slog.String("pre", pre),
				slog.String("from", partName),
				slog.String("to", objectName),
				slog.Any("err", err))
			return fmt.Errorf("copy single file failed: %w", err)
		}
		logger.Info("copy single file success",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// 复制成功后删除源文件（容错：删除失败仅告警，不中断流程）
		if delErr := bkt.Object(partName).Delete(ctx_); delErr != nil {
			logger.Warn("delete single source file failed (copy success)",
				slog.String("pre", pre),
				slog.String("partName", partName),
				slog.Any("err", delErr))
		} else {
			logger.Info("delete single source file success",
				slog.String("pre", pre),
				slog.String("partName", partName))
		}

		logger.Info("single file process completed",
			slog.String("pre", pre),
			slog.String("finalObject", objectName))
		return nil
	}

	// 2. 多文件场景：原有树形合成逻辑（保留）
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)
	// 创建GCS客户端
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
	//defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("create GCS client failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("new storage client failed: %w", err)
	}
	defer client.Close()

	bkt := client.Bucket(bucket)
	current := parts // 如需排序可打开注释：util.SortPartStrings(parts)
	level := 0
	var tempObjects []string // 记录所有临时生成的合成文件

	// 树形合成：每次合并最多32个分片（GCS Compose API限制）
	for len(current) > 1 {
		var next []string

		for i := 0; i < len(current); i += 32 {
			end := i + 32
			if end > len(current) {
				end = len(current)
			}
			group := current[i:end]
			tmpObjectName := fmt.Sprintf("%s.compose.%d.%d", objectName, level, i)

			// 构建待合成的对象列表
			var objs []*storage.ObjectHandle
			for _, p := range group {
				objs = append(objs, bkt.Object(p))
			}

			// 执行合成操作
			if _, err := bkt.Object(tmpObjectName).ComposerFrom(objs...).Run(ctx); err != nil {
				logger.Error("compose temp object failed",
					slog.String("pre", pre),
					slog.String("tmpObjectName", tmpObjectName),
					slog.Int("level", level),
					slog.Any("group", group),
					slog.Any("err", err))
				return fmt.Errorf("compose temp object %s failed: %w", tmpObjectName, err)
			}

			next = append(next, tmpObjectName)
			tempObjects = append(tempObjects, tmpObjectName)
			logger.Info("compose temp object success",
				slog.String("pre", pre),
				slog.String("name", tmpObjectName),
				slog.Int("level", level),
				slog.Any("from", group))
		}

		current = next
		level++
	}

	// 3. 多文件合成最终步骤：临时文件→最终文件
	if err := finalizeObject(ctx, bkt, current[0], objectName); err != nil {
		logger.Error("finalize object failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("finalize object failed: %w", err)
	}

	// 4. 清理多文件场景的临时文件和分片
	// 4.1 删除中间临时文件（排除已在finalizeObject删除的最终临时文件）
	for _, tmp := range tempObjects {
		if tmp != current[0] {
			if delErr := bkt.Object(tmp).Delete(ctx); delErr != nil {
				logger.Warn("delete temp object failed",
					slog.String("pre", pre),
					slog.String("tmp", tmp),
					slog.Any("err", delErr))
			}
		}
	}

	// 4.2 删除原始分片文件
	for _, p := range parts {
		if delErr := bkt.Object(p).Delete(ctx); delErr != nil {
			logger.Warn("delete part object failed",
				slog.String("pre", pre),
				slog.String("part", p),
				slog.Any("err", delErr))
		}
	}

	logger.Info("multi file compose success",
		slog.String("pre", pre),
		slog.String("finalObject", objectName))
	return nil
}

// finalizeObject 把临时文件复制到最终位置并删除临时文件
// 仅用于多文件合成的最终步骤
func finalizeObject(ctx context.Context, bkt *storage.BucketHandle, tempName, finalName string) error {
	// 复制临时文件到最终文件
	_, err := bkt.Object(finalName).
		CopierFrom(bkt.Object(tempName)).
		Run(ctx)
	if err != nil {
		return fmt.Errorf("copy temp to final failed: %w", err)
	}

	// 删除临时文件
	if err := bkt.Object(tempName).Delete(ctx); err != nil {
		return fmt.Errorf("delete temp object failed: %w", err)
	}
	return nil
}
