package split

import (
	"fmt"
	"log/slog"
	"rigel-client/util"
	"strconv"
	"time"
)

type ChunkState struct {
	Index      string
	FileName   string
	ObjectName string
	Offset     int64
	Size       int64
	LastSend   time.Time
	Acked      int
}

// SplitFile 支持指定范围分片（length>0）或全文件分片（length≤0）
// 参数说明：
//
//	size: 全局文件总大小（字节）
//	start: 分片起始字节（左闭，length≤0时，start=0则全文件，start>0则从start到文件末尾）
//	length: 分片长度（字节）→ length>0：指定范围；length≤0：全量分片（从start到文件末尾）
//	fileName: 原始文件名
//	chunks: 存储分片信息的安全Map
//	pre: 日志前缀
//	logger: 日志对象
func SplitFilebyRange(size int64, start, length int64, fileName string, chunks *util.SafeMap,
	pre string, logger *slog.Logger) error {

	// -------------------------- 1. 核心逻辑：处理 length ≤ 0 的全量分片 --------------------------
	var (
		rangeStart int64 // 最终分片的起始位置（左闭）
		rangeEnd   int64 // 最终分片的结束位置（右开）
	)

	// 情况1：length ≤ 0 → 全量分片（从start到文件末尾）
	if length <= 0 {
		rangeStart = start
		rangeEnd = size // 结束位置 = 文件总大小（右开）
		logger.Info("检测到 length ≤ 0，执行全量分片", slog.String("pre", pre),
			"start", rangeStart, "end", rangeEnd, "total_file_size", size)
	} else {
		// 情况2：length > 0 → 指定范围分片
		rangeStart = start
		rangeEnd = start + length
	}

	// -------------------------- 2. 范围合法性校验（兼容全量/指定范围） --------------------------
	if rangeStart < 0 {
		return fmt.Errorf("start 不能为负数（当前值：%d）", rangeStart)
	}
	if rangeStart >= size {
		return fmt.Errorf("start(%d) 超出文件总大小(%d)，无数据可分片", rangeStart, size)
	}
	// 修正结束位置（避免超出文件总大小）
	if rangeEnd > size {
		logger.Warn("分片范围超出文件总大小，自动截断", slog.String("pre", pre),
			"requested_end", rangeEnd, "file_total_size", size)
		rangeEnd = size
	}
	// 校验有效分片长度（必须 > 0）
	effectiveLength := rangeEnd - rangeStart
	if effectiveLength <= 0 {
		return fmt.Errorf("无有效分片数据（start=%d, end=%d, file_size=%d）", rangeStart, rangeEnd, size)
	}

	// -------------------------- 3. 初始化分片变量 --------------------------
	var (
		offset int64 = rangeStart // 分片起始偏移（从rangeStart开始）
		index  int   = 0          // 分片索引
	)
	// 自动选择分片大小（基于有效长度）
	chunkSize := util.AutoSelectChunkSize(effectiveLength)

	// -------------------------- 4. 执行分片（左闭右开） --------------------------
	for offset < rangeEnd {
		// 计算当前分片大小
		partSize := chunkSize
		if offset+partSize > rangeEnd {
			partSize = rangeEnd - offset
		}

		// 构造分片名称
		partName := fmt.Sprintf("%s.part.%05d", fileName, index)

		// 存储分片信息
		chunks.Set(strconv.Itoa(index), &ChunkState{
			Index:      strconv.Itoa(index),
			FileName:   fileName,
			ObjectName: partName,
			Offset:     offset,
			Size:       partSize,
			Acked:      0,
		})

		// 日志打印
		logger.Info("SplitFile", slog.String("pre", pre),
			"partName", partName,
			"global_start", offset,
			"global_end", offset+partSize,
			"part_size", partSize,
			"range_start", rangeStart,
			"range_end", rangeEnd,
			"total_file_size", size)

		offset += partSize
		index++
	}

	logger.Info("分片完成", slog.String("pre", pre),
		"total_chunks", index,
		"range_start", rangeStart,
		"range_end", rangeEnd,
		"effective_length", effectiveLength)

	return nil
}

//func SplitFile(size int64, fileName string, chunks *util.SafeMap,
//	pre string, logger *slog.Logger) error {
//
//	var (
//		offset int64
//		index  int
//	)
//
//	chunkSize := util.AutoSelectChunkSize(size)
//
//	for offset < size {
//		partSize := int64(chunkSize)
//		if offset+partSize > size {
//			partSize = size - offset
//		}
//
//		partName := fmt.Sprintf("%s.part.%05d", fileName, index)
//
//		chunks.Set(strconv.Itoa(index), &ChunkState{
//			Index:      strconv.Itoa(index),
//			FileName:   fileName,
//			ObjectName: partName,
//			Offset:     offset,
//			Size:       partSize,
//			Acked:      0,
//		})
//
//		offset += partSize
//		index++
//
//		logger.Info("SplitFile", slog.String("pre", pre),
//			"partName", partName, "offset", offset, "size", size)
//	}
//
//	return nil
//}
