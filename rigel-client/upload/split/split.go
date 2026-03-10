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

func SplitFile(size int64, fileName string, chunks *util.SafeMap,
	pre string, logger *slog.Logger) error {

	var (
		offset int64
		index  int
	)

	chunkSize := util.AutoSelectChunkSize(size)

	for offset < size {
		partSize := int64(chunkSize)
		if offset+partSize > size {
			partSize = size - offset
		}

		partName := fmt.Sprintf("%s.part.%05d", fileName, index)

		chunks.Set(strconv.Itoa(index), &ChunkState{
			Index:      strconv.Itoa(index),
			FileName:   fileName,
			ObjectName: partName,
			Offset:     offset,
			Size:       partSize,
			Acked:      0,
		})

		offset += partSize
		index++

		logger.Info("SplitFile", slog.String("pre", pre),
			"partName", partName, "offset", offset, "size", size)
	}

	return nil
}
