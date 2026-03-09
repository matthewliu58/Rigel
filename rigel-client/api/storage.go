package api

import (
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/util"
	"strconv"
	"strings"
)

// 核心Header常量
const (
	//HeaderFileName     = "X-File-Name"     // 最终合并后的文件名
	HeaderChunkName    = "X-Chunk-Name"    // 单个分片的自定义名称
	HeaderChunkNames   = "X-Chunk-Names"   // 逗号分隔的分片名列表（合并顺序）
	HeaderDeleteChunks = "X-Delete-Chunks" // 是否删除分片（true/false）
)

// ChunkMergeConfig 分片合并配置（适配发送端指定规则）
type ChunkMergeConfig struct {
	BaseDir       string   // 分片存储目录
	FinalFileName string   // 最终合并后的文件名
	ChunkNames    []string // 发送端指定的分片名列表（按合并顺序）
	DeleteChunks  bool     // 合并后是否删除分片
}

// ChunkUploadHandler 分片上传接口（接收发送端自定义命名的分片）
func ChunkUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("ChunkUploadHandler start", slog.String("pre", pre))

		// 1. 获取Header参数
		finalFileName := c.GetHeader(HeaderFileName)
		chunkName := c.GetHeader(HeaderChunkName)
		if finalFileName == "" || chunkName == "" {
			logger.Error("ChunkUploadHandler missing header", slog.String("pre", pre),
				slog.String("finalFileName", finalFileName),
				slog.String("chunkName", chunkName))
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Missing required headers: " + HeaderFileName + "/" + HeaderChunkName,
			})
			return
		}

		// 2. 获取上传的分片文件
		file, _, err := c.Request.FormFile("file")
		if err != nil {
			logger.Error("ChunkUploadHandler get chunk file failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Get chunk file failed: " + err.Error()})
			return
		}
		defer file.Close()

		// 3. 确保本地目录存在
		if err := os.MkdirAll(LocalBaseDir, 0755); err != nil {
			logger.Error("ChunkUploadHandler create base dir failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Create local dir failed: " + err.Error()})
			return
		}

		// 4. 生成分片保存路径（使用发送端指定的分片名）
		chunkPath := filepath.Join(LocalBaseDir, chunkName)

		// 5. 保存分片文件
		if err := SaveFileChunk(file, chunkPath, pre, logger); err != nil {
			logger.Error("ChunkUploadHandler save chunk failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Save chunk failed: " + err.Error()})
			return
		}

		// 6. 返回成功响应
		logger.Info("ChunkUploadHandler success", slog.String("pre", pre),
			slog.String("finalFileName", finalFileName),
			slog.String("chunkName", chunkName),
			slog.String("chunkPath", chunkPath))
		c.JSON(http.StatusOK, gin.H{
			"code":       200,
			"message":    "chunk upload success",
			"final_file": finalFileName,
			"chunk_name": chunkName,
			"save_path":  chunkPath,
		})
	}
}

// ChunkMergeHandler 分片合并接口（按发送端指定的顺序合并）
func ChunkMergeHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("ChunkMergeHandler start", slog.String("pre", pre))

		// 1. 获取Header参数
		finalFileName := c.GetHeader(HeaderFileName)
		chunkNamesStr := c.GetHeader(HeaderChunkNames)
		if finalFileName == "" || chunkNamesStr == "" {
			logger.Error("ChunkMergeHandler missing header", slog.String("pre", pre),
				slog.String("finalFileName", finalFileName),
				slog.String("chunkNamesStr", chunkNamesStr))
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Missing required headers: " + HeaderFileName + "/" + HeaderChunkNames,
			})
			return
		}

		// 2. 解析分片名列表（按发送端指定的顺序）
		chunkNames := strings.Split(chunkNamesStr, ",")
		if len(chunkNames) == 0 {
			logger.Error("ChunkMergeHandler empty chunk list", slog.String("pre", pre), slog.String("chunkNamesStr", chunkNamesStr))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid " + HeaderChunkNames + ": empty chunk list"})
			return
		}

		// 3. 解析是否删除分片（默认true）
		deleteChunks := true
		deleteChunksStr := c.GetHeader(HeaderDeleteChunks)
		if deleteChunksStr != "" {
			if b, err := strconv.ParseBool(deleteChunksStr); err == nil {
				deleteChunks = b
			}
		}

		// 4. 构建合并配置
		mergeCfg := ChunkMergeConfig{
			BaseDir:       LocalBaseDir,
			FinalFileName: finalFileName,
			ChunkNames:    chunkNames,
			DeleteChunks:  deleteChunks,
		}

		// 5. 执行分片合并
		if err := MergeFileChunks(mergeCfg, pre, logger); err != nil {
			logger.Error("ChunkMergeHandler merge failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":  500,
				"error": "Merge chunks failed: " + err.Error(),
			})
			return
		}

		// 6. 返回成功响应
		finalPath := filepath.Join(LocalBaseDir, finalFileName)
		logger.Info("ChunkMergeHandler success", slog.String("pre", pre),
			slog.String("finalFileName", finalFileName),
			slog.String("finalPath", finalPath),
			slog.Any("mergedChunks", chunkNames))
		c.JSON(http.StatusOK, gin.H{
			"code":          200,
			"message":       "file merge success (by sender order)",
			"final_file":    finalFileName,
			"final_path":    finalPath,
			"merged_chunks": chunkNames,
		})
	}
}

// SaveFileChunk 保存单个分片文件（接收发送端自定义命名）
func SaveFileChunk(chunkFile io.Reader, chunkPath string, pre string, logger *slog.Logger) error {
	// 创建分片目录（如果不存在）
	chunkDir := filepath.Dir(chunkPath)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		logger.Error("SaveFileChunk create dir failed", slog.String("pre", pre),
			slog.String("chunkDir", chunkDir), slog.Any("err", err))
		return err
	}

	// 创建分片文件并写入内容
	chunkOutFile, err := os.Create(chunkPath)
	if err != nil {
		logger.Error("SaveFileChunk create chunk file failed", slog.String("pre", pre),
			slog.String("chunkPath", chunkPath), slog.Any("err", err))
		return err
	}
	defer chunkOutFile.Close()

	// 流式写入（支持大文件）
	if _, err := io.Copy(chunkOutFile, chunkFile); err != nil {
		logger.Error("SaveFileChunk write chunk failed", slog.String("pre", pre),
			slog.String("chunkPath", chunkPath), slog.Any("err", err))
		return err
	}

	logger.Info("SaveFileChunk success", slog.String("pre", pre), slog.String("chunkPath", chunkPath))
	return nil
}

// MergeFileChunks 按发送端指定的顺序合并分片
func MergeFileChunks(cfg ChunkMergeConfig, pre string, logger *slog.Logger) error {
	// 1. 参数校验
	if cfg.BaseDir == "" || cfg.FinalFileName == "" || len(cfg.ChunkNames) == 0 {
		logger.Error("MergeFileChunks invalid config", slog.String("pre", pre), slog.Any("cfg", cfg))
		return os.ErrInvalid
	}

	// 2. 构建最终文件路径
	finalPath := filepath.Join(cfg.BaseDir, cfg.FinalFileName)

	// 3. 创建最终文件
	finalFile, err := os.Create(finalPath)
	if err != nil {
		logger.Error("MergeFileChunks create final file failed", slog.String("pre", pre),
			slog.String("finalPath", finalPath), slog.Any("err", err))
		return err
	}
	defer finalFile.Close()

	// 4. 按发送端指定顺序合并分片
	for idx, chunkName := range cfg.ChunkNames {
		chunkPath := filepath.Join(cfg.BaseDir, chunkName)

		// 检查分片是否存在
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			logger.Error("MergeFileChunks chunk not exist", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.String("chunkPath", chunkPath))
			return err
		}

		// 打开分片文件
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			logger.Error("MergeFileChunks open chunk failed", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.Any("err", err))
			return err
		}

		// 写入分片内容到最终文件
		if _, err := io.Copy(finalFile, chunkFile); err != nil {
			chunkFile.Close()
			logger.Error("MergeFileChunks copy chunk failed", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.Any("err", err))
			return err
		}
		chunkFile.Close()

		// 合并后删除分片（可选）
		if cfg.DeleteChunks {
			if err := os.Remove(chunkPath); err != nil {
				logger.Warn("MergeFileChunks delete chunk failed", slog.String("pre", pre),
					slog.String("chunkName", chunkName), slog.Any("err", err))
			}
		}

		logger.Info("MergeFileChunks processed chunk", slog.String("pre", pre),
			slog.Int("mergeOrder", idx),
			slog.String("chunkName", chunkName))
	}

	logger.Info("MergeFileChunks success", slog.String("pre", pre), slog.String("finalPath", finalPath))
	return nil
}
