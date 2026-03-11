package download

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// LocalReadRangeChunk 本地文件范围读取函数（返回流式Reader，支持范围/完整读取）
// 核心规则：
//  1. length ≤ 0 → 读取整个文件（忽略start，从0开始读全部）
//  2. length > 0  → 读取 [start, start+length) 范围的内容
//
// 参数说明（修正语法问题后）：
//
//	ctx: 上下文（预留，便于扩展超时/取消逻辑）
//	localDir: 本地文件所在目录（如 /home/matth/upload）
//	filename: 要读取的文件名（如 bigfile.bin）
//	start: 读取起始位置（字节）；length≤0时该参数无效
//	length: 读取长度（字节）；≤0时读取全部文件
//	pre: 日志前缀（用于定位请求）
//	logger: 日志对象
//
// 返回值：
//
//	io.ReadCloser: 流式读取的Reader（调用方需Close释放文件句柄）
//	string: 本地文件完整路径（便于日志/校验）
//	error: 错误信息
func LocalReadRangeChunk(
	ctx context.Context,
	localDir string,
	filename string,
	start, length int64,
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, string, error) {
	// 1. 拼接本地文件完整路径
	localFilePath := filepath.Join(localDir, filename)
	localFilePath = filepath.Clean(localFilePath) // 标准化路径（处理多斜杠/相对路径）

	logger.Info("开始本地文件范围读取",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.Int64("start", start),
		slog.Int64("length", length))

	// 2. 基础校验：文件是否存在/是否为文件
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", fmt.Errorf("文件不存在: %s", localFilePath)
		}
		return nil, "", fmt.Errorf("获取文件信息失败: %w", err)
	}
	if fileInfo.IsDir() {
		return nil, "", fmt.Errorf("%s 是目录，不是文件", localFilePath)
	}
	fileTotalSize := fileInfo.Size()
	if fileTotalSize == 0 {
		return nil, "", fmt.Errorf("文件 %s 为空", localFilePath)
	}

	// 3. 处理读取范围（兼容完整读取/范围读取）
	var actualStart, actualLength int64
	if length <= 0 {
		// 读取整个文件
		actualStart = 0
		actualLength = fileTotalSize
		logger.Info("读取整个文件",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Int64("fileTotalSize", fileTotalSize))
	} else {
		// 范围读取：校验起始位置和长度合法性
		if start < 0 {
			return nil, "", fmt.Errorf("起始位置start不能为负: %d", start)
		}
		if start >= fileTotalSize {
			return nil, "", fmt.Errorf("起始位置%d超出文件总大小%d", start, fileTotalSize)
		}

		actualStart = start
		// 修正长度：如果start+length超出文件大小，只读取到文件末尾
		if start+length > fileTotalSize {
			actualLength = fileTotalSize - start
			logger.Warn("读取长度超出文件大小，自动截断",
				slog.String("pre", pre),
				slog.Int64("requestedLength", length),
				slog.Int64("actualLength", actualLength),
				slog.Int64("fileTotalSize", fileTotalSize))
		} else {
			actualLength = length
		}
		logger.Info("读取文件指定范围",
			slog.String("pre", pre),
			slog.Int64("actualStart", actualStart),
			slog.Int64("actualLength", actualLength))
	}

	// 4. 打开文件（只读模式）
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("打开文件失败: %w", err)
	}

	// 5. 定位到读取起始位置
	if _, err := file.Seek(actualStart, io.SeekStart); err != nil {
		file.Close() // 失败时关闭文件句柄
		return nil, "", fmt.Errorf("文件定位到起始位置%d失败: %w", actualStart, err)
	}

	// 6. 封装Reader：限制读取长度 + 实现ReadCloser（统一资源释放）
	// LimitReader限制读取长度，MultiReader确保读取完指定长度后返回EOF
	limitedReader := io.LimitReader(file, actualLength)
	// 封装为ReadCloser，Close时关闭底层文件句柄
	readerWrapper := &localFileReaderWrapper{
		reader: limitedReader,
		file:   file,
		pre:    pre,
		logger: logger,
		path:   localFilePath,
	}

	logger.Info("本地文件Reader创建成功",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.Int64("readStart", actualStart),
		slog.Int64("readLength", actualLength))

	return readerWrapper, localFilePath, nil
}

// localFileReaderWrapper 封装本地文件Reader，实现ReadCloser接口
// 核心：统一资源释放（Close时关闭底层文件句柄）
type localFileReaderWrapper struct {
	reader io.Reader    // 限制长度后的Reader
	file   *os.File     // 底层文件句柄（用于Close）
	pre    string       // 日志前缀
	logger *slog.Logger // 日志对象
	path   string       // 文件路径（用于日志）
	closed bool         // 是否已关闭（避免重复关闭）
}

// Read 实现io.Reader接口：流式读取指定范围的内容
func (w *localFileReaderWrapper) Read(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("reader已关闭，无法读取")
	}
	return w.reader.Read(p)
}

// Close 实现io.ReadCloser接口：释放文件句柄
func (w *localFileReaderWrapper) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.file.Close(); err != nil {
		w.logger.Error("关闭本地文件Reader失败",
			slog.String("pre", w.pre),
			slog.String("filePath", w.path),
			slog.Any("err", err))
		return fmt.Errorf("关闭文件失败: %w", err)
	}
	w.logger.Info("本地文件Reader已关闭",
		slog.String("pre", w.pre),
		slog.String("filePath", w.path))
	return nil
}
