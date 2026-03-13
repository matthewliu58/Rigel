package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"rigel-client/util"
)

// 定义和服务端一致的Header常量
const (
	HeaderFileName     = "X-File-Name"     // 最终合并后的文件名
	HeaderChunkNames   = "X-Chunk-Names"   // 逗号分隔的分片名列表（合并顺序）
	HeaderDeleteChunks = "X-Delete-Chunks" // 是否删除分片（true/false）
)

// ChunkMergeClient 分片合并客户端（直接返回原始响应内容）
// 参数说明：
//
//	serverURL: 服务端接口地址（如 "http://localhost:8080/api/v1/chunk/merge"）
//	finalFileName: 合并后的最终文件名
//	chunkNames: 分片名列表（按合并顺序）
//	deleteChunks: 合并后是否删除分片
func ChunkMergeClient(ctx context.Context, serverURL, finalFileName string,
	chunkNames []string, deleteChunks bool, pre string,
	logger *slog.Logger) (string, int, error) {

	logger.Info("ChunkMergeClient", slog.String("pre", pre),
		slog.String("finalFileName", finalFileName), slog.Any("chunkNames", chunkNames))

	// 1. 校验必要参数
	if serverURL == "" {
		return "", 0, fmt.Errorf("server URL 不能为空")
	}
	if finalFileName == "" {
		return "", 0, fmt.Errorf("最终文件名不能为空")
	}
	if len(chunkNames) == 0 {
		return "", 0, fmt.Errorf("分片名列表不能为空")
	}

	mergeReq := util.ChunkMergeRequest{
		FinalFileName: finalFileName,
		ChunkNames:    chunkNames,
		DeleteChunks:  deleteChunks,
	}
	b, _ := json.Marshal(mergeReq)

	// 2. 构造请求（分片合并接口不需要请求体，参数都在Header中）
	req, err := http.NewRequest("POST", serverURL, bytes.NewReader(b))
	if err != nil {
		return "", 0, fmt.Errorf("创建请求失败: %v", err)
	}

	// 3. 设置请求Header
	// 设置最终文件名
	req.Header.Set(HeaderFileName, finalFileName)
	// 设置分片名列表（逗号分隔）
	//req.Header.Set(HeaderChunkNames, strings.Join(chunkNames, ","))
	// 设置是否删除分片
	//req.Header.Set(HeaderDeleteChunks, fmt.Sprintf("%t", deleteChunks))
	// 设置Content-Type（虽然没有请求体，但建议设置）
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	// 4. 创建HTTP客户端并发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 5. 读取响应内容（原始字符串）
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, fmt.Errorf("读取响应失败: %v", err)
	}

	logger.Info("ChunkMergeClient", slog.String("pre", pre),
		"respBody", string(respBody), "StatusCode", resp.StatusCode)

	// 返回原始响应内容、HTTP状态码
	return string(respBody), resp.StatusCode, nil
}
