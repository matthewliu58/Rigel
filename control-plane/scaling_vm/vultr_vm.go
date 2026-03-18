package scaling_vm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const vultrAPIBase = "https://api.vultr.com/v2"

// =====================
// Create VM
// =====================

type vultrCreateInstanceReq struct {
	Region   string   `json:"region"`
	Plan     string   `json:"plan"`
	OsID     int      `json:"os_id"`
	Label    string   `json:"label,omitempty"`
	Hostname string   `json:"hostname,omitempty"`
	SSHKeys  []string `json:"sshkey_id,omitempty"`
}

type vultrCreateInstanceResp struct {
	Instance struct {
		ID string `json:"id"`
	} `json:"instance"`
}

// CreateVultrVM 创建 Vultr 云主机
func CreateVultrVM(
	ctx context.Context,
	logger *slog.Logger,
	apiKey string,
	region string,
	plan string,
	vmName string,
	sshKeyIDs []string,
) (string, error) {

	reqBody := vultrCreateInstanceReq{
		Region:   region,
		Plan:     plan,
		OsID:     535, // Debian 12
		Label:    vmName,
		Hostname: vmName,
		SSHKeys:  sshKeyIDs,
	}

	data, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		vultrAPIBase+"/instances",
		bytes.NewReader(data),
	)
	if err != nil {
		logger.Error("创建 Vultr VM 请求失败", "error", err)
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("创建 Vultr VM HTTP 失败", "error", err)
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("创建 Vultr VM 失败: %s", body)
	}

	var result vultrCreateInstanceResp
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	logger.Info("Vultr VM 创建成功",
		"vmName", vmName,
		"instanceID", result.Instance.ID,
	)

	return result.Instance.ID, nil
}

// =====================
// Get External IP
// =====================

type vultrGetInstanceResp struct {
	Instance struct {
		ID     string `json:"id"`
		MainIP string `json:"main_ip"`
		Status string `json:"status"`
	} `json:"instance"`
}

// GetVultrVMExternalIP 获取 Vultr VM 公网 IP
func GetVultrVMExternalIP(
	ctx context.Context,
	logger *slog.Logger,
	apiKey string,
	instanceID string,
) (string, error) {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		vultrAPIBase+"/instances/"+instanceID,
		nil,
	)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("获取 Vultr VM 失败: %s", body)
	}

	var result vultrGetInstanceResp
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	logger.Info("获取 Vultr VM 公网 IP",
		"instanceID", instanceID,
		"ip", result.Instance.MainIP,
		"status", result.Instance.Status,
	)

	return result.Instance.MainIP, nil
}

// =====================
// Delete VM
// =====================

// DeleteVultrVM 删除 Vultr 云主机
func DeleteVultrVM(
	ctx context.Context,
	logger *slog.Logger,
	apiKey string,
	instanceID string,
) error {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		vultrAPIBase+"/instances/"+instanceID,
		nil,
	)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("删除 Vultr VM 失败: %s", body)
	}

	logger.Info("Vultr VM 删除成功", "instanceID", instanceID)
	return nil
}
