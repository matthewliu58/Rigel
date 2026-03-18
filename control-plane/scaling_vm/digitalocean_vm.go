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

const doAPIBase = "https://api.digitalocean.com/v2"

// =====================
// Create VM (Droplet)
// =====================

type doCreateDropletReq struct {
	Name       string   `json:"name"`
	Region     string   `json:"region"`
	Size       string   `json:"size"`
	Image      string   `json:"image"`
	SSHKeys    []string `json:"ssh_keys,omitempty"`
	IPv6       bool     `json:"ipv6"`
	Monitoring bool     `json:"monitoring"`
	Tags       []string `json:"tags,omitempty"`
}

type doCreateDropletResp struct {
	Droplet struct {
		ID int64 `json:"id"`
	} `json:"droplet"`
}

// CreateDigitalOceanVM 创建 DigitalOcean Droplet
//
// region: e.g. "nyc3" / "fra1" / "sgp1"
// size:   e.g. "s-2vcpu-4gb"
// image:  e.g. "debian-12-x64"
// sshKeyFingerprints: SSH key fingerprint（不是内容）
func CreateDigitalOceanVM(
	ctx context.Context,
	logger *slog.Logger,
	apiToken string,
	region string,
	size string,
	image string,
	vmName string,
	sshKeyFingerprints []string,
) (int64, error) {

	reqBody := doCreateDropletReq{
		Name:       vmName,
		Region:     region,
		Size:       size,
		Image:      image,
		SSHKeys:    sshKeyFingerprints,
		IPv6:       false,
		Monitoring: true,
		Tags:       []string{"scaling", "arcturus"},
	}

	data, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		doAPIBase+"/droplets",
		bytes.NewReader(data),
	)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("创建 DigitalOcean VM 失败: %s", body)
	}

	var result doCreateDropletResp
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	logger.Info("DigitalOcean VM 创建成功",
		"vmName", vmName,
		"dropletID", result.Droplet.ID,
	)

	return result.Droplet.ID, nil
}

// =====================
// Get External IP
// =====================

type doGetDropletResp struct {
	Droplet struct {
		ID       int64  `json:"id"`
		Status   string `json:"status"`
		Networks struct {
			V4 []struct {
				IPAddress string `json:"ip_address"`
				Type      string `json:"type"`
			} `json:"v4"`
		} `json:"networks"`
	} `json:"droplet"`
}

// GetDigitalOceanVMExternalIP 获取 Droplet 公网 IP
func GetDigitalOceanVMExternalIP(
	ctx context.Context,
	logger *slog.Logger,
	apiToken string,
	dropletID int64,
) (string, error) {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s/droplets/%d", doAPIBase, dropletID),
		nil,
	)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("获取 DigitalOcean VM 失败: %s", body)
	}

	var result doGetDropletResp
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	for _, net := range result.Droplet.Networks.V4 {
		if net.Type == "public" {
			logger.Info("获取 DigitalOcean VM 公网 IP",
				"dropletID", dropletID,
				"ip", net.IPAddress,
				"status", result.Droplet.Status,
			)
			return net.IPAddress, nil
		}
	}

	return "", fmt.Errorf("尚未分配公网 IP")
}

// =====================
// Delete VM
// =====================

// DeleteDigitalOceanVM 删除 Droplet
func DeleteDigitalOceanVM(
	ctx context.Context,
	logger *slog.Logger,
	apiToken string,
	dropletID int64,
) error {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		fmt.Sprintf("%s/droplets/%d", doAPIBase, dropletID),
		nil,
	)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("删除 DigitalOcean VM 失败: %s", body)
	}

	logger.Info("DigitalOcean VM 删除成功", "dropletID", dropletID)
	return nil
}
