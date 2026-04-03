package vultr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	VultrAPIBase = "https://api.vultr.com/v2"
	OsID         = 535 // Debian 12
	Plan         = "voc-g-8c-32gb-160s"
	//Timeout      = 1 * time.Minute
)

type Config struct {
	APIKey string `json:"apiKey"` // Vultr API
	Region string `json:"region"` // "ewr"
	//Plan   string `json:"plan"`   // "vc2-1c-2gb"
	//SSHKeys string `json:"sshKeys"`
}

type ScalingOperate struct {
	apiKey  string   // Vultr API
	region  string   // region
	plan    string   // specification
	sshKeys []string //
	osID    int      // os
	timeout time.Duration
}

type createInstanceReq struct {
	Region   string   `json:"region"`
	Plan     string   `json:"plan"`
	OsID     int      `json:"os_id"`
	Label    string   `json:"label,omitempty"`
	Hostname string   `json:"hostname,omitempty"`
	SSHKeys  []string `json:"sshkey_id,omitempty"`
}

type createInstanceResp struct {
	Instance struct {
		ID string `json:"id"`
	} `json:"instance"`
}

type getInstanceResp struct {
	Instance struct {
		ID     string `json:"id"`
		MainIP string `json:"main_ip"`
		Status string `json:"status"`
	} `json:"instance"`
}

func (vc *ScalingOperate) CreateVM(ctx context.Context, vmName string, pre string, logger *slog.Logger) (string, error) {

	reqBody := createInstanceReq{
		Region:   vc.region,
		Plan:     vc.plan,
		OsID:     vc.osID,
		Label:    vmName,
		Hostname: vmName,
		SSHKeys:  vc.sshKeys,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		logger.Error("Failed to serialize request body for creating VM", slog.String("pre", pre),
			slog.String("vmName", vmName), slog.Any("err", err))
		return "", err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		VultrAPIBase+"/instances",
		bytes.NewReader(data),
	)
	if err != nil {
		logger.Error("Failed to create VM", slog.String("pre", pre), slog.Any("err", err))
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+vc.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: vc.timeout}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Failed to send VM creation request", slog.String("pre", pre), slog.Any("err", err))
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		err := fmt.Errorf("failed to create Vultr VM: %s", body)
		logger.Error("failed to create VM", slog.String("pre", pre), slog.Any("err", err))
		return "", err
	}

	var result createInstanceResp
	if err := json.Unmarshal(body, &result); err != nil {
		logger.Error("failed to parse create VM response", slog.String("pre", pre), slog.Any("err", err))
		return "", err
	}

	logger.Info("successfully created VM", slog.String("pre", pre), slog.String("instanceID", result.Instance.ID))

	return result.Instance.ID, nil
}

func (vc *ScalingOperate) GetVMPublicIP(ctx context.Context, vmName string, pre string, logger *slog.Logger) (string, error) {

	// Vultr API通过instanceID查询，因此vmName参数实际传入instanceID
	instanceID := vmName

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		VultrAPIBase+"/instances/"+instanceID,
		nil,
	)
	if err != nil {
		logger.Error("failed to create Vultr query IP request", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+vc.apiKey)

	client := &http.Client{Timeout: vc.timeout}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("failed to send Vultr query IP request", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		err := fmt.Errorf("failed to get Vultr VM IP: %s", body)
		logger.Error("failed to query Vultr VM IP", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return "", err
	}

	var result getInstanceResp
	if err := json.Unmarshal(body, &result); err != nil {
		logger.Error("failed to parse Vultr query IP response", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return "", err
	}

	logger.Info("successfully obtained Vultr VM public IP", slog.String("pre", pre), slog.String("instanceID", instanceID),
		slog.String("ip", result.Instance.MainIP), slog.String("status", result.Instance.Status))

	return result.Instance.MainIP, nil
}

func (vc *ScalingOperate) DeleteVM(ctx context.Context, vmName string, pre string, logger *slog.Logger) error {

	instanceID := vmName

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		VultrAPIBase+"/instances/"+instanceID,
		nil,
	)
	if err != nil {
		logger.Error("failed to create Vultr delete VM request", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return err
	}

	req.Header.Set("Authorization", "Bearer "+vc.apiKey)

	client := &http.Client{Timeout: vc.timeout}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("failed to send Vultr delete VM request", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("failed to delete Vultr VM: %s", body)
		logger.Error("failed to delete Vultr VM", slog.String("pre", pre),
			slog.String("instanceID", instanceID), slog.Any("err", err))
		return err
	}

	logger.Info("successfully deleted Vultr VM", slog.String("pre", pre), slog.String("instanceID", instanceID))

	return nil
}

func NewScalingOperate(cfg *Config, sshKey string, pre string, logger *slog.Logger) *ScalingOperate {

	var sshKeys []string
	sshKeys = append(sshKeys, sshKey)

	so := &ScalingOperate{
		apiKey:  cfg.APIKey,
		region:  cfg.Region,
		plan:    Plan,
		sshKeys: sshKeys,
		osID:    OsID,
		timeout: 1 * time.Minute,
	}
	logger.Info("NewScalingOperate", slog.String("pre", pre), slog.Any("ScalingOperate", *so))

	return so
}

func ExtractVultrFromInterface(data string) (*Config, error) {

	if data == "" {
		return nil, errors.New("input JSON string is empty")
	}

	config := &Config{}
	if err := json.Unmarshal([]byte(data), config); err != nil {
		return nil, fmt.Errorf("failed to parse Vultr configuration: %w", err)
	}

	// Basic validation
	if config.APIKey == "" {
		return nil, errors.New("Vultr APIKey must not be empty")
	}
	if config.Region == "" {
		return nil, errors.New("Vultr Region must not be empty")
	}

	return config, nil
}
