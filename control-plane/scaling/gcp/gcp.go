package gcp

import (
	compute "cloud.google.com/go/compute/apiv1"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/api/option"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	"google.golang.org/protobuf/proto"
	"log/slog"
)

const (
	FireWallTag  = "default-allow-internal"
	InstanceType = "e2-medium"
	SourceImage  = "projects/debian-cloud/global/images/family/debian-12"
)

type Config struct {
	ProjectID string `json:"projectID"` // GCP 项目 ID
	Zone      string `json:"zone"`      // 机房
	CredFile  string `json:"credFile"`  // GCP 凭证文件路径
}

func NewScalingOperate(gcpCfg *Config, sshKey string,
	pre string, logger *slog.Logger) *ScalingOperate {

	so := &ScalingOperate{
		projectID:    gcpCfg.ProjectID,
		zone:         gcpCfg.Zone,
		credFile:     gcpCfg.CredFile,
		fireWallTag:  FireWallTag,  // 填充常量
		instanceType: InstanceType, // 填充常量
		sourceImage:  SourceImage,  // 填充常量
		sshKey:       sshKey,       // 填充额外入参
	}

	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("ScalingOperate", *so))
	return so
}

// ExtractGCPFromInterface 解析JSON字符串为*GCPConfig，自动填充常量默认值
func ExtractGCPFromInterface(data string) (*Config, error) {
	if data == "" {
		return nil, errors.New("输入的JSON字符串为空")
	}

	config := &Config{}
	if err := json.Unmarshal([]byte(data), config); err != nil {
		return nil, err
	}

	return config, nil
}

type ScalingOperate struct {
	projectID    string
	zone         string
	vmPrefix     string
	credFile     string
	fireWallTag  string
	instanceType string
	sourceImage  string
	sshKey       string
}

func (gc *ScalingOperate) CreateVM(
	ctx context.Context,
	vmName string,
	pre string,
	logger *slog.Logger,
) error {

	instancesClient, err := compute.NewInstancesRESTClient(ctx, option.WithCredentialsFile(gc.credFile))
	if err != nil {
		logger.Error("NewInstancesRESTClient failed", slog.String("pre", pre), "error", err)
		return err
	}
	defer instancesClient.Close()

	sshKey := gc.sshKey
	bootDisk := &computepb.AttachedDisk{
		AutoDelete: proto.Bool(true),
		Boot:       proto.Bool(true),
		Type:       proto.String(computepb.AttachedDisk_PERSISTENT.String()),
		InitializeParams: &computepb.AttachedDiskInitializeParams{
			SourceImage: proto.String(gc.sourceImage),
			DiskSizeGb:  proto.Int64(10), // 确保客户端在使用后被关闭
		},
	}

	networkInterface := &computepb.NetworkInterface{
		Network: proto.String("global/networks/default"),
		AccessConfigs: []*computepb.AccessConfig{ // 自动删除
			{
				Name: proto.String("External NAT"),   // 持久化磁盘类型
				Type: proto.String("ONE_TO_ONE_NAT"), // 让系统自动分配公网IP
			},
		},
	}

	instance := &computepb.Instance{
		Name:        proto.String(vmName),
		MachineType: proto.String(fmt.Sprintf("zones/%s/machineTypes/%s", gc.zone, gc.instanceType)), // 使用默认网络
		Disks:       []*computepb.AttachedDisk{bootDisk},
		NetworkInterfaces: []*computepb.NetworkInterface{
			networkInterface,
		},
		Tags: &computepb.Tags{
			Items: []string{"http-server", "https-server", "lb-health-check", gc.fireWallTag},
		},
		Metadata: &computepb.Metadata{
			Items: []*computepb.Items{
				{
					Key:   proto.String("ssh-keys"),
					Value: proto.String(sshKey),
				},
			},
		},
	}

	req := &computepb.InsertInstanceRequest{
		Project:          gc.projectID,
		Zone:             gc.zone,
		InstanceResource: instance,
	}

	op, err := instancesClient.Insert(ctx, req)
	if err != nil {
		logger.Error("Create VM failed", slog.String("pre", pre), slog.String("vmName", vmName),
			slog.String("zone", gc.zone), slog.Any("err", err))
		return err
	}

	logger.Info("VM create operation", slog.String("pre", pre),
		slog.String("vmName", vmName), slog.String("operation", op.Proto().GetName()))
	logger.Info("Check status",
		slog.String("pre", pre), slog.String("operation", op.Proto().GetName()),
		slog.String("zone", gc.zone), slog.String("project", gc.projectID),
		slog.String("cmd", fmt.Sprintf("gcloud compute operations describe %s --zone %s --project %s",
			op.Proto().GetName(), gc.zone, gc.projectID)),
	)

	return nil
}

func (gc *ScalingOperate) GetVMPublicIP(ctx context.Context, vmName string,
	pre string, logger *slog.Logger) (string, error) {

	client, err := compute.NewInstancesRESTClient(ctx, option.WithCredentialsFile(gc.credFile))
	if err != nil {
		logger.Error("NewInstancesRESTClient failed", slog.String("pre", pre), "error", err)
		return "", err
	}
	defer client.Close()

	req := &computepb.GetInstanceRequest{
		Project:  gc.projectID,
		Zone:     gc.zone,
		Instance: vmName,
	}

	vm, err := client.Get(ctx, req)
	if err != nil {
		logger.Error("Get VM info failed", slog.String("pre", pre), "error", err)
		return "", err
	}

	if len(vm.NetworkInterfaces) == 0 || len(vm.NetworkInterfaces[0].AccessConfigs) == 0 {
		return "", fmt.Errorf("no network interface or public network configuration found")
	}

	natIP := vm.NetworkInterfaces[0].AccessConfigs[0].GetNatIP()
	logger.Info("GetNatIP", slog.String("pre", pre), "vmName", vmName, "ip", natIP)
	return natIP, nil
}

// DeleteVM 删除指定的 VM
func (gc *ScalingOperate) DeleteVM(ctx context.Context, vmName string, pre string, logger *slog.Logger) error {

	instancesClient, err := compute.NewInstancesRESTClient(ctx, option.WithCredentialsFile(gc.credFile))
	if err != nil {
		logger.Error("NewInstancesRESTClient failed", slog.String("pre", pre), "error", err)
		return err
	}
	defer instancesClient.Close()

	req := &computepb.DeleteInstanceRequest{
		Project:  gc.projectID,
		Zone:     gc.zone,
		Instance: vmName,
	}

	op, err := instancesClient.Delete(ctx, req)
	if err != nil {
		logger.Error("Delete VM failed", slog.String("pre", pre), slog.String("vmName", vmName),
			slog.String("zone", gc.zone), slog.Any("err", err))
		return err
	}

	logger.Info("VM deletion operation", slog.String("pre", pre),
		slog.String("vmName", vmName), slog.String("operation", op.Proto().GetName()))
	logger.Info("Check the status with the command",
		slog.String("pre", pre), slog.String("operation", op.Proto().GetName()),
		slog.String("zone", gc.zone), slog.String("project", gc.projectID),
		slog.String("cmd", fmt.Sprintf("gcloud compute operations describe %s --zone %s --project %s",
			op.Proto().GetName(), gc.zone, gc.projectID)))

	return nil
}
