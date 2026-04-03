package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

const (
	Debian12AmiID       = "ami-0d985f0b838479990"
	DefaultInstanceType = types.InstanceTypeT2Micro
	DefaultVolumeSize   = int32(10)
	//Timeout             = 30 * time.Second
)

type AWSConfig struct {
	Region       string `json:"region"`       // AWS区域（如 us-east-1）
	AccessKey    string `json:"accessKey"`    // AWS Access Key ID
	SecretKey    string `json:"secretKey"`    // AWS Secret Access Key
	InstanceType string `json:"instanceType"` // 实例类型（可选，默认t2.micro）
	//SSHKeyName   string `json:"sshKeyName"`   // AWS EC2密钥对名称（提前在控制台创建）
}

type ScalingOperate struct {
	region       string             // AWS区域
	accessKey    string             // Access Key ID
	secretKey    string             // Secret Access Key
	instanceType types.InstanceType // 实例类型
	sshKeyName   string             // SSH密钥对名称
	amiID        string             // 镜像ID
	volumeSize   int32              // 磁盘大小
	ec2Client    *ec2.Client        // EC2客户端
}

// CreateVM 创建AWS EC2实例（实现OperateInterface接口）
func (as *ScalingOperate) CreateVM(
	ctx context.Context,
	vmName string,
	pre string,
	logger *slog.Logger,
) (string, error) {
	// 初始化EC2客户端
	if as.ec2Client == nil {
		client, err := as.initEC2Client(ctx)
		if err != nil {
			logger.Error("初始化AWS EC2客户端失败", slog.String("pre", pre), "error", err)
			return "", err
		}
		as.ec2Client = client
	}

	// 构建创建实例请求
	input := &ec2.RunInstancesInput{
		// 基础配置
		ImageId:      aws.String(as.amiID),
		InstanceType: as.instanceType,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		// 磁盘配置
		BlockDeviceMappings: []types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/sda1"),
				Ebs: &types.EbsBlockDevice{
					VolumeSize:          aws.Int32(as.volumeSize),
					DeleteOnTermination: aws.Bool(true),
					VolumeType:          types.VolumeTypeGp2,
				},
			},
		},
		// SSH密钥对
		KeyName: aws.String(as.sshKeyName),
		// 网络配置：分配公网IP
		NetworkInterfaces: []types.InstanceNetworkInterfaceSpecification{
			{
				AssociatePublicIpAddress: aws.Bool(true),
				DeviceIndex:              aws.Int32(0),
				Groups:                   []string{}, // 使用默认安全组
				SubnetId:                 nil,        // 使用默认子网
			},
		},
		// 标签：设置实例名称
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeInstance,
				Tags: []types.Tag{
					{
						Key:   aws.String("Name"),
						Value: aws.String(vmName),
					},
				},
			},
		},
	}

	// 发送创建请求
	result, err := as.ec2Client.RunInstances(ctx, input)
	if err != nil {
		// 解析AWS错误
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			err = fmt.Errorf("AWS API错误: %s - %s", apiErr.ErrorCode(), apiErr.ErrorMessage())
		}
		logger.Error("创建AWS EC2实例失败",
			slog.String("pre", pre),
			slog.String("vmName", vmName),
			slog.String("region", as.region),
			"error", err)
		return "", err
	}

	// 获取实例ID
	instanceID := aws.ToString(result.Instances[0].InstanceId)
	logger.Info("AWS EC2实例创建成功",
		slog.String("pre", pre),
		slog.String("vmName", vmName),
		slog.String("instanceID", instanceID),
		slog.String("status", string(result.Instances[0].State.Name)),
	)

	return instanceID, nil
}

// GetVMPublicIP 获取AWS EC2实例公网IP（实现OperateInterface接口）
// 注意：vmName参数传入实例ID（对齐Vultr逻辑）
func (as *ScalingOperate) GetVMPublicIP(
	ctx context.Context,
	vmName string, // 实际传入instanceID
	pre string,
	logger *slog.Logger,
) (string, error) {
	// 初始化EC2客户端
	if as.ec2Client == nil {
		client, err := as.initEC2Client(ctx)
		if err != nil {
			logger.Error("初始化AWS EC2客户端失败", slog.String("pre", pre), "error", err)
			return "", err
		}
		as.ec2Client = client
	}

	// 构建查询实例请求
	input := &ec2.DescribeInstancesInput{
		InstanceIds: []string{vmName}, // vmName = instanceID
	}

	// 发送查询请求
	result, err := as.ec2Client.DescribeInstances(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			err = fmt.Errorf("AWS API错误: %s - %s", apiErr.ErrorCode(), apiErr.ErrorMessage())
		}
		logger.Error("查询AWS EC2实例失败",
			slog.String("pre", pre),
			slog.String("instanceID", vmName),
			slog.String("region", as.region),
			"error", err)
		return "", err
	}

	// 解析公网IP
	if len(result.Reservations) == 0 || len(result.Reservations[0].Instances) == 0 {
		return "", fmt.Errorf("实例ID %s 不存在", vmName)
	}
	instance := result.Reservations[0].Instances[0]
	publicIP := aws.ToString(instance.PublicIpAddress)

	if publicIP == "" {
		return "", fmt.Errorf("实例 %s 未分配公网IP", vmName)
	}

	logger.Info("获取AWS EC2实例公网IP",
		slog.String("pre", pre),
		slog.String("instanceID", vmName),
		slog.String("ip", publicIP),
		slog.String("status", string(instance.State.Name)),
	)

	return publicIP, nil
}

// DeleteVM 删除AWS EC2实例（实现OperateInterface接口）
// 注意：vmName参数传入实例ID
func (as *ScalingOperate) DeleteVM(
	ctx context.Context,
	vmName string, // 实际传入instanceID
	pre string,
	logger *slog.Logger,
) error {
	// 初始化EC2客户端
	if as.ec2Client == nil {
		client, err := as.initEC2Client(ctx)
		if err != nil {
			logger.Error("初始化AWS EC2客户端失败", slog.String("pre", pre), "error", err)
			return err
		}
		as.ec2Client = client
	}

	// 构建删除实例请求
	input := &ec2.TerminateInstancesInput{
		InstanceIds: []string{vmName}, // vmName = instanceID
	}

	// 发送删除请求
	result, err := as.ec2Client.TerminateInstances(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			err = fmt.Errorf("AWS API错误: %s - %s", apiErr.ErrorCode(), apiErr.ErrorMessage())
		}
		logger.Error("删除AWS EC2实例失败",
			slog.String("pre", pre),
			slog.String("instanceID", vmName),
			slog.String("region", as.region),
			"error", err)
		return err
	}

	// 解析删除状态
	if len(result.TerminatingInstances) > 0 {
		instance := result.TerminatingInstances[0]
		logger.Info("AWS EC2实例删除成功",
			slog.String("pre", pre),
			slog.String("instanceID", aws.ToString(instance.InstanceId)),
			slog.String("previousStatus", string(instance.PreviousState.Name)),
			slog.String("currentStatus", string(instance.CurrentState.Name)),
		)
	}

	return nil
}

// initEC2Client 初始化EC2客户端
func (as *ScalingOperate) initEC2Client(ctx context.Context) (*ec2.Client, error) {

	// 1. 自定义HTTP客户端（纯Go标准库，无任何SDK中间件，绝对稳定）
	httpClient := &http.Client{
		Timeout: 1 * time.Minute, // 全局超时：包含连接、TLS、读取、响应的所有阶段
		Transport: &http.Transport{ // 细分超时（可选，进一步保障稳定性）
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second, // TCP连接超时
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second, // TLS握手超时
			ResponseHeaderTimeout: 10 * time.Second, // 响应头超时
		},
	}

	// 2. 构建AWS配置（只保留「必须项」，移除所有可选易出错配置）
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(as.region), // 区域（必须）
		// 凭证配置（必须）
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     as.accessKey,
				SecretAccessKey: as.secretKey,
				Source:          "custom-config",
			}, nil
		})),
		// HTTP客户端（带超时，核心）
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("加载AWS配置失败: %w", err)
	}

	// 3. 创建EC2客户端（极简版，SDK默认配置）
	ec2Client := ec2.NewFromConfig(cfg)

	return ec2Client, nil
}

// NewScalingOperate 初始化AWS操作结构体（对齐GCP/Vultr的NewScalingOperate）
func NewScalingOperate(
	awsCfg *AWSConfig,
	sshKey string, // 兼容GCP的sshKey入参（AWS用配置中的SSHKeyName）
	pre string,
	logger *slog.Logger,
) *ScalingOperate {
	// 解析实例类型（默认t2.micro）
	instanceType := DefaultInstanceType
	if awsCfg.InstanceType != "" {
		instanceType = types.InstanceType(awsCfg.InstanceType)
	}

	// 初始化操作结构体
	so := &ScalingOperate{
		region:       awsCfg.Region,
		accessKey:    awsCfg.AccessKey,
		secretKey:    awsCfg.SecretKey,
		instanceType: instanceType,
		sshKeyName:   sshKey,
		amiID:        Debian12AmiID,
		volumeSize:   DefaultVolumeSize,
		ec2Client:    nil, // 延迟初始化客户端
	}

	logger.Info("NewAWSScalingOperate", slog.String("pre", pre), slog.Any("ScalingOperate", *so))
	return so
}

func ExtractAWSFromInterface(data string) (*AWSConfig, error) {
	if data == "" {
		return nil, errors.New("输入的JSON字符串为空")
	}

	config := &AWSConfig{}
	if err := json.Unmarshal([]byte(data), config); err != nil {
		return nil, fmt.Errorf("解析AWS配置失败: %w", err)
	}

	// 基础校验
	if config.Region == "" {
		return nil, errors.New("AWS Region不能为空")
	}
	if config.AccessKey == "" {
		return nil, errors.New("AWS AccessKey不能为空")
	}
	if config.SecretKey == "" {
		return nil, errors.New("AWS SecretKey不能为空")
	}
	//if config.SSHKeyName == "" {
	//	return nil, errors.New("AWS SSHKeyName不能为空")
	//}

	return config, nil
}
