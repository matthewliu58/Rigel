package scaling

import (
	"context"
	"control-plane/scaling/aws"
	"control-plane/scaling/gcp"
	"control-plane/scaling/vultr"
	"control-plane/util"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type NodeStatus int

const (
	StateInactive NodeStatus = iota
	StateDormant
	StateTriggered
	StateScalingUp
	StateReleasing
	StatePermanent
	StateEnd
)

const (
	ActionInit    = "init"
	ActionScaleUp = "scale_up"
	ActionSleep   = "sleep"
	ActionStart   = "start"
	ActionRelease = "release"
	ActionAddVM   = "add"
	ActionDelVM   = "del"
)

// ScaleConfig 定义弹性伸缩相关参数
type ScaleConfig struct {
	VolatilityWeight    float64       `json:"volatility_weight"` // 队列与波动相关
	QueueWeight         float64       `json:"queue_weight"`
	DecayFactor         float64       `json:"decay_factor"`
	VolatilityThreshold float64       `json:"volatility_threshold"`
	CostWeight          float64       `json:"cost_weight"`        // DPP 控制参数
	ScalingCostFixed    float64       `json:"scaling_cost_fixed"` // 成本相关
	ScalingCostVariable float64       `json:"scaling_cost_variable"`
	ScalingRatio        float64       `json:"scaling_ratio"`
	BaseRetentionTime   time.Duration `json:"base_retention_time"` // 保留机制
	RetentionAmplifier  float64       `json:"retention_amplifier"`
	RetentionDecay      time.Duration `json:"retention_decay"`
	PermanentThreshold  time.Duration `json:"permanent_threshold"`
	PermanentDuration   time.Duration `json:"permanent_duration"`
	TickerInterval      time.Duration `json:"ticker_interval"` // 定时任务
}

// NodeState 表示每个节点的弹性伸缩状态
type NodeState struct {
	ID              string           `json:"id"`
	State           NodeStatus       `json:"state"`
	Perturbation    float64          `json:"perturbation"` //perturbation
	Accumulation    float64          `json:"accumulation"` //volatility accumulation
	RetainTime      time.Time        `json:"retain_time"`
	ScaledVMs       []VM             `json:"scaled_vms"` //目前只能单个轮次转动这个状态机
	ScaleHistory    []ScaleEvent     `json:"scale_history"`
	VolatilityQueue *util.FixedQueue `json:"volatility_queue"` // 波动队列（只暴露 snapshot）
}

type ScaleEvent struct {
	Time     time.Time `json:"time"`
	Amount   int       `json:"amount"`
	ScaledVM VM        `json:"scaled_vm,omitempty"`
}

type VM struct {
	PublicIP  string    `json:"public_ip"`
	VMName    string    `json:"vm_name"`
	StartTime time.Time `json:"start_time"`
	//Status    NodeStatus `json:"status"`
}

type ScalerOverride struct {
	Now        *time.Time  `json:"now"`
	Delta      *float64    `json:"delta"`
	State      *NodeStatus `json:"state"`
	RetainTime *time.Time  `json:"retain_time"`
}

type Scaler struct {
	Interface    VMScalingInterfaces
	Config       *ScaleConfig    `json:"config"`             // 配置
	Node         *NodeState      `json:"node"`               // 单节点状态
	Override     *ScalerOverride `json:"override,omitempty"` // ====== 测试 / 模拟用 ======
	ManualAction string
	stopChan     chan struct{} // 定时任务停止通道
	tryMu        TryMutex      // 读写锁，保护 node 状态并发访问
	logger       *slog.Logger  //日志
}

type TryMutex struct {
	locked int32
	mu     sync.Mutex
}

func (m *TryMutex) TryLock() bool {
	if !atomic.CompareAndSwapInt32(&m.locked, 0, 1) {
		// 已经被锁住，直接返回 false
		return false
	}
	m.mu.Lock()
	return true
}

func (m *TryMutex) Unlock() {
	atomic.StoreInt32(&m.locked, 0)
	m.mu.Unlock()
}

// NewDefaultScaleConfig 返回带默认值的 ScaleConfig
func NewDefaultScaleConfig() *ScaleConfig {
	return &ScaleConfig{
		VolatilityWeight:    1.0, // 队列与波动相关
		QueueWeight:         1.0,
		DecayFactor:         0.8,
		VolatilityThreshold: 0.3,  // 小波动忽略
		CostWeight:          1.0,  // 默认成本敏感度	// DPP 控制参数
		ScalingCostFixed:    10.0, // 举例// 成本相关
		ScalingCostVariable: 1.0,
		ScalingRatio:        0.1,             // 默认扩容 1 台
		BaseRetentionTime:   5 * time.Minute, // 保留机制
		RetentionAmplifier:  1.0,
		RetentionDecay:      10 * time.Minute,
		PermanentThreshold:  1 * time.Hour,
		PermanentDuration:   1 * time.Hour,
		TickerInterval:      30 * time.Second, // 定时任务
	}
}

// NewNodeState 初始化单个节点状态
func NewNodeState(id string, queue *util.FixedQueue) *NodeState {
	return &NodeState{
		ID:              id,
		Perturbation:    0, // 当前扰动 perturbation
		Accumulation:    0, // volatility accumulation
		State:           StateInactive,
		RetainTime:      time.Time{}, // 保持时间
		ScaledVMs:       []VM{},      //目前只能单个轮次转动这个状态机
		ScaleHistory:    []ScaleEvent{},
		VolatilityQueue: queue,
	}
}

// NewScaler 初始化 Scaler 控制器
func NewScaler(nodeID string, config *ScaleConfig, queue *util.FixedQueue, pre string, logger *slog.Logger) *Scaler {

	logger.Info("NewScaler", slog.String("pre", pre), slog.String("nodeID", nodeID), slog.Any("config", config))
	if config == nil {
		config = NewDefaultScaleConfig()
	}

	si := InitInterface(util.Config_.Node.Provider, util.Config_.Scaling.ScalingConfig, pre, logger)
	return &Scaler{
		Interface:    si,
		Config:       config,
		Node:         NewNodeState(nodeID, queue),
		stopChan:     make(chan struct{}),
		Override:     nil,
		ManualAction: ActionInit,
		logger:       logger,
	}
}

func (s *Scaler) scalerDump(pre string, logger *slog.Logger) {
	logger.Info("Scalar dump", slog.String("pre", pre), slog.Any("config", s.Config),
		slog.Any("node", s.Node), slog.Any("override", s.Override))
}

func (s *Scaler) now() time.Time {
	now := time.Now()
	if s.Override != nil && s.Override.Now != nil {
		now = *s.Override.Now
	}
	return now
}

func (s *Scaler) getRetainTime() time.Time {
	if s.Override != nil && s.Override.RetainTime != nil {
		return *s.Override.RetainTime
	}
	return s.Node.RetainTime
}

func (s *Scaler) getState() NodeStatus {
	if s.Override != nil && s.Override.State != nil {
		return *s.Override.State
	}
	return s.Node.State
}

const (
	Vultr = "vultr"
	GCP   = "gcp"
	AWS   = "aws"
)

type OperateInterface interface {
	CreateVM(ctx context.Context, vmName string, pre string, logger *slog.Logger) (string, error)
	GetVMPublicIP(ctx context.Context, vmName string, pre string, logger *slog.Logger) (string, error)
	DeleteVM(ctx context.Context, vmName string, pre string, logger *slog.Logger) error
}

type VMScalingInterfaces struct {
	Operate OperateInterface
}

func InitInterface(provider, config string, pre string, logger *slog.Logger) VMScalingInterfaces {

	var vs VMScalingInterfaces
	vs.Operate = nil

	switch provider {
	case GCP:
		gcpCfg, err := gcp.ExtractGCPFromInterface(config)
		if err != nil {
			logger.Error("ExtractGCPFromInterface failed", slog.String("pre", pre), slog.Any("err", err))
			return vs
		}
		vs.Operate = gcp.NewScalingOperate(gcpCfg, util.Config_.Scaling.SshKey, pre, logger)
	case Vultr:
		vultrCfg, err := vultr.ExtractVultrFromInterface(config)
		if err != nil {
			logger.Error("ExtractVultrFromInterface failed", slog.String("pre", pre), slog.Any("err", err))
			return vs
		}
		vs.Operate = vultr.NewScalingOperate(vultrCfg, util.Config_.Scaling.SshKey, pre, logger)
	case AWS:
		awsCfg, err := aws.ExtractAWSFromInterface(config)
		if err != nil {
			logger.Error("ExtractAWSFromInterface failed", slog.String("pre", pre), slog.Any("err", err))
			return vs
		}
		vs.Operate = aws.NewScalingOperate(awsCfg, util.Config_.Scaling.SshKey, pre, logger)
	default:
		logger.Error("Unsupported provider", slog.String("pre", pre), slog.String("provider", provider))
	}

	return vs
}
