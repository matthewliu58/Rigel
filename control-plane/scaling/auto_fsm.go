package scaling

import (
	"bytes"
	"context"
	em "control-plane/pkg/envoy_manager"
	"control-plane/storage"
	"control-plane/util"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"time"
)

func (s *Scaler) StartAutoScalingTicker(pre string) {

	s.logger.Info("StartAutoScalingTicker", slog.String("pre", pre))

	ticker := time.NewTicker(s.Config.TickerInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.AutoScaling()
			case <-s.stopChan:
				ticker.Stop()
				return
			}
		}
	}()
}

// StopTicker 停止定时任务
func (s *Scaler) StopTicker() {
	close(s.stopChan)
}

// 计算当前扰动量 \widetilde P_i(t)
func (s *Scaler) calculatePerturbation(pre string) float64 {

	var queue []interface{}

	queue = s.Node.VolatilityQueue.SnapshotLatestFirst()
	if len(queue) <= 1 {
		s.logger.Info("The data of volatility queue is spare", slog.String("pre", pre))
		return 0
	}

	avgCache := queue[0].(storage.NetworkTelemetry).NodeCongestion.AvgWeightedCache
	avgCache_ := queue[1].(storage.NetworkTelemetry).NodeCongestion.AvgWeightedCache
	threshold := s.Config.VolatilityThreshold

	if (avgCache <= threshold && avgCache_ <= threshold) ||
		(avgCache >= threshold && avgCache_ >= threshold) {

		s.logger.Info("Latest volatility is insignificant",
			slog.String("pre", pre), slog.Float64("avg_cache_curr", avgCache),
			slog.Float64("avg_cache_prev", avgCache_), slog.Float64("threshold", threshold))
		return 0
	}

	return math.Abs(avgCache - avgCache_)

}

func (s *Scaler) calculateVolatilityAccumulation() float64 {

	z := s.Node.P*s.Config.VolatilityWeight + s.Node.Z*s.Config.DecayFactor

	if z < 0 {
		return 0
	}
	return z
}

func (s *Scaler) calculateDelta(node *NodeState) float64 {

	if s.Override != nil && s.Override.Delta != nil {
		return *s.Override.Delta
	}

	P := node.P
	Z := node.Z

	cost := s.calculateCost(node)

	delta := -s.Config.DecayFactor*s.Config.VolatilityWeight*s.Config.QueueWeight*Z*P + s.Config.CostWeight*cost
	return delta
}

// calculateCost 按论文公式计算成本
func (s *Scaler) calculateCost(node *NodeState) float64 {
	switch node.State {
	case StateInactive:
		return s.Config.ScalingCostFixed + s.Config.ScalingCostVariable*node.P
	case StateDormant:
		return s.Config.ScalingCostVariable * node.P
	default:
		return 0
	}
}

func (s *Scaler) AutoScaling() {

	pre := util.GenerateRandomLetters(5)
	s.logger.Info("AutoScaling", slog.String("pre", pre))

	if !s.tryMu.TryLock() {
		s.logger.Warn("Cannot get lock", slog.String("pre", pre))
		return
	}
	defer s.tryMu.Unlock()

	//if s.ManualAction != ActionInit {
	//	s.logger.Info("In the manual mode", slog.String("pre", pre), slog.String("action", s.ManualAction))
	//	return
	//}

	//1. 检查当前状态
	s.scalerDump(pre+"-before-check-state", s.logger)

	node := s.Node
	switch s.getState() {
	case StateScalingUp:
		s.logger.Info("Node is scaling up", slog.String("pre", pre))
		return
	case StateReleasing:
		s.logger.Info("Node is releasing", slog.String("pre", pre))
		return
	case StateTriggered:
		if s.now().Before(s.getRetainTime()) {
			s.logger.Info("Node is triggered, retention is available",
				slog.String("pre", pre))
			return
		}
	case StateDormant, StatePermanent:
		if s.now().Before(s.getRetainTime()) {
			s.logger.Info("Node is dormant or permanent, retention is available",
				slog.String("pre", pre))
		} else {
			s.logger.Info("Node is dormant or permanent, retention is not available",
				slog.String("pre", pre))
			node.State = StateReleasing
		}
	case StateInactive:
		s.logger.Info("Node is inactive", slog.String("pre", pre))
	default:
		s.logger.Warn("Unhandled default case", slog.String("pre", pre))
	}

	// 2. 计算当前扰动量 P 和波动值 Z and delta
	node.P = s.calculatePerturbation(pre)
	node.Z = s.calculateVolatilityAccumulation()
	delta := s.calculateDelta(s.Node)

	s.scalerDump(pre+"-after-calculate-delta", s.logger)
	s.logger.Info("Calculate delta", slog.String("pre", pre), slog.Float64("delta", delta))

	// 3. scaling
	if delta < 0 {
		switch s.getState() {
		case StateInactive:
			node.State = StateScalingUp
			ok, vm := s.triggerScalingFromInit(1, VM{}, pre, s.logger)
			if vm.PublicIP != "" {
				node.ScaledVMs = append(node.ScaledVMs, vm)
			}
			if ok {
				node.State = StateTriggered
				node.ScaleHistory = append(node.ScaleHistory, ScaleEvent{Time: s.now(), Amount: 1, ScaledVM: vm})
				retain, state := s.calculateRetention(pre)
				node.RetainTime = retain
				if state == StatePermanent {
					node.State = StatePermanent
				}
			} else {
				s.logger.Error("TriggerScalingFromInit failed", slog.String("pre", pre))
			}
		case StateDormant:
			node.State = StateTriggered
			if s.triggerScalingFromDormant(VM{}, pre) {
				node.State = StateTriggered
				node.ScaleHistory = append(node.ScaleHistory, ScaleEvent{Time: s.now(), Amount: 1})
				retain, state := s.calculateRetention(pre)
				node.RetainTime = retain
				if state == StatePermanent {
					node.State = StatePermanent
				}
			} else {
				s.logger.Error("TriggerScalingFromDormant failed", slog.String("pre", pre))
			}
		default:
			s.logger.Warn("Unhandled default case", slog.String("pre", pre))
		}
	}

	s.scalerDump(pre+"-after-scaling", s.logger)
	if node.State == StateScalingUp || node.State == StateTriggered || node.State == StatePermanent {
		return
	}

	// 如果没有触发扩容，根据当前状态处理
	switch s.getState() {
	case StateDormant, StatePermanent:
		s.logger.Info("Node is dormant or permanent, retention is not available", slog.String("pre", pre))
		node.State = StateReleasing
		s.triggerRelease(VM{}, pre)
		node.State = StateInactive
	case StateScalingUp:
		retain, _ := s.calculateRetention(pre)
		node.RetainTime = retain
		node.State = StateDormant
		s.triggerDormant(VM{}, pre)
		s.logger.Info("The state of node is changed to dormant from scaling up", slog.String("pre", pre))
	default:
		s.logger.Warn("Unhandled default case", slog.String("pre", pre))
	}

	s.scalerDump(pre+"-before-end", s.logger)
}

func (s *Scaler) triggerScalingFromDormant(vm_ VM, pre string) bool {

	s.logger.Info("TriggerScalingFromDormant", slog.String("pre", pre), slog.Any("VM", vm_))

	var ip string

	if vm_.PublicIP != "" {
		ip = vm_.PublicIP
	} else {
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm := s.Node.ScaledVMs[0]
		ip = vm.PublicIP
	}

	setState := "on"
	if b := setHealthState(ip, setState, pre, s.logger); b == false {
		return false
	}
	return true
}

func (s *Scaler) triggerDormant(vm_ VM, pre string) bool {

	s.logger.Info("TriggerDormant", slog.String("pre", pre), slog.Any("VM", vm_))

	var ip string

	if vm_.PublicIP != "" {
		ip = vm_.PublicIP
	} else {
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm := s.Node.ScaledVMs[0]
		ip = vm.PublicIP
	}

	setState := "off"
	if b := setHealthState(ip, setState, pre, s.logger); b == false {
		return false
	}
	return true
}

func (s *Scaler) triggerRelease(vm_ VM, pre string) bool {

	s.logger.Info("TriggerRelease", slog.String("pre", pre), slog.Any("VM", vm_))

	var vm VM
	if vm_.PublicIP != "" {
		vm = vm_
	} else {
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm = s.Node.ScaledVMs[0]
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	err := s.Interface.Operate.DeleteVM(ctx, vm.VMName, pre, logger)
	if err != nil {
		s.logger.Error("DeleteVM failed", slog.String("pre", pre), slog.Any("err", err))
	}

	//update envoy 配置
	if _, err := sendAddTargetIpsRequest([]em.EnvoyTargetAddr{{IP: vm.PublicIP, Port: 8095}},
		ActionDelVM, pre, logger); err != nil {

		s.logger.Error("SendAddTargetIpsRequest failed", slog.String("pre", pre),
			slog.Any("VM", vm_), slog.Any("err", err))
	} else {

		s.logger.Info("SendAddTargetIpsRequest success", slog.String("pre", pre),
			slog.Any("VM", vm_))
	}

	s.logger.Info("Releasing node", slog.String("pre", pre), slog.String("VM name", vm.VMName))
	return true
}

// calculateRetention 计算节点的 Retain Time，返回绝对时间点
func (s *Scaler) calculateRetention(pre string) (time.Time, NodeStatus) {

	now := s.now()
	var activePotent float64
	retDec := s.Config.RetentionDecay
	validIdx := 0

	for _, evt := range s.Node.ScaleHistory {
		delta := now.Sub(evt.Time)
		if delta > retDec {
			continue
		}
		s.Node.ScaleHistory[validIdx] = evt
		validIdx++
		activePotent += float64(evt.Amount) * expDecay(delta, retDec)
	}
	s.Node.ScaleHistory = s.Node.ScaleHistory[:validIdx]

	baseRe := s.Config.BaseRetentionTime
	reAmpl := s.Config.RetentionAmplifier
	perThr := s.Config.PermanentThreshold

	// 计算 Retention 时间长度
	retDur := baseRe + time.Duration(reAmpl*activePotent)
	s.logger.Info("CalculateRetention", slog.String("pre", pre), slog.Any("retDur", retDur))

	// 如果超过永久阈值，直接返回永久时间
	if retDur >= perThr {
		return now.Add(s.Config.PermanentDuration), StatePermanent
	}

	return now.Add(retDur), StateEnd
}

// 指数衰减函数
func expDecay(delta time.Duration, tau time.Duration) float64 {
	return math.Exp(-float64(delta) / float64(tau))
}

func setHealthState(apiHost, setState, pre string, logger *slog.Logger) bool {

	apiURL := fmt.Sprintf("http://%s:8095/healthStateChange", apiHost) // 使用传入的 apiHost
	params := url.Values{}
	params.Add("set", setState)

	reqURL := fmt.Sprintf("%s?%s", apiURL, params.Encode())
	resp, err := http.Get(reqURL)
	if err != nil {
		logger.Error("Request failed", slog.String("pre", pre),
			slog.String("url", reqURL), slog.Any("err", err))
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		logger.Info("HealthStateChange success", slog.String("pre", pre),
			slog.String("set state", setState))
	} else {
		logger.Error("HealthStateChange failed", slog.String("pre", pre),
			slog.Int64("status code", int64(resp.StatusCode)))
		return false
	}
	return true
}

// sendRequest 向指定的 API 路由发送请求
func sendAddTargetIpsRequest(targetIps []em.EnvoyTargetAddr,
	action, pre string, logger *slog.Logger) (*em.APICommonResp, error) {

	url := "http://127.0.0.1:8081/envoy/cfg/setTargetIps"
	body, err := json.Marshal(targetIps)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	logger.Info("SendAddTargetIpsRequest", slog.String("pre", pre),
		slog.String("action", action), slog.Any("addr", targetIps))

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Action", action)

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	var response em.APICommonResp
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &response, nil
}

func (s *Scaler) createVM(ctx context.Context, pre string, logger *slog.Logger) (VM, error) {

	vmName := util.Config_.Node.Provider + "-" + util.GenerateRandomLetters_(5)
	logger.Info("Creating VM", slog.String("pre", pre), slog.String("VM", vmName))

	// 1 creating
	if err := s.Interface.Operate.CreateVM(ctx, vmName, pre, logger); err != nil {
		return VM{}, err
	}

	logger.Info("Waiting for VM startup & public IP", slog.String("pre", pre), slog.String("VM", vmName))

	const (
		totalTimeout = 2 * time.Minute
		pollInterval = 10 * time.Second
	)
	var ip string
	var err error
	timeout := time.After(totalTimeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	//2 checking
	for {
		select {
		case <-ctx.Done():
			return VM{}, fmt.Errorf("CreateVM context canceled: %w", ctx.Err())
		case <-timeout:
			return VM{}, fmt.Errorf("Timeout waiting for VM %s public IP after %v", vmName, totalTimeout)
		case <-ticker.C:
			ip, err = s.Interface.Operate.GetVMPublicIP(ctx, vmName, pre, logger)
			if err == nil && ip != "" {
				logger.Info("Got VM public IP successfully",
					slog.String("pre", pre), slog.String("vmName", vmName), slog.String("ip", ip))
				goto END
			}
			logger.Info("VM IP not ready yet, retrying...",
				slog.String("pre", pre), slog.String("vmName", vmName))
		}
	}
END:
	return VM{ip, vmName, s.now()}, nil
}

func (s *Scaler) deployAndAttachVM(vm VM, pre string, logger *slog.Logger) error {

	logger.Info("Deploying binaries to VM", slog.String("pre", pre), slog.Any("vm", vm))

	if err := deployBinaryToServer(
		username,
		vm.PublicIP,
		"22",
		localPathProxy,
		remotePathProxy,
		binaryProxy,
		pre,
		logger,
	); err != nil {

		s.logger.Error("DeployAndAttachVM failed", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.Any("vm", vm), slog.Any("err", err))
		return err
	} else {

		s.logger.Info("deployAndAttachVM success", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.Any("vm", vm))
	}

	time.Sleep(2 * time.Second)

	if err := deployBinaryToServer(
		username,
		vm.PublicIP,
		"22",
		localPathPlane,
		remotePathPlane,
		binaryPlane,
		pre,
		logger,
	); err != nil {

		s.logger.Error("DeployAndAttachVM failed", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.Any("VM", vm), slog.Any("err", err))
		return err
	} else {

		s.logger.Info("DeployAndAttachVM success", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.Any("VM", vm))
	}

	if _, err := sendAddTargetIpsRequest([]em.EnvoyTargetAddr{{IP: vm.PublicIP, Port: 8095}}, ActionAddVM, pre, logger); err != nil {
		s.logger.Error("SendAddTargetIpsRequest failed", slog.String("pre", pre),
			slog.Any("VM", vm), slog.Any("err", err))
		return err
	} else {

		s.logger.Info("SendAddTargetIpsRequest success", slog.String("pre", pre),
			slog.Any("VM", vm))
	}
	return nil
}

func (s *Scaler) triggerScalingFromInit(n int, vm_ VM, pre string, logger *slog.Logger) (bool, VM) {

	logger.Info("TriggerScalingFromInit", slog.String("pre", pre), slog.Any("n", n), slog.Any("vm", vm_))

	vm := VM{}
	var err error

	if vm_.PublicIP != "" {
		s.logger.Info("Specific VM action", slog.String("pre", pre))
		vm = vm_
	} else {
		if len(s.Node.ScaledVMs) == 0 {

			s.logger.Info("Crete new VM", slog.String("pre", pre))
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			vm, err = s.createVM(ctx, pre, logger)
			if err != nil {
				s.logger.Error("Create VM failed", slog.String("pre", pre), slog.Any("err", err))
				return false, VM{}
			}
		} else {

			vm = s.Node.ScaledVMs[0]
			s.logger.Info("Already exist VM", slog.String("pre", pre))
		}
	}

	logger.Info("Create VM success", slog.String("pre", pre), slog.Any("VM", vm))

	err = s.deployAndAttachVM(vm, pre, logger)
	if err != nil {
		s.logger.Error("DeployAndAttachVM failed", slog.String("pre", pre), slog.Any("err", err))
		return false, vm
	}

	s.logger.Info("DeployAndAttachVM success", slog.String("pre", pre), slog.Any("VM", vm))
	return true, vm
}
