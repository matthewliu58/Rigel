package scaling_vm

import (
	"bytes"
	"context"
	"control-plane/pkg/envoy_manager"
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

//维护vm信息;扩容&缩容&安装环境以及二进制;开启&关闭健康检查; 这些都是elastic scaling的approach

// StartTicker 启动定时任务
func (s *Scaler) StartAutoScalingTicker(pre string) {
	s.logger.Info("start ticker", "pre", pre)
	ticker := time.NewTicker(s.Config.TickerInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.autoScaling()
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

	//还没有足够数据
	if len(queue) <= 1 {
		s.logger.Info("the data of volatility queue is spare", slog.String("pre", pre))
		return 0
	}

	//如果最新的波动小于阈值 ，则直接返回 0
	avgCache := queue[0].(storage.NetworkTelemetry).NodeCongestion.AvgWeightedCache
	avgCache_ := queue[1].(storage.NetworkTelemetry).NodeCongestion.AvgWeightedCache

	if (avgCache <= s.Config.VolatilityThreshold && avgCache_ <= s.Config.VolatilityThreshold) ||
		(avgCache >= s.Config.VolatilityThreshold && avgCache_ >= s.Config.VolatilityThreshold) {

		s.logger.Info("latest volatility is insignificant",
			slog.String("pre", pre),
			slog.Float64("avg_cache_curr", avgCache),
			slog.Float64("avg_cache_prev", avgCache_),
			slog.Float64("threshold", s.Config.VolatilityThreshold),
		)
		return 0
	}

	p := math.Abs(avgCache - avgCache_)
	return p

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

	// 1️⃣ 当前扰动量
	P := node.P
	Z := node.Z

	// 2️⃣ 成本，根据节点当前状态
	cost := s.calculateCost(node)

	// 3️⃣ 公式
	delta := -s.Config.DecayFactor*s.Config.VolatilityWeight*s.Config.QueueWeight*Z*P +
		s.Config.CostWeight*cost

	return delta
}

// calculateCost 按论文公式计算成本
func (s *Scaler) calculateCost(node *NodeState) float64 {
	switch node.State {
	case Inactive:
		return s.Config.ScalingCostFixed + s.Config.ScalingCostVariable*node.P
	case Dormant:
		return s.Config.ScalingCostVariable * node.P
	default:
		return 0
	}
}

// evaluateScaling 核心扩容判断逻辑
// evaluateScaling 核心扩容判断与状态管理逻辑
func (s *Scaler) autoScaling() {

	pre := util.GenerateRandomLetters(5)

	// 尝试获取锁，若获取不到则直接返回
	if !s.tryMu.TryLock() {
		s.logger.Warn("cannot get lock", slog.String("pre", pre),
			slog.Any("err", "cannot get lock"),
		)
		return
	}
	defer s.tryMu.Unlock()

	s.logger.Info("autoScaling", slog.String("pre", pre))

	if s.ManualAction != "init" {
		s.logger.Info("in the manual mode", slog.String("pre", pre),
			slog.String("action", s.ManualAction))
		return
	}

	//-----------------------------------------------------------------------------------------------------------------/

	// 1️⃣ 判断当前节点状态，如果不需要扩容则直接返回
	s.ScalerDump(pre+"-1", s.logger) //打印 node
	node := s.Node
	switch s.getState() {
	case ScalingUp:
		s.logger.Info("node is scaling up", slog.String("pre", pre))
		return
	case Releasing:
		s.logger.Info("node is releasing", slog.String("pre", pre))
		return
	case Triggered:
		if s.now().Before(s.getRetainTime()) {
			s.logger.Info("node is triggered, but retention time not reached",
				slog.String("pre", pre))
			return
		}
		//往下走就是已经超时
	case Dormant, Permanent:
		if s.now().Before(s.getRetainTime()) {
			s.logger.Info("node is dormant or permanent, but retention time not reached",
				slog.String("pre", pre))
			// 后面检验一下是不是需要扩容 如果扩容这个状态就会被change
		} else {
			s.logger.Info("node is dormant or permanent, and retention time reached",
				slog.String("pre", pre))
			//如果后面不触发 Triggered 走到最后就会被删除
			node.State = Releasing
		}
	case Inactive:
		s.logger.Info("node is inactive", slog.String("pre", pre))
	default:
		s.logger.Warn("unhandled default case", slog.String("pre", pre))
	}

	//-----------------------------------------------------------------------------------------------------------------

	// 1️⃣ 计算当前扰动量 P 和波动值 Z
	node.P = s.calculatePerturbation(pre)
	node.Z = s.calculateVolatilityAccumulation()
	delta := s.calculateDelta(s.Node)
	s.ScalerDump(pre+"-2", s.logger)
	s.logger.Info("calculate delta", slog.String("pre", pre), slog.Float64("delta", delta))
	// 2️⃣ 判断是否需要触发扩容
	if delta < 0 {
		switch s.getState() {
		case Inactive:
			node.State = ScalingUp
			ok, vm := s.triggerScaling1_(1, VM{}, pre, s.logger)
			if vm.PublicIP != "" {
				node.ScaledVMs = append(node.ScaledVMs, vm)
			}
			if ok {
				node.State = Triggered
				node.ScaleHistory = append(node.ScaleHistory, ScaleEvent{Time: s.now(), Amount: 1, ScaledVM: vm})
				//node.ScaledVMs = append(node.ScaledVMs, vm)
				retain, state := s.calculateRetention(pre)
				node.RetainTime = retain
				if state == Permanent {
					node.State = Permanent
				}
			} else {
				s.logger.Error("triggerScaling1 failed", slog.String("pre", pre))
			}
		case Dormant:
			node.State = Triggered
			if s.triggerScaling2(VM{}, pre) {
				node.State = Triggered
				node.ScaleHistory = append(node.ScaleHistory, ScaleEvent{Time: s.now(), Amount: 1})
				retain, state := s.calculateRetention(pre)
				node.RetainTime = retain
				if state == Permanent {
					node.State = Permanent
				}
			} else {
				s.logger.Error("triggerScaling2 failed", slog.String("pre", pre))
			}
		default:
			s.logger.Warn("unhandled default case", slog.String("pre", pre))
		}
	}
	s.ScalerDump(pre+"-3", s.logger) //打印 node
	if node.State == ScalingUp || node.State == Triggered || node.State == Permanent {
		return
	}

	//-----------------------------------------------------------------------------------------------------------------/

	// 3️⃣ 如果没有触发扩容，根据当前状态处理
	switch s.getState() {
	case Dormant, Permanent:
		s.logger.Info("node is dormant or permanent, and retention time reached", slog.String("pre", pre))
		node.State = Releasing
		s.triggerRelease(VM{}, pre)
		node.State = Inactive
	case ScalingUp:
		retain, _ := s.calculateRetention(pre)
		node.RetainTime = retain
		node.State = Dormant
		s.triggerDormant(VM{}, pre)
		s.logger.Info("the state of node is changed to dormant from scaling up", slog.String("pre", pre))
	default:
		s.logger.Warn("unhandled default case", slog.String("pre", pre))
	}
	s.ScalerDump(pre+"-4", s.logger) //打印 node

	return
}

// triggerScaling 模拟扩容动作
//func (s *Scaler) triggerScaling1(n int, pre string, logger *slog.Logger) (bool, VM) {
//
//	logger.Info("triggerScaling1", slog.String("pre", pre), "n", n)
//
//	//获取本节点配置信息
//	//扩容
//	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
//	defer cancel() // 确保上下文最终被释放
//
//	gcp := util.Config_.GCP
//	vmName := gcp.VMPrefix + util.GenerateRandomLetters1(5)
//	err := CreateVM(ctx, gcp.ProjectID, gcp.Zone, vmName, gcp.CredFile, pre, logger)
//
//	if err != nil {
//		logger.Error("创建 VM 失败", slog.String("pre", pre), "error", err)
//		return false, VM{}
//	}
//
//	// 在创建虚拟机后等待一定时间，确保 VM 启动完成
//	logger.Info("Waiting for VM to start...", slog.String("pre", pre), "vmName", vmName)
//	time.Sleep(3 * time.Minute) // 等待 10 分钟
//
//	//获取ip等信息用于管理
//	ip, err := GetVMExternalIP(ctx, logger, gcp.ProjectID, gcp.Zone, vmName, gcp.CredFile, pre)
//	if err != nil {
//		logger.Error("获取 VM 外部 IP 失败", slog.String("pre", pre), "error", err)
//		return false, VM{}
//	}
//
//	logger.Info("Scaling node", slog.String("pre", pre), slog.String("zone", gcp.Zone),
//		slog.String("vm name", vmName), slog.String("ip", ip))
//
//	//安装环境 启动 触发envoy
//	err = deployBinaryToServer(username, ip, "22", localPathProxy, remotePathProxy, binaryProxy, logger)
//	if err != nil {
//		logger.Error("部署二进制文件失败", slog.String("pre", pre),
//			slog.String("remote proxy", remotePathProxy), "error", err)
//		return false, VM{}
//	}
//	err = deployBinaryToServer(username, ip, "22", localPathPlane, remotePathPlane, binaryPlane, logger)
//	if err != nil {
//		logger.Error("部署二进制文件失败", slog.String("pre", pre),
//			slog.String("remote proxy", remotePathProxy), "error", err)
//		return false, VM{}
//	}
//
//	//关联到envoy
//	_, err = sendAddTargetIpsRequest([]envoy_manager.EnvoyTargetAddr{envoy_manager.EnvoyTargetAddr{ip, 8095}})
//	if err != nil {
//		logger.Error("关联到envoy失败", "error", err)
//		return false, VM{}
//	}
//
//	return true, VM{ip, vmName, s.now(), Triggered}
//}

func (s *Scaler) triggerScaling2(vm_ VM, pre string) bool {

	s.logger.Info("triggerScaling2", slog.String("pre", pre), slog.Any("vm", vm_))

	var ip, setState string

	if vm_.PublicIP != "" {
		ip = vm_.PublicIP
	} else {
		//找到睡眠的vm获取 ip
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm := s.Node.ScaledVMs[0]
		ip = vm.PublicIP
	}

	setState = "on"

	if b := setHealthState(ip, setState, pre, s.logger); b == false {
		return false
	}
	return true
}

func (s *Scaler) triggerDormant(vm_ VM, pre string) bool {

	s.logger.Info("triggerDormant", slog.String("pre", pre), slog.Any("vm", vm_))

	var ip, setState string

	if vm_.PublicIP != "" {
		ip = vm_.PublicIP
	} else {
		//找到睡眠的vm获取 ip
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm := s.Node.ScaledVMs[0]
		ip = vm.PublicIP
	}

	setState = "off"

	if b := setHealthState(ip, setState, pre, s.logger); b == false {
		return false
	}
	return true
}

// triggerRelease 模拟释放动作
func (s *Scaler) triggerRelease(vm_ VM, pre string) bool {

	s.logger.Info("triggerRelease", slog.String("pre", pre), slog.Any("vm", vm_))

	var vm VM
	if vm_.PublicIP != "" {
		vm = vm_
	} else {
		//找到睡眠的vm获取 ip
		if len(s.Node.ScaledVMs) <= 0 {
			s.logger.Error("No scaled VMs found", slog.String("pre", pre))
			return false
		}
		vm = s.Node.ScaledVMs[0]
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // 确保上下文最终被释放

	gcp := util.Config_.GCP
	err := DeleteVM(ctx, logger, gcp.ProjectID, gcp.Zone, vm.VMName, gcp.CredFile, pre)
	if err != nil {
		s.logger.Error("删除 VM 失败", slog.String("pre", pre), "error", err)
	}

	//update envoy 配置
	if _, err := sendAddTargetIpsRequest(
		[]envoy_manager.EnvoyTargetAddr{{vm.PublicIP, 8095}},
		"del", pre, logger); err != nil {
		s.logger.Error("sendAddTargetIpsRequest failed", slog.String("pre", pre),
			slog.Any("vm", vm_), slog.Any("err", err))
	} else {
		s.logger.Info("sendAddTargetIpsRequest success", slog.String("pre", pre),
			slog.Any("vm", vm_))
	}

	s.logger.Info("Releasing node", slog.String("pre", pre), slog.String("vm name", vm.VMName))
	return true
}

// calculateRetention 计算节点的 Retain Time，返回绝对时间点
func (s *Scaler) calculateRetention(pre string) (time.Time, NodeStatus) {
	now := s.now()
	var activationPotential float64

	for _, evt := range s.Node.ScaleHistory {
		// 只考虑 tau 内的触发事件
		delta := now.Sub(evt.Time)
		if delta > s.Config.RetentionDecay {
			continue
		}
		activationPotential += float64(evt.Amount) * expDecay(delta, s.Config.RetentionDecay)
	}

	// 计算 Retention 时间长度
	retentionDuration := s.Config.BaseRetentionTime + time.Duration(s.Config.RetentionAmplifier*activationPotential)

	// 如果超过永久阈值，直接返回永久时间
	if retentionDuration >= s.Config.PermanentThreshold {
		return now.Add(s.Config.PermanentDuration), Permanent
	}

	// 返回节点保持活跃的绝对时间点
	return now.Add(retentionDuration), End
}

// 指数衰减函数
func expDecay(delta time.Duration, tau time.Duration) float64 {
	return math.Exp(-float64(delta) / float64(tau))
}

// setHealthState 用于向 API 发送请求，设置健康状态
// 参数 apiHost 是主机地址，setState 是健康状态（可以是 "on" 或 "off"）
func setHealthState(apiHost, setState, pre string, logger *slog.Logger) bool {
	// 创建 URL 和查询参数
	apiURL := fmt.Sprintf("http://%s:8095/healthStateChange", apiHost) // 使用传入的 apiHost
	params := url.Values{}
	params.Add("set", setState)

	// 构建完整的请求 URL
	reqURL := fmt.Sprintf("%s?%s", apiURL, params.Encode())

	// 调用 API 并设置健康状态
	resp, err := http.Get(reqURL)
	if err != nil {
		logger.Error("请求失败", slog.String("pre", pre), slog.Any("err", err))
		return false
	}
	defer resp.Body.Close()

	// 输出响应状态
	logger.Info("响应状态码", slog.String("pre", pre), slog.Int64("status code", int64(resp.StatusCode)))

	// 根据响应状态码处理结果
	if resp.StatusCode == http.StatusOK {
		logger.Info("健康状态已成功设置为", slog.String("pre", pre), slog.String("set state", setState))
	} else {
		logger.Error("健康状态设置失败状态码", slog.String("pre", pre),
			slog.Int64("status code", int64(resp.StatusCode)))
		return false
	}
	return true
}

// sendRequest 向指定的 API 路由发送请求
func sendAddTargetIpsRequest(targetIps []envoy_manager.EnvoyTargetAddr,
	action, pre string, logger *slog.Logger) (*envoy_manager.APICommonResp, error) {
	// 构建请求体
	url := "http://127.0.0.1:8081/envoy/cfg/setTargetIps"
	body, err := json.Marshal(targetIps)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	logger.Info("sendAddTargetIpsRequest", slog.String("pre", pre),
		slog.String("action", action), slog.Any("addr", targetIps))

	// 构建请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// 设置 Content-Type 和自定义 Header
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Action", action) // <- 这里加上 action

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// 解析响应体
	var response envoy_manager.APICommonResp
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &response, nil
}

func (s *Scaler) createVM(
	ctx context.Context,
	pre string,
	logger *slog.Logger,
) (VM, error) {

	gcp := util.Config_.GCP
	vmName := gcp.VMPrefix + util.GenerateRandomLetters1(5)

	logger.Info("creating VM",
		slog.String("pre", pre),
		slog.String("vmName", vmName),
		slog.String("zone", gcp.Zone),
	)

	if err := CreateVM(
		ctx,
		gcp.ProjectID,
		gcp.Zone,
		vmName,
		gcp.CredFile,
		pre,
		logger,
	); err != nil {
		return VM{}, err
	}

	logger.Info("waiting for VM startup",
		slog.String("pre", pre),
		slog.String("vmName", vmName),
	)

	time.Sleep(2 * time.Minute)

	ip, err := GetVMExternalIP(
		ctx,
		logger,
		gcp.ProjectID,
		gcp.Zone,
		vmName,
		gcp.CredFile,
		pre,
	)
	if err != nil {
		return VM{}, err
	}

	return VM{ip, vmName, s.now()}, nil
}

func (s *Scaler) deployAndAttachVM(
	vm VM,
	pre string,
	logger *slog.Logger,
) error {

	b, _ := json.Marshal(vm)
	logger.Info("deploying binaries to VM",
		slog.String("pre", pre),
		slog.String("vm", string(b)),
	)

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
		s.logger.Error("deployAndAttachVM failed", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.String("vm", string(b)), slog.Any("err", err))
		return err
	} else {
		s.logger.Info("deployAndAttachVM success", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.String("vm", string(b)))
	}

	//确保proxy启动完
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
		s.logger.Error("deployAndAttachVM failed", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.String("vm", string(b)), slog.Any("err", err))
		return err
	} else {
		s.logger.Info("deployAndAttachVM success", slog.String("pre", pre),
			slog.String("binaryPlane", binaryPlane), slog.String("vm", string(b)))
	}

	if _, err := sendAddTargetIpsRequest(
		[]envoy_manager.EnvoyTargetAddr{{vm.PublicIP, 8095}},
		"add", pre, logger); err != nil {
		s.logger.Error("sendAddTargetIpsRequest failed", slog.String("pre", pre),
			slog.String("vm", string(b)), slog.Any("err", err))
		return err
	} else {
		s.logger.Info("sendAddTargetIpsRequest success", slog.String("pre", pre),
			slog.String("vm", string(b)))
	}
	return nil
}

func (s *Scaler) triggerScaling1_(n int, vm_ VM, pre string, logger *slog.Logger) (bool, VM) {

	logger.Info("triggerScaling1_", slog.String("pre", pre), "n", n, slog.Any("vm", vm_))

	vm := VM{}
	var err error

	if vm_.PublicIP != "" {
		s.logger.Info("specific vm action", slog.String("pre", pre))
		vm = vm_
	} else {
		if len(s.Node.ScaledVMs) == 0 {
			s.logger.Info("crete new vm", slog.String("pre", pre))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel() // 确保上下文最终被释放
			vm, err = s.createVM(ctx, pre, logger)
			if err != nil {
				s.logger.Error("createVM failed", slog.String("pre", pre), slog.Any("err", err))
				return false, VM{}
			}
		} else {
			vm = s.Node.ScaledVMs[0]
			s.logger.Info("already exist vm", slog.String("pre", pre))
		}
	}

	b, _ := json.Marshal(vm)
	logger.Info("createVM success", slog.String("pre", pre), slog.String("vm", string(b)))

	err = s.deployAndAttachVM(vm, pre, logger)
	if err != nil {
		s.logger.Error("deployAndAttachVM failed", slog.String("pre", pre), slog.Any("err", err))
		return false, vm
	}

	s.logger.Info("deployAndAttachVM success", slog.String("pre", pre), slog.String("vm", string(b)))

	return true, vm
}
