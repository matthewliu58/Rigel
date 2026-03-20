package storage

import (
	"control-plane/sync/etcd_client"
	"control-plane/util"
	"control-plane/vm_info"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log/slog"
	"time"
)

// VMWeightedAvgResult 封装加权缓存平均值计算结果（用于发送给Etcd的结构体）
//type NodeWeightedAvgResult struct {
//	WeightedAvg        float64   `json:"weighted_avg"`
//	TotalWeightedCache float64   `json:"total_weighted_cache"`
//	TotalActiveConns   float64   `json:"total_active_conns"`
//	VMCount            int       `json:"vm_count"`
//	CalculateTime      time.Time `json:"calculate_time"`
//
//	// 节点信息
//	PublicIP  string `json:"public_ip"`
//	Provider  string `json:"provider"`
//	Continent string `json:"continent"`
//}

// 节点拥塞指标
type NodeCongestionInfo struct {
	AvgWeightedCache   float64   `json:"avg_weighted_cache"`   // 节点加权平均拥塞指标
	TotalWeightedCache float64   `json:"total_weighted_cache"` // 节点总加权缓存
	TotalActiveConn    float64   `json:"total_active_conn"`    // 节点总活跃连接数
	VMCount            int       `json:"vm_count"`             // 节点 VM 数量
	CalculateTime      time.Time `json:"calculate_time"`       // 计算时间
}

// 链路拥塞信息
type LinkCongestionInfo struct {
	TargetIP       string            `json:"target_ip"` // 目标节点 IP
	Target         vm_info.ProbeTask `json:"target"`
	PacketLoss     float64           `json:"packet_loss"`     // 丢包率，百分比
	WeightedCache  float64           `json:"weighted_cache"`  // 链路缓存情况（可选）
	AverageLatency float64           `json:"average_latency"` // 平均延迟（毫秒）
	BandwidthUsage float64           `json:"bandwidth_usage"` // 带宽利用率（可选百分比）
}

// 节点遥测数据
type NetworkTelemetry struct {
	NodeCongestion  NodeCongestionInfo            `json:"node_congestion"`  // 节点拥塞指标
	PublicIP        string                        `json:"public_ip"`        // 节点公网 IP
	Provider        string                        `json:"provider"`         // 云厂商
	Continent       string                        `json:"continent"`        // 所属大洲
	LinksCongestion map[string]LinkCongestionInfo `json:"links_congestion"` // 节点到其他节点的链路拥塞信息
}

//1、定时器 读storage文件 汇聚group信息 到etcd 并且 加入一个全局的 queue供 elastic scaling使用

func CalcClusterWeightedAvg(fs *FileStorage, interval time.Duration,
	etcdClient *clientv3.Client, queue *util.FixedQueue, logPre string, logger *slog.Logger) {
	// 1. 内嵌定时器，直接创建
	ticker := time.NewTicker(interval)
	defer ticker.Stop() // 程序退出时回收定时器资源

	// 2. 日志输出启动信息
	logger.Info("定时加权计算启动成功", slog.String("pre", logPre),
		slog.Duration("间隔", interval), slog.String("存储目录", fs.storageDir))

	// 临时链路统计结构体，补充json tag适配JSON解析/序列化
	type tempLinksCongStruct struct {
		TargetIP         string            `json:"target_ip"`
		PacketLosses     []float64         `json:"packet_losses"`     // 若为单个丢包率则用packet_loss，数组用packet_losses
		AverageLatencies []float64         `json:"average_latencies"` // 单个延迟则用average_latency，数组用average_latencies
		ProbeTask        vm_info.ProbeTask `json:"probe_task"`
	}

	// 3. 无限循环，定时触发核心逻辑（复用GetAll()）
	for {
		// 监听定时器信号，到达间隔执行计算
		<-ticker.C

		logPre := util.GenerateRandomLetters(5)

		// 4. 复用GetAll()获取所有VMReport数据
		allReports, err := fs.GetAll(logPre)
		if err != nil {
			logger.Warn("调用GetAll失败，跳过本次计算", slog.String("pre", logPre), slog.Any("err", err))
			continue
		}

		// 5. 初始化统计变量，执行核心计算
		var (
			totalWeightedCache float64 // 总加权缓存：Σ(ActiveConnections*AvgCachePerConn)
			totalActiveConn    float64 // 总活跃连接数：Σ(ActiveConnections)
			totalLinksCong     map[string]tempLinksCongStruct
		)
		totalLinksCong = make(map[string]tempLinksCongStruct)

		// 6. 遍历GetAll()结果，累加统计值
		for _, report := range allReports {
			activeConn := float64(report.Congestion.ActiveConnections)
			avgCache := report.Congestion.AvgCachePerConn
			totalWeightedCache += activeConn * avgCache
			totalActiveConn += activeConn
			//探测任务copy
			for _, v := range report.LinksCongestion {
				if _, ok := totalLinksCong[v.TargetIP]; !ok {
					t := tempLinksCongStruct{}
					t.TargetIP = v.TargetIP
					t.ProbeTask = v.Target
					totalLinksCong[t.TargetIP] = t
				}
			}
			//处理链路
			for _, v := range report.LinksCongestion {
				t := totalLinksCong[v.TargetIP]
				t.PacketLosses = append(t.PacketLosses, v.PacketLoss)
				t.AverageLatencies = append(t.AverageLatencies, v.AverageLatency)
				totalLinksCong[v.TargetIP] = t
			}
		}
		b, _ := json.Marshal(totalLinksCong)
		logger.Info("totalLinksCong info", slog.String("pre", logPre),
			slog.String("data", string(b)))

		// 7. 避免除以0，输出计算结果
		var avgWeightedCache float64 = 0
		if totalActiveConn <= 0 {
			logger.Info("本次计算总活跃连接数为0无需计算平均值", slog.String("pre", logPre))
			totalWeightedCache = 0
		} else {
			avgWeightedCache = totalWeightedCache / totalActiveConn
		}

		//简单求均值
		linkMap := make(map[string]LinkCongestionInfo)
		for k, vs := range totalLinksCong {
			var avgLoss float64 = 0
			for _, v := range vs.PacketLosses {
				avgLoss += v
			}
			if avgLoss != 0 && len(vs.PacketLosses) > 0 {
				avgLoss = avgLoss / float64(len(vs.PacketLosses))
			}

			var avgLatency float64 = 0
			for _, v := range vs.AverageLatencies {
				avgLatency += v
			}
			if avgLatency != 0 && len(vs.AverageLatencies) > 0 {
				avgLatency = avgLatency / float64(len(vs.AverageLatencies))
			}

			linkMap[k] = LinkCongestionInfo{TargetIP: k, PacketLoss: avgLoss,
				Target: vs.ProbeTask, AverageLatency: avgLatency}
		}

		// 填充结果结构体
		result := NetworkTelemetry{
			NodeCongestion: NodeCongestionInfo{
				AvgWeightedCache:   avgWeightedCache,
				TotalWeightedCache: totalWeightedCache,
				TotalActiveConn:    totalActiveConn,
				VMCount:            len(allReports),
				CalculateTime:      time.Now(),
			},
			LinksCongestion: linkMap,
			PublicIP:        util.Config_.Node.IP.Public,
			Provider:        util.Config_.Node.Provider,
			Continent:       util.Config_.Node.Continent,
		}

		// 4. 结构体序列化为JSON（Etcd存储二进制数据，JSON格式易解析）
		jsonData, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			logger.Warn("结构体JSON序列化失败，跳过本次发送",
				slog.String("pre", logPre), slog.Any("错误", err))
			continue
		}
		logger.Info("结构体JSON序列化成功", slog.String("pre", logPre),
			slog.String("data", string(jsonData)))

		// 5. 发送（写入）数据到Etcd（*clientv3.Client核心操作）
		ip, _ := util.GetPublicIP()
		key := fmt.Sprintf("/routing/%s", ip)
		etcd_client.PutKey(etcdClient, key, string(jsonData), logPre, logger)
		_ = etcd_client.PutKeyWithLease(etcdClient, key, string(jsonData), int64(60*expireTime), logPre, logger)

		//放入queue 为自动化扩缩容做准备
		queue.Push(result)
		queue.Print(logPre)

		logger.Info("定时计算完成",
			slog.String("pre", logPre), slog.String("data", string(jsonData)))
	}
}
