package routing

import (
	"control-plane/storage"
	"control-plane/util"
	"fmt"
	"log/slog"
	"math"
	"sync"
)

type Edge struct {
	SourceIp             string  `json:"source_ip"`             // A 节点名/ID
	DestinationIp        string  `json:"destination_ip"`        // B 节点名/ID
	SourceProvider       string  `json:"source_provider"`       // A 节点云服务商 // 节点额外信息
	SourceContinent      string  `json:"source_continent"`      // A 节点大区
	DestinationProvider  string  `json:"destination_provider"`  // B 节点云服务商
	DestinationContinent string  `json:"destination_continent"` // B 节点大区
	EdgeWeight           float64 `json:"edge_weight"`           // 综合权重，用于最短路径计算
	//BandwidthPrice float64 `json:"bandwidth_price"` // A 节点出口带宽价格 ($/GB)// 网络/缓存信息
	//Latency float64 `json:"latency"` // A->B 时延
	//Loss float64 `json:"loss"` //A->B 丢包率
	//CacheUsageRatio float64 `json:"cache_usage_ratio"` // 缓存占用比例 [0,1]
	mu sync.RWMutex // 保护动态字段（BandwidthPrice, Latency, CacheUsageRatio, EdgeWeight）
}

func (e *Edge) UpdateWeight(newWeight float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.EdgeWeight = newWeight
}

func (e *Edge) Weight() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.EdgeWeight
}

type GraphManager struct {
	mu     sync.RWMutex
	edges  map[string]*Edge                     // key: "source->destination"
	nodes  map[string]*storage.NetworkTelemetry // storage.NetworkTelemetry
	logger *slog.Logger
}

// NewGraphManager 初始化
func NewGraphManager(logger *slog.Logger) *GraphManager {
	return &GraphManager{
		edges:  make(map[string]*Edge),
		nodes:  make(map[string]*storage.NetworkTelemetry),
		logger: logger,
	}
}

func (g *GraphManager) GetNode(id string) (*storage.NetworkTelemetry, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, ok := g.nodes[id]
	return node, ok
}

func (g *GraphManager) GetNodes() []*storage.NetworkTelemetry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]*storage.NetworkTelemetry, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

func (g *GraphManager) RemoveNode(id, logPre string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 删除节点
	delete(g.nodes, id)

	// 删除与该节点相关的所有边
	for key, e := range g.edges {
		if e.SourceIp == id || e.DestinationIp == id {
			delete(g.edges, key)
		}
	}
}

func (g *GraphManager) GetEdges() []*Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	list := make([]*Edge, 0, len(g.edges))
	for _, e := range g.edges {
		list = append(list, e)
	}
	return list
}

func (g *GraphManager) GetEdge(edgeID string) *Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if e, ok := g.edges[edgeID]; ok {
		return e
	}
	return nil
}

func InNode(n string) string {
	return n + "-1"
}

func OutNode(n string) string {
	return n + "-2"
}

func (g *GraphManager) AddNode(node *storage.NetworkTelemetry, logPre string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 1. 添加节点
	g.nodes[node.PublicIP] = node

	// 2. 添加虚拟边（in->out）
	in := InNode(node.PublicIP)
	out := OutNode(node.PublicIP)
	newLine := in + "->" + out
	r := EdgeRisk(node.NodeCongestion.AvgWeightedCache, 0, 0,
		logPre+"-"+newLine, g.logger)

	oldLine, ok := g.edges[newLine]
	if ok {
		oldLine.EdgeWeight = r
	} else {
		g.edges[newLine] = &Edge{
			SourceIp:        in,
			DestinationIp:   out,
			SourceProvider:  node.Provider,
			SourceContinent: node.Continent,
			EdgeWeight:      r,
		}
	}
	//g.logger.Info("EdgeRisk", slog.String("pre", logPre), in + "->" + out, r)

	//添加节点到cloud storage server
	for _, v := range node.LinksCongestion {
		if v.Target.TargetType == "cloud_storage" {

			pp, _ := util.GetBandwidthPrice(node.Provider, node.Continent, v.Target.Region, g.logger)
			cloudFull := fmt.Sprintf("%s_%s_%s", v.Target.Provider, v.Target.Region, v.Target.ID)
			newLine = out + "->" + cloudFull
			r = EdgeRisk(0, pp, v.PacketLoss, logPre+"-"+newLine, g.logger)

			oldLine, ok := g.edges[newLine]
			if ok {
				oldLine.EdgeWeight = r
			} else {
				g.edges[newLine] = &Edge{
					SourceIp:        out,
					DestinationIp:   cloudFull,
					SourceProvider:  node.Provider,
					SourceContinent: node.Continent,
					EdgeWeight:      r,
				}
			}
			//g.logger.Info("EdgeRisk", slog.String("pre", logPre), out+"->"+cloudFull, r)
		}
	}

	// 3. 添加 inter-node 边：当前节点的 out -> 其他节点的 in
	for id, other := range g.nodes {
		if id == node.PublicIP {
			continue
		}
		pp, _ := util.GetBandwidthPrice(node.Provider, node.Continent, other.Continent, g.logger)
		var ll float64 = 0
		if val, ok := node.LinksCongestion[id]; ok && val.PacketLoss > 0 {
			ll = val.PacketLoss
		}
		// 新节点 out -> 老节点 in
		newLine = out + "->" + InNode(id)
		r = EdgeRisk(0, pp, ll, logPre+"-"+newLine, g.logger)

		oldLine, ok := g.edges[newLine]
		if ok {
			oldLine.EdgeWeight = r
		} else {
			g.edges[newLine] = &Edge{
				SourceIp:        out,
				DestinationIp:   InNode(id),
				SourceProvider:  node.Provider,
				SourceContinent: node.Continent,
				EdgeWeight:      r,
			}
		}
		//g.logger.Info("EdgeRisk", slog.String("pre", logPre), out+"->"+InNode(id), r)

		pp_, _ := util.GetBandwidthPrice(other.Provider, other.Continent, node.Continent, g.logger)
		//var ll_ float64 = 0
		//if val, ok := node.LinksCongestion[id]; ok && val.PacketLoss > 0 {
		//	ll_ = val.PacketLoss
		//}
		// 老节点 out -> 新节点 in
		newLine = OutNode(id) + "->" + in
		r = EdgeRisk(0, pp_, ll, logPre+"-"+newLine, g.logger)

		oldLine, ok = g.edges[newLine]
		if ok {
			oldLine.EdgeWeight = r
		} else {
			g.edges[newLine] = &Edge{
				SourceIp:        OutNode(id),
				DestinationIp:   in,
				SourceProvider:  other.Provider,
				SourceContinent: other.Continent,
				EdgeWeight:      r,
			}
		}
		//g.logger.Info("EdgeRisk", slog.String("pre", logPre), OutNode(id)+"->"+in, r)
	}
}

func (g *GraphManager) DumpGraph(logPre string) {
	//打印整个拓扑图 的 节点和边
	g.logger.Debug("DumpGraph", slog.String("pre", logPre))
	for _, node := range g.GetNodes() {
		g.logger.Debug("Graph Node", slog.String("pre", logPre), slog.Any("node", node))
	}
	for _, edge := range g.GetEdges() {
		g.logger.Debug("Graph Edge", slog.String("pre", logPre), slog.Any("edge", edge))
	}
}

// EdgeRisk computes the unified risk score for both virtual and physical edges.
func EdgeRisk(cacheUtil, cost, lossRate float64, pre string, l *slog.Logger) float64 {

	l.Info("EdgeRisk", slog.String("pre", pre), slog.Float64("cacheUtil", cacheUtil),
		slog.Float64("cost", cost), slog.Float64("lossRate", lossRate))

	const (
		// -------- 缓存配置 --------
		cacheThreshold = 0.6
		cacheMax       = 1.0

		// -------- 成本配置 --------
		costMax = 0.15

		// -------- 丢包敏感配置 --------
		lossInflection = 0.05
		lossSharpness  = 40.0

		// -------- 核心权重 --------
		wCoreCache = 0.5
		wCoreLoss  = 0.5

		// -------- 成本偏好 --------
		baseCostPref    = 0.4 // 基础成本权重
		maxCostPref     = 0.6 // 网络极好时，最大成本权重
		healthThreshold = 0.2 // coreRisk < 0.2 = 网络很健康
	)

	// 缓存风险
	var cacheRisk float64
	if cacheUtil > cacheThreshold {
		excess := cacheUtil - cacheThreshold
		maxExcess := cacheMax - cacheThreshold
		cacheRisk = math.Pow(excess/maxExcess, 1.5)
	}

	// 成本风险
	var costRisk float64
	if cost > 0 {
		costRisk = cost / costMax
		if costRisk > 1.0 {
			costRisk = 1.0
		}
	}

	// 丢包风险
	var lossRisk float64
	if lossRate > 0 {
		if lossRate >= 1.0 {
			return 1.0
		}
		x := lossSharpness * (lossRate - lossInflection)
		lossRisk = 1.0 / (1.0 + math.Exp(-x))
	}

	// 系统风险（缓存+丢包 对等权重）
	coreSurvival := (1 - wCoreCache*cacheRisk) * (1 - wCoreLoss*lossRisk)
	coreRisk := 1 - coreSurvival

	// 成本权重
	var wCost float64
	if coreRisk <= healthThreshold {
		// 网络很健康 → 提高成本权重（最多到 maxCostPref）
		health := 1 - (coreRisk / healthThreshold)
		wCost = baseCostPref + (maxCostPref-baseCostPref)*health
	} else {
		// 网络一般/差 → 用基础权重，钱不重要
		wCost = baseCostPref
	}

	totalRisk := coreRisk + (1.0-coreRisk)*wCost*costRisk
	if totalRisk > 1.0 {
		totalRisk = 1.0
	}

	l.Info("EdgeRisk result", slog.String("pre", pre), slog.Float64("cacheRisk", cacheRisk),
		slog.Float64("lossRisk", lossRisk), slog.Float64("coreRisk", coreRisk),
		slog.Float64("wCost", wCost), slog.Float64("totalRisk", totalRisk))

	return totalRisk
}
