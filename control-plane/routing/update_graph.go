package routing

import (
	"control-plane/storage"
	"control-plane/util"
	"fmt"
	"log/slog"
	"math"
	"sync"
)

//定时器维护 更新 路由map 供选路使用

// Edge 表示两个节点之间的边（逻辑或物理）
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

// UpdateWeight 安全更新 EdgeWeight
func (e *Edge) UpdateWeight(newWeight float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.EdgeWeight = newWeight
}

// Weight 安全读取 EdgeWeight
func (e *Edge) Weight() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.EdgeWeight
}

// ----------------------- GraphManager -----------------------
// 全局图管理器，维护 edges 和节点
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

// GetNodes 返回图中所有节点的指针切片
func (g *GraphManager) GetNodes() []*storage.NetworkTelemetry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]*storage.NetworkTelemetry, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// RemoveNode 删除节点及其相关的所有边
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

// GetEdges 返回当前所有 edges
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

// InitGraph 初始化节点和对应的虚拟边 & inter-node 边
// AddNodeWithEdges 将节点加入 GraphManager，并同时生成虚拟边和 inter-node 边
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

	return
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
//
// Inputs:
//
//	cacheUtil - average cache utilization (0~1), only meaningful for virtual edges
//	cost      - monetary / bandwidth cost, only meaningful for physical edges
//	lossRate  - packet loss rate (0~1), only meaningful for physical edges
//
// Convention:
//   - For virtual edges: cost = 0, lossRate = 0
//   - For physical edges: cacheUtil = 0
//
// Output:
//
//	A non-negative additive risk score (lower is better).
func EdgeRisk(cacheUtil, cost, lossRate float64, pre string, l *slog.Logger) float64 {
	// -----------------------------
	// Policy constants (system values)
	// -----------------------------

	l.Info("EdgeRisk", slog.String("pre", pre), slog.Any("cacheUtil", cacheUtil),
		slog.Any("cost", cost), slog.Any("lossRate", lossRate))

	const (
		cacheThreshold = 0.6 // Cache policy
		cacheScale     = 0.4
		costBaseline   = 0.2 // Cost policy
		wCache         = 0.5 // Weights (value priorities)
		wCost          = 0.4
		wLoss          = 0.1
	)

	var cacheRisk float64
	if cacheUtil > cacheThreshold {
		cacheRisk = math.Log(1 + (cacheUtil-cacheThreshold)/cacheScale)
	}

	var costRisk float64
	if cost > 0 {
		costRisk = math.Log(1 + cost/costBaseline)
	}

	var lossRisk float64
	if lossRate > 0 {
		if lossRate >= 1.0 {
			return math.Inf(1)
		}
		lossRisk = -math.Log(1 - lossRate)
	}

	r := wCache*cacheRisk + wCost*costRisk + wLoss*lossRisk

	l.Info("EdgeRisk", slog.String("pre", pre),
		slog.Float64("cacheRisk", cacheRisk),
		slog.Float64("costRisk", costRisk),
		slog.Float64("lossRisk", lossRisk),
		slog.Float64("risk", r))

	return r
}
