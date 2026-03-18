package routing

import (
	"control-plane/storage"
	"control-plane/util"
	"fmt"
	"log/slog"
	"math"
	"strings"
)

// 输入是client区域和cloud storage 区域
func (g *GraphManager) Routing(startContinent string, request util.EndPoints,
	pre string, logger *slog.Logger) util.RoutingInfo {

	logger.Info("Routing", slog.String("pre", pre),
		"startContinent", startContinent, "userRouteRequest", request)

	// 获取所有节点
	allNodes := g.GetNodes()

	// 根据大洲过滤 start 和 end 节点 && 展现寻找最优路径
	var startNodes []*storage.NetworkTelemetry

	for _, node := range allNodes {
		if node.Continent == startContinent {
			startNodes = append(startNodes, node)
		}
	}

	//client所在大区没有接入点 直接公网传输
	if len(startNodes) == 0 {
		logger.Warn("No nodes found for start continent",
			slog.String("pre", pre), "startContinent", startContinent)
		return util.RoutingInfo{}
	}

	cloudFull := fmt.Sprintf("%s_%s_%s",
		request.CloudProvider, request.CloudRegion, request.CloudID)

	//没有到该cloud storage的路径
	if _, ok := g.FindEdgeBySuffix(cloudFull); !ok {
		logger.Warn("No cloud node found",
			slog.String("pre", pre), "cloudFull", cloudFull)
		return util.RoutingInfo{}
	}

	// 遍历 start × end 节点组合，寻找最短路径
	var bestPath []string
	type Path struct {
		path []string
		cost float64
	}
	var tempPaths []Path
	minCost := math.Inf(1)
	for _, sNode := range startNodes {
		path, cost := g.Dijkstra(InNode(sNode.PublicIP), cloudFull)
		if path == nil {
			continue
		}
		tempPaths = append(tempPaths, Path{path, cost})
		if len(path) > 0 && cost < minCost {
			minCost = cost
			bestPath = path
		}
	}
	logger.Info("All candidate paths", slog.String("pre", pre),
		"paths", fmt.Sprintf("%+v", tempPaths))

	// 输出结果
	if len(bestPath) == 0 {
		logger.Warn("No path found between continents", slog.String("pre", pre),
			"startContinent", startContinent, "endContinent", cloudFull)
	} else {
		logger.Info("Shortest path found",
			slog.String("pre", pre), "startContinent", startContinent,
			"endContinent", cloudFull, "path", bestPath, "totalRisk", minCost)
	}

	var hops []string
	hopMap := make(map[string]string)
	for _, h := range bestPath {
		tempIP := strings.Split(h, "-")[0]
		if _, ok := hopMap[tempIP]; !ok {
			hops = append(hops, tempIP)
			hopMap[tempIP] = tempIP
		}
	}
	var hops_ []string
	for i := 0; i < len(hops)-1; i++ { //去掉最后一个 后面替换成真实的ip:port
		hops_ = append(hops_, hops[i]+":8090") //gateway port
	}
	merged := strings.Join(hops_, ",")
	merged += "," + request.ServerIP

	//计算速率
	rate := ComputeAdmissionRate(Task{WeightU: 1, MinRate: 10, MaxRate: 20},
		minCost, 1.0, 100, pre, g.logger)
	var paths []util.PathInfo
	paths = append(paths, util.PathInfo{Hops: merged, Rate: int64(rate)})
	rout := util.RoutingInfo{Routing: paths}
	logger.Info("routing result", slog.String("pre", pre),
		slog.Any("rout", rout))
	return rout
}
