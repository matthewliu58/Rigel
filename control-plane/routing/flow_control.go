package routing

import (
	"log/slog"
	"math"
)

//α = 1.0 // proportional fairness
//V^M = 100.0      // 控制队列规模 ~ O(100)
//w_k^U    = {1,2,4} // 任务优先级（SLA）
//w_e^M = 1.0 // 默认所有边等权
//a_k_max = 链路瓶颈 // Mbps 或 packets/s

type Task struct {
	ID      string
	WeightU float64 // w_k^U
	MinRate float64
	MaxRate float64 // a_k^max
}

func ComputeAdmissionRate(
	task Task,
	cost float64,
	alpha float64,
	V float64,
	pre string,
	logger *slog.Logger,
) float64 {

	logger.Info("ComputeAdmissionRate", slog.String("pre", pre),
		slog.Any("task", task), slog.Any("cost", cost),
		slog.Any("alpha", alpha), slog.Any("V", V))

	// Step 1: compute path cost C_k^*(t)
	var pathCost float64 = cost

	// Numerical safety
	if pathCost <= 0 {
		return 0
	}

	// Step 2: closed-form optimal rate
	rawRate := math.Pow(
		V*task.WeightU/pathCost,
		1.0/alpha,
	)

	logger.Info("ComputeAdmissionRate", slog.String("pre", pre), slog.Any("rawRate", rawRate))

	// Step 3: projection to a feasible region
	if rawRate < task.MinRate {
		return task.MinRate
	}
	if rawRate > task.MaxRate {
		return task.MaxRate
	}

	return rawRate
}
