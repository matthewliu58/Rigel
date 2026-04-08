package api

import (
	"control-plane/scaling"
	"control-plane/util"
	model "control-plane/vm_info"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ManualScalingRequest HTTP请求体
type ManualScalingRequest struct {
	Action   string `json:"action"` // scale_up, start, sleep, release, attach_envoy
	PublicIP string `json:"public_ip"`
	VMName   string `json:"vm_name"`
	//Count  int    `json:"count,omitempty"` // 扩容数量，仅 scale_up 使用
}

// ManualScalingHandler 封装manual scaling API逻辑
type ManualScalingAPIHandler struct {
	Scaler  *scaling.Scaler
	Logger  *slog.Logger
	logger1 *slog.Logger
}

// NewManualScalingAPIHandler 初始化Handler
func NewManualScalingAPIHandler(s *scaling.Scaler, logger, logger1 *slog.Logger) *ManualScalingAPIHandler {
	return &ManualScalingAPIHandler{Scaler: s, Logger: logger, logger1: logger1}
}

// PostManualScaling 处理 POST /api/v1/scaling/manual
func (h *ManualScalingAPIHandler) PostManualScaling(c *gin.Context) {
	resp := model.ApiResponse{
		Code: 200,
		Msg:  "OK",
		Data: nil,
	}
	pre := util.GenerateRandomLetters(5)
	// 解析请求体
	var req ManualScalingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Code = 400
		resp.Msg = err.Error()
		c.JSON(http.StatusOK, resp)
		h.Logger.Error("Failed to parse ManualScalingRequest", slog.String("pre", pre), slog.Any("error", err))
		return
	}

	b, _ := json.Marshal(req)
	h.Logger.Info("ManualScalingRequest", slog.String("pre", pre), slog.String("data", string(b)))

	// 执行手动操作
	h.Scaler.ManualScaling(pre, req.Action, req.PublicIP, req.VMName)

	// 返回结果
	c.JSON(http.StatusOK, resp)
}

// InitManualScalingRouter 注册manual scaling路由
func InitManualScalingRouter(router *gin.Engine, s *scaling.Scaler, logger, logger1 *slog.Logger) *gin.Engine {
	r := router
	apiV1 := r.Group("/api/v1")
	{
		scalingGroup := apiV1.Group("/scaling")
		{
			handler := NewManualScalingAPIHandler(s, logger, logger1)
			scalingGroup.POST("", handler.PostManualScaling)
		}
	}
	return r
}
