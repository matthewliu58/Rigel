package api

import (
	envoymanager2 "control-plane/pkg/envoy-manager"
	"control-plane/util"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http"
)

// EnvoyPortAPIHandler Envoy端口API处理器
type EnvoyPortAPIHandler struct {
	operator *envoymanager2.EnvoyOperator
	logger   *slog.Logger
	logger1  *slog.Logger
}

// NewEnvoyPortAPIHandler 创建API处理器实例
func NewEnvoyPortAPIHandler(operator *envoymanager2.EnvoyOperator, logger *slog.Logger) *EnvoyPortAPIHandler {
	return &EnvoyPortAPIHandler{
		operator: operator,
		logger:   logger,
	}
}

// HandleEnvoyPortCreate 处理创建Envoy端口请求（POST /envoy/port/create）
func (h *EnvoyPortAPIHandler) HandleEnvoyPortCreate(c *gin.Context) {
	var req envoymanager2.EnvoyPortCreateReq

	pre := util.GenerateRandomLetters(5)
	h.logger.Info("HandleEnvoyPortCreate", slog.String("pre", pre))

	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("绑定创建端口请求失败", "error", err)
		c.JSON(http.StatusBadRequest, envoymanager2.APICommonResp{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	// 创建/更新端口（配置文件写入/home/matth/envoy.yaml）
	portCfg, err := h.operator.CreateOrUpdateEnvoyPort(req, pre, h.logger, h.logger1)
	if err != nil {
		h.logger.Error("创建Envoy端口失败", "port", req.Port, "error", err)
		c.JSON(http.StatusInternalServerError, envoymanager2.APICommonResp{
			Code:    500,
			Message: "创建端口失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, envoymanager2.APICommonResp{
		Code:    0,
		Message: "端口创建/更新成功",
		Data:    portCfg,
	})
}

// HandleEnvoyPortDisable 处理禁用Envoy端口请求（POST /envoy/port/disable）
func (h *EnvoyPortAPIHandler) HandleEnvoyPortDisable(c *gin.Context) {

	pre := util.GenerateRandomLetters(5)
	h.logger.Info("HandleEnvoyPortDisable", slog.String("pre", pre))

	var req envoymanager2.EnvoyPortDisableReq
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("绑定禁用端口请求失败", "error", err)
		c.JSON(http.StatusBadRequest, envoymanager2.APICommonResp{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	// 禁用端口
	if err := h.operator.DisableEnvoyPort(req.Port, pre, h.logger, h.logger1); err != nil {
		h.logger.Error("禁用Envoy端口失败", "port", req.Port, "error", err)
		c.JSON(http.StatusInternalServerError, envoymanager2.APICommonResp{
			Code:    500,
			Message: "禁用端口失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, envoymanager2.APICommonResp{
		Code:    0,
		Message: "端口禁用成功",
	})
}

// HandleEnvoyPortQuery 处理查询Envoy端口请求（GET /envoy/port/query）
func (h *EnvoyPortAPIHandler) HandleEnvoyCfgQuery(c *gin.Context) {

	// 查询端口配置
	cfg, err := h.operator.GetCurrentConfig()
	if err != nil {
		h.logger.Error("查询 Envoy cfg 失败", "error", err)
		c.JSON(http.StatusNotFound, envoymanager2.APICommonResp{
			Code:    404,
			Message: "Envoy cfg 未找到: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, envoymanager2.APICommonResp{
		Code:    0,
		Message: "查询 Envoy cfg 成功",
		Data:    cfg,
	})
}

func (o *EnvoyPortAPIHandler) UpdateGlobalTargetAddrsHandler(c *gin.Context) {

	pre := util.GenerateRandomLetters(5)
	o.logger.Info("Update envoy target addrs", slog.String("pre", pre))

	// 1. 绑定并校验请求体
	var req []envoymanager2.EnvoyTargetAddr
	if err := c.ShouldBindJSON(&req); err != nil {
		o.logger.Error("Invalid request body", slog.String("pre", pre), "error", err)
		c.JSON(http.StatusOK, envoymanager2.APICommonResp{
			Code:    400,
			Message: err.Error(),
			Data:    nil,
		})
		return
	}

	action := c.GetHeader("Action")
	o.logger.Info("action and addr", slog.String("pre", pre),
		slog.String("action", action), slog.Any("addr", req))

	if action == "" {
		o.logger.Error("Invalid request body", slog.String("pre", pre), slog.String("action", action))
		c.JSON(http.StatusOK, envoymanager2.APICommonResp{
			Code:    400,
			Message: "Action 参数错误",
			Data:    nil,
		})
		return
	}

	// 2. 调用核心方法更新配置
	if err := o.operator.UpdateGlobalTargetAddrs(req, action, pre, o.logger, o.logger1); err != nil {
		o.logger.Error("Failed to update target addrs", slog.String("pre", pre), "error", err)
		c.JSON(http.StatusInternalServerError, envoymanager2.APICommonResp{
			Code:    500,
			Message: err.Error(),
			Data:    nil,
		})
		return
	}

	// 3. 返回成功响应
	o.logger.Info("Update envoy target addrs success", slog.String("pre", pre), "target_addrs", req)
	c.JSON(http.StatusOK, envoymanager2.APICommonResp{
		Code:    200,
		Message: "更新后端地址成功",
		Data:    nil,
	})
}

func (o *EnvoyPortAPIHandler) GetPortBandwidthConfigHandler(c *gin.Context) {
	cfgCopy, err := o.operator.GetCurrentConfig()
	config := make(map[int]int64)
	if err != nil {
		c.JSON(http.StatusOK, config)
		return
	}
	for _, port := range cfgCopy.Ports {
		config[port.Port] = port.RateLimit.Bandwidth
	}
	c.JSON(http.StatusOK, config)
}

// InitEnvoyAPIRouter 初始化Envoy端口API路由（已固化matth目录路径）
func InitEnvoyAPIRouter(router *gin.Engine, operator *envoymanager2.EnvoyOperator, logger, logger1 *slog.Logger) {

	// 3. 创建API处理器
	handler := NewEnvoyPortAPIHandler(operator, logger)

	// 4. 注册路由
	envoyGroup := router.Group("/envoy/port")
	{
		envoyGroup.POST("/create", handler.HandleEnvoyPortCreate)
		envoyGroup.POST("/disable", handler.HandleEnvoyPortDisable)
	}
	envoyGroup1 := router.Group("/envoy/cfg")
	{
		envoyGroup1.POST("/setTargetIps", handler.UpdateGlobalTargetAddrsHandler)
		envoyGroup1.GET("/query", handler.HandleEnvoyCfgQuery)
	}
	envoyGroup2 := router.Group("/config")
	{
		envoyGroup2.GET("/port_bandwidth", handler.GetPortBandwidthConfigHandler)
	}
}
