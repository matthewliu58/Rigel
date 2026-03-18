#!/bin/bash
set -euo pipefail

# --------------------------
# 0. 前置检查
# --------------------------
#if [ "$USER" != "matth" ]; then
#    echo "❌ 必须以 matth 用户运行"
#    exit 1
#fi

# --------------------------
# 1. 常量定义
# --------------------------
ENVOY_VERSION="1.28.0"
ENVOY_HOME="/root"
ENVOY_BIN="${ENVOY_HOME}/envoy"
ENVOY_CONFIG="${ENVOY_HOME}/envoy-mini.yaml"
DOWNLOAD_URL=""
LUA_SCRIPT_PATH="${ENVOY_HOME}/access_router.lua"
OWNER="root:root"

# --------------------------
# 2. 架构检测
# --------------------------
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    DOWNLOAD_URL="https://github.com/envoyproxy/envoy/releases/download/v${ENVOY_VERSION}/envoy-${ENVOY_VERSION}-linux-x86_64"
elif [ "$ARCH" = "aarch64" ]; then
    DOWNLOAD_URL="https://github.com/envoyproxy/envoy/releases/download/v${ENVOY_VERSION}/envoy-${ENVOY_VERSION}-linux-aarch64"
else
    echo "❌ 不支持架构 ${ARCH}"
    exit 1
fi

# --------------------------
# 3. 系统依赖
# --------------------------
sudo apt update
sudo apt install -y curl ca-certificates libssl3 --no-install-recommends
sudo apt clean

# --------------------------
# 4. 下载 Envoy
# --------------------------
if [ -f "${ENVOY_BIN}" ]; then
    mv "${ENVOY_BIN}" "${ENVOY_BIN}.bak"
fi

curl -L "${DOWNLOAD_URL}" -o "${ENVOY_BIN}"
chmod +x "${ENVOY_BIN}"
#chown 640 "${ENVOY_BIN}"
sudo chown "${OWNER}" "${ENVOY_BIN}"

"${ENVOY_BIN}" --version

# --------------------------
# 5. 生成最小配置
# --------------------------
cat > "${ENVOY_CONFIG}" << EOF
admin:
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9901

static_resources:
  listeners: []
  clusters: []
EOF

#curl http://127.0.0.1:8081/config/port_bandwidth
## 正确返回示例（端口可字符串/数字，值必须是数字）
#{"8095":10485760, 8096:5242880}

echo "📝 生成 Lua 脚本 ${LUA_SCRIPT_PATH}..."
cat > "${LUA_SCRIPT_PATH}" << EOF
-- ${ENVOY_HOME}/lua/port_bandwidth_limit.lua
-- 核心配置（调整为合理周期）
local CHECK_INTERVAL = 5                     -- 带宽统计周期：5秒（兼顾精度和性能）
local DEFAULT_BW_LIMIT = 10 * 1024 * 1024    -- 备用默认限流值：10MB/s（x-rate头不存在/无效时使用）
local port_in_stats = {}                     -- 端口带宽统计缓存（全局变量）

-- 核心3：从请求头x-rate获取限流值（优先使用，无效则用默认值）
local function get_port_bw_limit(request_handle, log_map)
    -- 1. 获取请求头x-rate的值（忽略大小写，兼容X-Rate/x-rate等写法）
    local req_headers = request_handle:headers()
    local x_rate_str = req_headers:get("x-rate") or req_headers:get("X-Rate")

    -- 2. 解析x-rate值（预期为数字，单位：MB/s，自动转换为字节/秒）
    local x_rate_mb = tonumber(x_rate_str)
    local bw_limit = DEFAULT_BW_LIMIT  -- 默认兜底值

    if x_rate_mb and x_rate_mb > 0 then
        bw_limit = x_rate_mb * 1024 * 1024  -- 转换为字节/秒（与带宽统计单位一致）
        local info_msg = string.format("[Lua-INFO-1] 从x-rate头获取限流值：%.2fMB/s（转换后：%d字节/秒）", x_rate_mb, bw_limit)
        table.insert(log_map, info_msg)  -- 存入统一log_map
        request_handle:logErr(info_msg)  -- 统一用logErr输出，带级别标记
    else
        local warn_msg = string.format("[Lua-WARN-2] x-rate头不存在/无效（值：%s），使用默认限流值：10MB/s", x_rate_str or "nil")
        table.insert(log_map, warn_msg)  -- 存入统一log_map
        request_handle:logErr(warn_msg)  -- 统一用logErr输出，带级别标记
    end

    return bw_limit
end

-- 核心4：从x-port请求头获取端口（简单直接，替代自动获取）
local function get_port_from_header(request_handle, log_map)
    local current_port = nil
    local req_headers = request_handle:headers()
    -- 获取x-port头（忽略大小写，兼容X-Port/x-port等写法）
    local x_port_str = req_headers:get("x-port") or req_headers:get("X-Port")

    -- 解析端口号（必须是数字且在1-65535范围内，符合TCP/IP端口规范）
    local port_num = tonumber(x_port_str)
    if port_num and port_num > 0 and port_num <= 65535 then
        current_port = tonumber(port_num)
    end

    -- 日志记录端口获取结果，存入统一log_map
    local info_msg = string.format("[Lua-INFO-3] 从x-port头获取端口：%s（原始值：%s）", current_port or "获取失败/无效", x_port_str or "nil")
    table.insert(log_map, info_msg)
    request_handle:logErr(info_msg)
    return current_port
end

-- 核心5：计算端口实时入带宽（核心优化：先判断时间差，再获取指标，避免无效操作）
local function calculate_port_in_bandwidth(request_handle, port, log_map)
    if not port_in_stats[port] then
        port_in_stats[port] = { last_bytes = 0, last_check_time = os.time() }
    end
    local stats = port_in_stats[port]

    local bandwidth = 0
    local now = os.time()
    local time_diff = now - stats.last_check_time

    -- 第一步：先判断时间差是否满足统计周期，不满足则直接返回上次带宽值
    if time_diff < CHECK_INTERVAL or time_diff <= 0 then
        bandwidth = stats.last_bw or 0
        local info_msg = string.format("[Lua-INFO-4] 端口%d未到统计周期（当前差%d秒，要求≥%d秒），使用上次带宽值：%.2fMB/s",
          port, time_diff, CHECK_INTERVAL, bandwidth/1024/1024)
        table.insert(log_map, info_msg)  -- 存入统一log_map
        request_handle:logErr(info_msg)
        return bandwidth -- 提前返回，不执行后续逻辑
    end

    -- 第二步：仅当时间差满足要求时，才获取Envoy指标并计算带宽
    local stat_prefix = "ingress_http_" .. port
    local current_bytes = 0
    local ok, counter = pcall(function()
        return request_handle:stats():counter(stat_prefix .. ".downstream_rq_bytes_total")
    end)
    if ok and counter then
        current_bytes = counter:value()
    else
        local warn_msg = string.format("[Lua-WARN-5] 无法获取端口%d的带宽指标：%s", port, counter or "指标不存在")
        table.insert(log_map, warn_msg)  -- 存入统一log_map
        request_handle:logErr(warn_msg)
        return 0
    end

    -- 计算实时带宽并更新缓存
    local byte_diff = current_bytes - stats.last_bytes
    bandwidth = byte_diff / time_diff  -- 字节/秒
    stats.last_bytes = current_bytes
    stats.last_check_time = now
    stats.last_bw = bandwidth

    local info_msg = string.format("[Lua-INFO-6] 端口%d更新带宽统计：时间差=%d秒，累计字节差=%d，实时带宽=%.2fMB/s",
      port, time_diff, byte_diff, bandwidth/1024/1024)
    table.insert(log_map, info_msg)  -- 存入统一log_map
    request_handle:logErr(info_msg)

    return bandwidth
end

-- 核心6：请求限流逻辑（移除error_log_map，所有日志统一存入log_map）
function envoy_on_request(request_handle)
    -- 1. 初始化局部log_map，所有日志（信息/警告/错误）均存入此处，不再使用error_log_map
    local log_map = {}

    -- 1a. 打印所有请求 Header
    local headers = request_handle:headers()
    local header_log = "[Lua-INFO-7] 请求 Header 列表: "
    for key, value in pairs(headers) do
        header_log = header_log .. string.format("%s=%s; ", key, value)
    end
    table.insert(log_map, header_log)
    request_handle:logErr(header_log)

    local x_enable_str = headers:get("x-rate-limit-enable")
    if x_enable_str == nil then

        -- 拼接log_map所有日志，写入元数据（无需单独处理错误元数据）
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        -- 返回400 Bad Request响应
        request_handle:respond(
            {
                [":status"] = "400",
                Content_Type = "text/plain; charset=utf-8",
                X_Error_Type = "Missing x-rate-limit-enable header"
            },
            "Bad Request: x-rate-limit-enable header is missing or invalid."
        )

        return
    end

    if x_enable_str == "false" then
        -- 拼接log_map所有日志，写入元数据（无需单独处理错误元数据）
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)
        return
    end

    -- 2. 从x-port头获取端口
    local current_port = get_port_from_header(request_handle, log_map)
    if not current_port then
        local err_msg = "[Lua-ERROR-8] 限流失败：x-port头不存在/无效（请传递合法端口号1-65535）"
        -- 仅存入log_map，移除error_log_map相关操作
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)

        -- 拼接log_map所有日志，写入元数据（无需单独处理错误元数据）
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        -- 返回400 Bad Request响应
        request_handle:respond(
            {
                [":status"] = "400",
                Content_Type = "text/plain; charset=utf-8",
                X_Error_Type = "Invalid Port (x-port header)"
            },
            "Bad Request: x-port header is missing or invalid. Please pass a valid port number (range: 1-65535)."
        )

        return
    end

    -- 3. 从x-rate头获取限流阈值
    local port_limit = get_port_bw_limit(request_handle, log_map)
    local port_limit_mb = port_limit / 1024 / 1024

    -- 4. 按x-port传递的端口计算实时带宽（已优化：先判断时间差，再拿指标）
    local current_bw = calculate_port_in_bandwidth(request_handle, current_port, log_map)
    if current_bw <= 0 then
        local info_msg = string.format("[Lua-INFO-9] 端口%d带宽计算异常：%d字节/秒", current_port, current_bw)
        table.insert(log_map, info_msg)
        request_handle:logErr(info_msg)
    end
    local current_bw_mb = current_bw / 1024 / 1024

    -- 5. 带宽超限判断：触发503限流响应
    if current_bw > port_limit then

        local limit_msg = string.format("[Lua-INFO-10] 端口%d触发限流：%.2fMB/s > %.2fMB/s（阈值来自x-rate头）",
          current_port, current_bw_mb, port_limit_mb)
        table.insert(log_map, limit_msg)
        request_handle:logErr(limit_msg)

        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        request_handle:respond(
            {
                [":status"] = "503",
                X_Limit_Type = "Port In Bandwidth",
                X_Current_Port = tostring(current_port),
                X_Current_BW = string.format("%.2fMB/s", current_bw_mb),
                X_Max_BW = string.format("%.2fMB/s", port_limit_mb),
                X_Rate_Source = "Request Header x-rate",
                X_Port_Source = "Request Header x-port"
            },
            string.format("Port %d Bandwidth Limit Exceeded (Max: %.2fMB/s, From x-rate Header)", current_port, port_limit_mb)
        )

        return
    end

    -- 6. 带宽正常：记录日志
    local normal_msg = string.format("[Lua-INFO-11] 端口%d带宽正常：%.2fMB/s（上限：%.2fMB/s，阈值来自x-rate头）",
      current_port, current_bw_mb, port_limit_mb)
    table.insert(log_map, normal_msg)
    request_handle:logErr(normal_msg)


    -- 7. 核心：拼接log_map所有日志（含信息/警告/错误），一次性写入元数据，避免覆盖
    local full_info_msg = table.concat(log_map, "; ")
    request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_info_msg)

    -- 移除所有error_log_map相关的无效代码
end

-- 响应阶段空实现
function envoy_on_response(response_handle)
end
EOF

chown "${OWNER}" "${ENVOY_CONFIG}"
chown "${OWNER}" "${LUA_SCRIPT_PATH}"
chmod 644 "${ENVOY_CONFIG}"
chmod 644 "${LUA_SCRIPT_PATH}"

echo "✅ Envoy 安装完成！配置文件：${ENVOY_CONFIG}，二进制：${ENVOY_BIN}"
echo "⚠️ 请通过 Go 程序启动 Envoy"