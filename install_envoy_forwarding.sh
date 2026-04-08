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
ENVOY_HOME="/home/matth"
OWNER="matth:matth"
ENVOY_BIN="${ENVOY_HOME}/envoy"
ENVOY_CONFIG="${ENVOY_HOME}/envoy-mini.yaml"
DOWNLOAD_URL=""
LUA_SCRIPT_PATH="${ENVOY_HOME}/hop_router.lua"  # Lua script in same directory as config
PROFILE_DIR="$(dirname ${ENVOY_CONFIG})/profile"

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
    echo "ℹ️  发现已存在 Envoy 二进制，备份为 ${ENVOY_BIN}.bak"
    mv "${ENVOY_BIN}" "${ENVOY_BIN}.bak"
fi

echo "📥 下载 Envoy ${ENVOY_VERSION} (${ARCH})..."
curl -L "${DOWNLOAD_URL}" -o "${ENVOY_BIN}"
chmod +x "${ENVOY_BIN}"
#chown 640 "${ENVOY_BIN}"
sudo chown "${OWNER}" "${ENVOY_BIN}"

echo "✅ Envoy 版本验证："
"${ENVOY_BIN}" --version

# --------------------------
# 5. 创建 profile 目录（避免Admin报错）
# --------------------------
mkdir -p "${PROFILE_DIR}"
sudo chown "${OWNER}" "${PROFILE_DIR}"
chmod 755 "${PROFILE_DIR}"

# --------------------------
# 6. 生成最小配置
# --------------------------
echo "📝 生成 Envoy 配置文件 ${ENVOY_CONFIG}..."
cat > "${ENVOY_CONFIG}" << EOF
# Envoy 1.28.0 最小启动配置：强制保留Lua脚本加载（必选）
admin:
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9901
  access_log_path: "%ENV{ENVOY_HOME}%/admin_access.log"
  profile_path: "%ENV{ENVOY_HOME}%/profile"

layered_runtime:
  layers:
    - name: static_layer_0
      static_layer:
        envoy:
          lua:
            log_level: info
            allow_dynamic_loading: true
            enable_resty: true

static_resources:
  listeners:
    - name: listener_8095
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 8095
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                codec_type: HTTP1
                stat_prefix: ingress_http_8095
                access_log:
                  - name: envoy.access_logs.file
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
                      path: "%ENV{ENVOY_HOME}%/listener_8095_business.log"
                      log_format:
                        text_format: >
                          [%START_TIME%] "%REQ(:METHOD)% %REQ(:PATH)% %PROTOCOL%" %RESPONSE_CODE% %BYTES_RECEIVED% %BYTES_SENT%
                          [LISTENER] listener_8095 [PORT] 8095
                          [UPSTREAM] %UPSTREAM_HOST%
                          [LUA-INFO] %DYNAMIC_METADATA(lua_info:msg)%
                          \n
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: local_service
                      domains: ["*"]
                      routes:
                        - match:
                            prefix: "/"
                          route:
                            cluster: dynamic_target_cluster
                http_filters:
                  - name: envoy.filters.http.lua
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
                      default_source_code:
                        filename: "%ENV{ENVOY_HOME}%/hop_router.lua"
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

  clusters:
    - name: dynamic_target_cluster
      type: STRICT_DNS
      connect_timeout: 0.25s
      lb_policy: ROUND_ROBIN
      load_assignment:
        cluster_name: dynamic_target_cluster
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: 127.0.0.1  # 默认占位
                      port_value: 8080    # 默认占位
EOF

#场景 1：单跳代理（仅 B → S3）
#Client 发起请求时携带的 Headers：
# 核心 Headers（替换为实际地址）
#x-hops: 192.168.1.100:8080,s3.example.com:80    # 最终目标：S3 的 IP/域名+Port
#x-index: 1                   # 固定值 1
#
## 关键：Host 指向 B 节点的实际地址（TCP 自动转发）
#Host: 192.168.1.100:8080
#
## 通用 Headers
#Content-Type: application/json
#Accept: application/json

#场景 2：2 跳代理（A → B → S3）
#Client 发起请求时携带的 Headers：
## 核心 Headers（代理链+S3 均为 IP:Port）
#x-hops: 192.168.1.90:8080,192.168.1.100:8080,s3.example.com:80
#x-index: 1                   # 固定值 2（指向 B 节点）
#
## 关键：Host 指向 A 节点的实际地址
#Host: 192.168.1.90:8080
#
## 通用 Headers
#Content-Type: application/json
#Accept: application/json

#还要带上Client header 排查的时候知道从哪里来的

# --------------------------
# 7. 生成 Lua 脚本
# --------------------------
echo "📝 生成 Lua 脚本 ${LUA_SCRIPT_PATH}..."
cat > "${LUA_SCRIPT_PATH}" << EOF
-- Envoy Lua Filter: 极简hops动态路由（仅请求转发+响应透传）
-- 核心：动态设置Envoy转发目标，摆脱静态集群依赖
-- 优化：1. new_index > hops_len 时返回5xx错误 2. 仅最后一次写入dynamic_metadata 3. 移除所有无用冗余 4. 响应极致透传
-- ==============================================
-- 通用常量定义（仅保留核心必需项）
-- ==============================================
local HEADER_CONST = {
    HOPS = "x-hops",          -- 转发链：A1,A2,...An,S3（核心必需）
    INDEX = "x-index",        -- 游标索引（初始=1，核心必需）
    HOST = "Host",            -- 转发核心Header（核心必需）
    STATUS = ":status"        -- 响应状态码（仅用于本地错误响应）
}

local BUSINESS_RULE = {
    EMPTY_VALUE = "",               -- 空值兜底
    SEPARATOR = ",",                -- hops分隔符
    INIT_INDEX = "1",               -- 初始index=1
    SERVER_ERROR_CODE = "503"       -- new_index超出长度时返回503（服务不可用，适合转发异常场景）
}

-- ==============================================
-- 通用工具函数（仅保留必需的字符串拆分）
-- ==============================================
-- 拆分字符串为数组（解析hops，核心必需）
local function split_str(str, sep)
    local arr = {}
    if str == nil or str == BUSINESS_RULE.EMPTY_VALUE then
        return arr
    end
    for val in string.gmatch(str, "[^" .. sep .. "]+") do
        table.insert(arr, val)
    end
    return arr
end

-- ==============================================
-- 请求阶段（核心：解析x-hops转发请求，动态设置Envoy转发目标）
-- 新增：new_index > hops_len 时返回5xx错误，终止转发
-- ==============================================
function envoy_on_request(request_handle)
    -- 初始化log_map，统一缓存所有日志（信息/警告/错误）
    local log_map = {}

    -- 初始日志
    local init_msg = "[Lua-INFO-1] Start processing hop router request"
    table.insert(log_map, init_msg)
    request_handle:logErr(init_msg)

    -- 1. 读取请求Header（仅保留核心必需的hops_str和index_str）
    local hops_str = request_handle:headers():get(HEADER_CONST.HOPS) or BUSINESS_RULE.EMPTY_VALUE
    local index_str = request_handle:headers():get(HEADER_CONST.INDEX) or BUSINESS_RULE.INIT_INDEX
    local read_header_msg = string.format("[Lua-INFO-2] Read request headers | x-hops=%s | x-index=%s",
        hops_str, index_str)
    table.insert(log_map, read_header_msg)
    request_handle:logErr(read_header_msg)

    -- 2. 格式转换（核心必需：计算转发节点的基础）
    local hops_arr = split_str(hops_str, BUSINESS_RULE.SEPARATOR)
    local current_index = tonumber(index_str) or tonumber(BUSINESS_RULE.INIT_INDEX)
    local hops_len = #hops_arr
    local format_msg = string.format("[Lua-INFO-3] Format convert success | hops length=%d | current_index=%d",
        hops_len, current_index)
    table.insert(log_map, format_msg)
    request_handle:logErr(format_msg)

    -- 3. 空hops拒绝转发（核心错误处理：无转发链则无法继续）
    if hops_len == 0 then
        local err_msg = "[Lua-ERROR-4] Missing x-hops header, reject forwarding"
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- 一次性写入错误元数据
        request_handle:streamInfo():dynamicMetadata():set("lua_error", "msg", table.concat(log_map, "; "))
        -- 返回400响应
        request_handle:respond({[HEADER_CONST.STATUS] = "400"}, "Missing x-hops header")
        return
    end

    -- 4. 计算转发目标（核心业务逻辑：基于index定位转发节点）
    local target_hop = BUSINESS_RULE.EMPTY_VALUE
    local new_index = current_index + 1
    -- 新增：先判断new_index是否超出hops长度，超出则返回5xx
    if new_index > hops_len then
        local err_msg = string.format("[Lua-ERROR-5] Forward index out of range | new_index=%d | hops length=%d | current_index=%d",
            new_index, hops_len, current_index)
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- 一次性写入错误元数据
        request_handle:streamInfo():dynamicMetadata():set("lua_error","msg", table.concat(log_map, "; "))
        -- 返回5xx状态码（此处用503，可根据需求改为500等其他5xx码）
        request_handle:respond({[HEADER_CONST.STATUS] = BUSINESS_RULE.SERVER_ERROR_CODE},
            "Forward index out of range (no valid target hop)")
        return
    end

    -- 正常转发：new_index <= hops_len → 取对应节点
    target_hop = hops_arr[new_index]
    local forward_msg = string.format("[Lua-INFO-6] Normal forward | current_index=%d → target=%s | hops=%s",
        current_index, target_hop, hops_str)
    table.insert(log_map, forward_msg)
    request_handle:logErr(forward_msg)

    -- 5. 执行转发（核心业务逻辑：设置动态转发目标，修改Header）
    local target_ip, target_port = string.match(target_hop, "([^:]+):(%d+)")
    if target_ip and target_port then
        -- Header操作：设置动态转发目标（核心必需）
        request_handle:headers():replace(":authority", target_ip..":"..target_port)
        request_handle:headers():replace("x-host", target_hop)
        request_handle:headers():replace(HEADER_CONST.HOST, target_hop)

        local set_target_msg = string.format("[Lua-INFO-7] Set dynamic forward target | IP=%s | Port=%s | target_hop=%s",
            target_ip, target_port, target_hop)
        table.insert(log_map, set_target_msg)
        request_handle:logErr(set_target_msg)
    else
        local err_msg = string.format("[Lua-ERROR-8] Invalid target hop format | target_hop=%s", target_hop)
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- 一次性写入错误元数据
        request_handle:stream_info():dynamic_metadata():set("lua_error","msg", table.concat(log_map, "; "))
        -- 返回400响应
        request_handle:respond({[HEADER_CONST.STATUS] = "400"}, "Invalid target hop format (required: IP:Port)")
        return
    end

    -- 6. 更新Index Header（核心必需：传给下一跳，推进转发链路）
    request_handle:headers():replace(HEADER_CONST.INDEX, tostring(new_index))
    local update_index_msg = string.format("[Lua-INFO-9] Update x-index header | old_index=%d → new_index=%d",
        current_index, new_index)
    table.insert(log_map, update_index_msg)
    request_handle:logErr(update_index_msg)

    -- 最终日志
    local final_msg = string.format("[Lua-INFO-10] Request processed | hops=%s | current_index=%d | new_index=%d",
        hops_str, current_index, new_index)
    table.insert(log_map, final_msg)
    request_handle:logErr(final_msg)

    -- 核心优化：仅此处一次性写入lua_info元数据（全程唯一一次）
    request_handle:streamInfo():dynamicMetadata():set("lua_info","msg", table.concat(log_map, "; "))
end

-- ==============================================
-- 响应阶段（极致精简：无任何无用操作，纯透传）
-- ==============================================
function envoy_on_response(response_handle)
    -- 无任何响应修改操作 → Envoy自动原路返回下游所有响应（包括400/500等错误）
end
EOF

# --------------------------
# 8. 设置文件权限
# --------------------------
chown "${OWNER}" "${ENVOY_CONFIG}"
chown "${OWNER}" "${LUA_SCRIPT_PATH}"
chmod 644 "${ENVOY_CONFIG}"
chmod 644 "${LUA_SCRIPT_PATH}"

# --------------------------
# 9. 完成提示
# --------------------------
echo -e "\n✅ Envoy 安装配置全部完成！"
echo -e "📌 关键文件路径："
echo -e "  - Envoy 二进制：${ENVOY_BIN}"
echo -e "  - 配置文件：${ENVOY_CONFIG}"
echo -e "  - Lua 脚本：${LUA_SCRIPT_PATH}"
echo -e "  - Admin 日志：$(dirname ${ENVOY_CONFIG})/admin_access.log"
echo -e "  - 性能分析目录：${PROFILE_DIR}"
echo -e "⚠️  请通过 Go 程序启动 Envoy（启动命令参考：${ENVOY_BIN} -c ${ENVOY_CONFIG}）"