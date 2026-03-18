package envoy_manager

import (
	"os"
	"text/template"

	"gopkg.in/yaml.v3"
)

// EnvoyYamlTemplate Envoy config template (v1.28.0)
const EnvoyYamlTemplate = `
admin:
  address:
    socket_address:
      address: 0.0.0.0
      port_value: {{.AdminPort}}
  # 可选：deprecated，但目前还能用
  access_log_path: "{{$.PathBase}}/admin_access.log"
  profile_path: "{{$.PathBase}}/profile"

# ================= Lua runtime =================
layered_runtime:
  layers:
    - name: static_layer_0
      static_layer:
        envoy:
          lua:
            allow_dynamic_loading: true
            enable_resty: true
            log_level: info

static_resources:
  listeners:
{{- range .Ports }}
{{- if .Enabled }}
    - name: listener_{{.Port}}
      address:
        socket_address:
          address: 0.0.0.0
          port_value: {{.Port}}
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager

                stat_prefix: ingress_http_{{.Port}}

                # 正确字段名：access_log（不是 access_logs）
                access_log:
                  - name: envoy.access_logs.file
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
                      path: "{{$.PathBase}}/all_listeners_business.log"
                      log_format:
                        text_format: >
                          [%START_TIME%] "%REQ(:METHOD)% %REQ(:PATH)% %PROTOCOL%" %RESPONSE_CODE% %BYTES_RECEIVED% %BYTES_SENT%
                          [LISTENER] route_{{.Port}}
                          [LUA-INFO] %DYNAMIC_METADATA(lua_info:msg)%
                          \n

                route_config:
                  name: local_route_{{.Port}}
                  virtual_hosts:
                    - name: local_service_{{.Port}}
                      domains: ["*"]
                      routes:
                        - match:
                            prefix: "/"
                          route:
                            cluster: target_cluster

                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

                #http_filters:
                #  - name: envoy.filters.http.lua
                #    typed_config:
                #      "@type": type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
                #      default_source_code:
                #        filename: "{{$.PathBase}}/access_router.lua"

                #  - name: envoy.filters.http.router
                #    typed_config:
                #      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
{{- end }}
{{- end }}

  clusters:
    - name: target_cluster
      connect_timeout: 0.25s
      type: STATIC
      lb_policy: ROUND_ROBIN

      # 健康检查只允许在 cluster.health_checks
      health_checks:
        - timeout: 1s
          interval: 5s
          unhealthy_threshold: 2
          healthy_threshold: 2
          http_health_check:
            path: "/health"

      load_assignment:
        cluster_name: target_cluster
        endpoints:
          - lb_endpoints:
{{- range .TargetAddrs }}
              - endpoint:
                  address:
                    socket_address:
                      address: {{.IP}}
                      port_value: {{.Port}}
                  health_check_config:
                    port_value: {{.Port}}
{{- end }}
`

// RenderEnvoyYamlConfig 渲染Envoy YAML配置文件到matth目录
func RenderEnvoyYamlConfig(cfg *EnvoyGlobalConfig, outputPath string) error {
	// 解析模板
	tpl, err := template.New("envoy_config").Parse(EnvoyYamlTemplate)
	if err != nil {
		return err
	}

	// 创建/覆盖配置文件（matth目录有读写权限）
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 渲染模板并写入文件
	if err := tpl.Execute(file, cfg); err != nil {
		return err
	}

	// 验证YAML格式（增强鲁棒性）
	var validate map[string]interface{}
	yamlFile, err := os.ReadFile(outputPath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(yamlFile, &validate)
}

//- name: envoy.filters.network.bandwidth_limit
//  typed_config:
//    "@type": type.googleapis.com/envoy.extensions.filters.network.bandwidth_limit.v3.BandwidthLimit
//    stat_prefix: bandwidth_limit_{{.Port}}
//    max_download_bandwidth: {{.RateLimit.Bandwidth}}
//    max_upload_bandwidth: {{.RateLimit.Bandwidth}}
