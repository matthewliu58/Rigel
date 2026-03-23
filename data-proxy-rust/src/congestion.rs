use crate::config::{
    ACTIVE_TRANSFERS, BUFFER_SIZE, WARNING_LEVEL_FOR_BUFFER,
};
use crate::utils::get_pid;
use serde::Serialize;
use sysinfo::{ProcessExt, System, SystemExt};
use tracing::{error, info, warn};

/// 代理状态结构体（对应 Go 的 ProxyStatus）
#[derive(Debug, Serialize)]
pub struct ProxyStatus {
    pub active_connections: i64,
    pub total_mem: u64,
    pub process_mem: u64,
    pub avg_cache_per_conn: f64,
    pub cache_usage_ratio: f64,
}

/// 检查拥塞状态（对应 Go 的 CheckCongestion）
pub fn check_congestion(all_buffer_size: usize) -> ProxyStatus {
    let mut status = ProxyStatus {
        active_connections: 0,
        total_mem: 0,
        process_mem: 0,
        avg_cache_per_conn: 0.0,
        cache_usage_ratio: 0.0,
    };

    // 获取系统总内存（替代 exec 调用 /proc/meminfo）
    let mut sys = System::new_all();
    sys.refresh_all();
    status.total_mem = sys.total_memory();

    // 获取当前进程内存（替代 ps 命令）
    let pid = get_pid() as i32;
    if let Some(process) = sys.process(pid) {
        status.process_mem = process.memory() * 1024; // rss 转为 bytes
    } else {
        error!("Failed to get process memory info");
        return status;
    }

    // 计算内存使用率
    let usage_ratio = status.process_mem as f64 / status.total_mem as f64;
    info!(
        "Proxy memory: {} MiB, Total memory: {} MiB, Ratio: {:.2}%",
        status.process_mem / 1024 / 1024,
        status.total_mem / 1024 / 1024,
        usage_ratio * 100.0
    );

    // 活跃连接数
    let active = ACTIVE_TRANSFERS.load(std::sync::atomic::Ordering::SeqCst);
    status.active_connections = active;

    if active <= 0 {
        return status;
    }

    // 平均每连接内存
    let per_conn_cache = all_buffer_size * 1024;
    let avg_cache = status.process_mem as f64 / active as f64;
    status.avg_cache_per_conn = avg_cache;
    status.cache_usage_ratio = avg_cache / per_conn_cache as f64;

    info!(
        "Active connections: {}, Average per-connection memory: {:.2} KB",
        active,
        avg_cache / 1024.0
    );

    // 拥塞警告
    if status.cache_usage_ratio > WARNING_LEVEL_FOR_BUFFER {
        warn!("Potential congestion: average per-connection buffer near {} KB", per_conn_cache / 1024);
    }

    info!("Proxy status: {:?}", status);
    status
}