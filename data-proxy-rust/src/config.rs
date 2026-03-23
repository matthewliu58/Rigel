use once_cell::sync::OnceCell;
use std::sync::RwLock;

// 全局配置（简化版，可扩展读取 YAML）
pub static PORT: OnceCell<String> = OnceCell::new();

// HTTP 头常量
pub const HEADER_HOPS: &str = "x-hops";
pub const HEADER_INDEX: &str = "x-index";
pub const HEADER_DEST_TYPE: &str = "X-Dest-Type";
pub const REMOTE_DISK: &str = "remote-disk";
pub const DEFAULT_INDEX: &str = "1";
pub const SERVER_ERROR_CODE: u16 = 503;
pub const BUFFER_SIZE: usize = 64 * 1024;

// 拥塞检测阈值
pub const WARNING_LEVEL_FOR_BUFFER: f64 = 0.6;
pub const CRITICAL_LEVEL_FOR_BUFFER: f64 = 0.8;

// 健康状态（全局）
pub static STATUS: RwLock<String> = RwLock::new(String::from("on"));

// 活跃传输数（原子计数）
pub static ACTIVE_TRANSFERS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);