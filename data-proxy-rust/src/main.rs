mod config;
mod logger;
mod client_pool;
mod proxy_handler;
mod health;
mod congestion;
mod utils;

use axum::{routing::get, Router};
use crate::config::{BUFFER_SIZE, PORT};
use crate::congestion::{check_congestion};
use crate::health::{health, health_state_change};
use crate::logger::init_logger;
use crate::proxy_handler::proxy_handler;
use serde_json::json;
use tracing::{error, info};

use serde::Deserialize;
use std::error::Error;

// 限制进程最大内存（RLIMIT_AS）
// use libc::{rlimit, RLIMIT_AS, setrlimit};

// 配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub port: String,
    pub mem: u64,   // 单位：GB
}

/// 加载配置
pub fn load_config() -> Result<AppConfig, Box<dyn Error>> {
    let cfg = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .build()?;

    let config = cfg.try_deserialize::<AppConfig>()?;
    Ok(config)
}

//设置进程最大内存（从配置文件读取）
// fn set_process_max_memory(mem_gb: u64) {
//     let max_bytes = mem_gb * 1024 * 1024 * 1024; // 转成字节
//
//     let rlim = rlimit {
//         rlim_cur: max_bytes,
//         rlim_max: max_bytes,
//     };
//
//     unsafe {
//         setrlimit(RLIMIT_AS, &rlim);
//     }
//     info!("内存限制已设置: {} GB", mem_gb);
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 日志
    if let Err(e) = init_logger() {
        panic!("Failed to init logger: {}", e);
    }

    let pre = "init";
    info!(%pre, "Starting data proxy service");

    // 加载配置
    let config = load_config()?;
    info!("加载配置成功：{:?}", config);

    // 端口
    PORT.set(config.port.clone()).unwrap();

    //从配置文件设置内存上限
//     set_process_max_memory(config.mem);
//     let max_mem = get_process_max_memory();
//     println!("当前进程最大内存：{} MB", max_mem / 1024 / 1024);

    // 路由
    let app = Router::new()
        .route("/healthStateChange", get(health_state_change))
        .route("/health", get(health))
        .route("/getCongestionInfo", get(|| async {
            let status = check_congestion(2 * BUFFER_SIZE);
            json!(status).to_string()
        }))
        .fallback(proxy_handler);

    // 启动
    let port = PORT.get().unwrap();
    let addr = format!("0.0.0.0:{}", port);
    info!(%pre, "Listening on {}", addr);

    axum::Server::bind(&addr.parse()?)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}