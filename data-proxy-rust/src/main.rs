mod config;
mod logger;
mod client_pool;
mod proxy_handler;
mod health;
mod congestion;
mod utils;

use axum::{routing::get, Router};
use crate::config::{BUFFER_SIZE, PORT};
use crate::congestion::check_congestion;
use crate::health::{health, health_state_change};
use crate::logger::init_logger;
use crate::proxy_handler::proxy_handler;
use serde_json::json;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    if let Err(e) = init_logger() {
        panic!("Failed to init logger: {}", e);
    }

    let pre = "init";
    info!(%pre, "Starting data proxy service");

    // 初始化端口（默认 8095）
    PORT.set("8095".to_string()).unwrap();

    // 构建路由
    let app = Router::new()
        // 健康检查接口
        .route("/healthStateChange", get(health_state_change))
        .route("/health", get(health))
        // 拥塞信息接口
        .route("/getCongestionInfo", get(|| async {
            let status = check_congestion(2 * BUFFER_SIZE);
            json!(status).to_string()
        }))
        // 所有其他路由走代理
        .fallback(proxy_handler);

    // 启动服务
    let port = PORT.get().unwrap();
    let addr = format!("0.0.0.0:{}", port);
    info!(%pre, "Listening on {}", addr);

    if let Err(e) = axum::Server::bind(&addr.parse()?)
        .serve(app.into_make_svc())
        .await
    {
        error!(%pre, "Failed to start server: {}", e);
        std::process::exit(1);
    }

    Ok(())
}