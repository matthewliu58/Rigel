use axum::{extract::Query, http::StatusCode, Json};
use serde::Deserialize;
use crate::config::STATUS;
use crate::utils::generate_random_letters;
use tracing::{info, warn};

/// 健康状态切换查询参数
#[derive(Deserialize, Debug)]
pub struct HealthStateQuery {
    set: Option<String>,
}

/// 健康状态切换接口（/healthStateChange）
pub async fn health_state_change(Query(params): Query<HealthStateQuery>) -> (StatusCode, &'static str) {
    let pre = generate_random_letters(5);
    info!(%pre, "healthStateChange");

    let set = params.set.unwrap_or_else(|| "on".to_string());
    info!(%pre, "get switch val: {}", set);

    // 修改状态（加锁）
    if let Ok(mut status_lock) = STATUS.write() {
        *status_lock = if set == "off" {
            "off".to_string()
        } else {
            "on".to_string()
        };
    } else {
        warn!(%pre, "Failed to acquire status lock");
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }

    (StatusCode::OK, "success")
}

/// 健康检查接口（/health）
pub async fn health() -> (StatusCode, &'static str) {
    let pre = generate_random_letters(5);

    // 读取状态（加锁）
    let status = match STATUS.read() {
        Ok(lock) => lock.clone(),
        Err(e) => {
            warn!(%pre, "Failed to read status: {}", e);
            "on".to_string()
        }
    };

    info!(%pre, status = %status, "health check");

    if status == "off" {
        (StatusCode::INTERNAL_SERVER_ERROR, "error")
    } else {
        (StatusCode::OK, "success")
    }
}