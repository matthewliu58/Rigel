use hyper::Client;
use hyper_tls::HttpsConnector;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, LazyLock};
use std::time::Duration;

type HttpClient = Client<HttpsConnector<hyper::client::HttpConnector>>;

// 客户端池（全局）—— 修复静态初始化错误
static CLIENT_POOL: LazyLock<RwLock<HashMap<String, Arc<HttpClient>>>> = LazyLock::new(|| {
    RwLock::new(HashMap::new())
});

/// 获取 HTTP/HTTPS 客户端（复用连接池，对应 Go 的 getClient）
pub fn get_client(target: &str, _scheme: &str) -> Arc<HttpClient> {
    // 读锁检查是否存在
    if let Ok(read_lock) = CLIENT_POOL.read() {
        if let Some(client) = read_lock.get(target) {
            return Arc::clone(client);
        }
    }

    // 创建新客户端
    let https = HttpsConnector::new();
    let client = Client::builder()
        .pool_max_idle_per_host(50)
        .pool_idle_timeout(Duration::from_secs(10))
        .http1_read_buf_exact_size(crate::config::BUFFER_SIZE)
        .http1_max_buf_size(crate::config::BUFFER_SIZE)
        .build::<_, hyper::Body>(https);

    let client_arc = Arc::new(client);

    // 写锁插入池
    if let Ok(mut write_lock) = CLIENT_POOL.write() {
        write_lock.insert(target.to_string(), Arc::clone(&client_arc));
    }

    client_arc
}