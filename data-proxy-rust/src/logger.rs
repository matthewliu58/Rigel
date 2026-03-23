use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};
use std::fs::OpenOptions;
use std::path::Path;

/// 初始化日志（对应 Go 的自定义 slog Handler）
pub fn init_logger() -> Result<(), std::io::Error> {
    // 创建日志目录
    let log_dir = "log";
    std::fs::create_dir_all(log_dir)?;

    // 打开日志文件
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(Path::new(log_dir).join("app.log"))?;

    // 配置日志订阅器（包含文件名、行号、函数名）
    tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("tokio=warn".parse().unwrap()),
        )
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_writer(log_file)
                .with_file(true)
                .with_line_number(true)
                .with_function_name(true)
                .with_span_events(FmtSpan::CLOSE),
        )
        .init();

    Ok(())
}