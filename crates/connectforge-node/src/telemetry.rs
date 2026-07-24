use anyhow::Context as _;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Install the global JSON tracing subscriber (idempotent-ish: only the first
/// call wins for the process).
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,connectforge=debug"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .try_init();
}

/// Install the global Prometheus recorder and return its render handle.
///
/// # Errors
/// Returns an error if a global recorder was already installed.
pub fn install_metrics() -> anyhow::Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .context("install prometheus recorder")
}

/// Build (without globally installing) a Prometheus recorder handle — used by
/// tests to avoid clobbering the global recorder.
#[cfg(test)]
#[must_use]
pub fn build_metrics_handle() -> PrometheusHandle {
    PrometheusBuilder::new().build_recorder().handle()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_handle_renders() {
        let handle = build_metrics_handle();
        // Rendering an empty registry yields a (possibly empty) string.
        let _ = handle.render();
    }
}
