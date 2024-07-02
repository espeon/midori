// Blatantly stolen from https://seaforge.org/xacrimon/no-no/src/branch/main/backend/src/specials.rs

use std::{future::Future, pin::Pin};

use axum::{http::StatusCode, response::IntoResponse};
use futures::future;
use tokio::signal;
use tracing::info;

pub async fn handler_404() -> impl IntoResponse {
    // classic go 404
    (StatusCode::NOT_FOUND, "404 page not found")
}

pub async fn do_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("starting graceful shutdown...");
}

pub struct Closer {
    closers: Vec<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

impl Closer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            closers: Vec::new(),
        }
    }

    pub fn add<F>(&mut self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.closers.push(Box::pin(f));
    }

    pub async fn close(self) {
        future::join_all(self.closers).await;
    }
}