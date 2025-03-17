// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{http::StatusCode, routing::get, Extension, Router, Server};
use prometheus::{Registry, TextEncoder};
use std::net::SocketAddr;
use std::thread;

pub const METRICS_ROUTE: &str = "/metrics";

pub fn start_prometheus_server(address: SocketAddr, registry: &Registry) {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(registry.clone()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    thread::Builder::new()
        .name("prometheus-server".to_string())
        .spawn(move || {
            runtime.block_on(
                async move { Server::bind(&address).serve(app.into_make_service()).await },
            )
        })
        .unwrap();
}

async fn metrics(registry: Extension<Registry>) -> (StatusCode, String) {
    let metrics_families = registry.gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unable to encode metrics: {error}"),
        ),
    }
}
