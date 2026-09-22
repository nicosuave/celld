// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Deployment-owned, fixed TCP endpoints for named container objects.
use anyhow::ensure;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TcpIngress {
    pub listen_port: u16,
    pub class_name: String,
    pub object_name: String,
    pub container_port: u16,
    #[serde(default = "startup_path")]
    pub startup_path: String,
    #[serde(default = "connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "max_connections")]
    pub max_connections: usize,
}

fn startup_path() -> String {
    "/start-tcp".into()
}
fn connect_timeout() -> u64 {
    30_000
}
fn max_connections() -> usize {
    1024
}

pub fn read(metadata: &serde_json::Value, classes: &[String]) -> anyhow::Result<Vec<TcpIngress>> {
    let routes: Vec<TcpIngress> = match metadata.get("tcp") {
        Some(value) => serde_json::from_value(value.clone())?,
        None => Vec::new(),
    };
    for (index, route) in routes.iter().enumerate() {
        ensure!(
            route.listen_port != 0 && route.container_port != 0,
            "TCP ports must be nonzero"
        );
        ensure!(
            classes.contains(&route.class_name),
            "TCP class {:?} must declare a container",
            route.class_name
        );
        ensure!(
            route.object_name.len() <= 1024,
            "TCP object_name exceeds 1024 bytes"
        );
        ensure!(route.startup_path.starts_with('/') && !route.startup_path.starts_with("//")
            && route.startup_path.len() <= 4096
            && !route.startup_path.chars().any(|c| c.is_control() || matches!(c, '?' | '#' | '\\')),
            "TCP startup_path must be an absolute path without query or fragment (at most 4096 bytes)");
        ensure!(
            (1..=300_000).contains(&route.connect_timeout_ms)
                && (1..=65_536).contains(&route.max_connections),
            "TCP connect_timeout_ms must be 1..=300000 and max_connections 1..=65536"
        );
        ensure!(
            !routes[..index]
                .iter()
                .any(|other| other.listen_port == route.listen_port),
            "duplicate TCP listen_port {}",
            route.listen_port
        );
        ensure!(
            !routes[..index]
                .iter()
                .any(|other| other.class_name == route.class_name
                    && other.object_name == route.object_name
                    && other.container_port == route.container_port
                    && other.startup_path == route.startup_path),
            "duplicate TCP target"
        );
    }
    Ok(routes)
}
