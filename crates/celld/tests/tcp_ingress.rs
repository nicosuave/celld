// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Real processes, peer transport, V8 and Docker, with the transactional local
//! object store as the shared bucket fixture. No production cloud credentials.
//! Run with Docker and esbuild available:
//! cargo test -p celld --test tcp_ingress -- --ignored --nocapture

#![cfg(unix)]
#![allow(clippy::disallowed_methods)] // Process integration tests use host I/O.

use anyhow::Context;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

struct Node {
    child: Child,
    http: u16,
    internal: u16,
    tcp: u16,
    rejected_hook: u16,
    unavailable_port: u16,
    name: String,
    log: PathBuf,
}

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Node {
    async fn start(root: &Path, label: &str, bucket: &Path) -> anyhow::Result<Self> {
        let http = port();
        let internal = port();
        let tcp = port();
        let rejected_hook = port();
        let unavailable_port = port();
        let name = format!("tcp-test-{}-{label}", std::process::id());
        let config = root.join(format!("{label}.json"));
        let target = |startup_path: &str| {
            json!({
                "script": "tcp-container", "class_name": "EchoContainer",
                "object_name": "primary", "port": 7000, "startup_path": startup_path
            })
        };
        let mut unavailable = target("/start-tcp");
        unavailable["port"] = json!(7002);
        std::fs::write(
            &config,
            serde_json::to_vec(&json!([
                { "listen": format!("127.0.0.1:{tcp}"), "target": target("/start-tcp"), "max_connections": 1 },
            { "listen": format!("127.0.0.1:{rejected_hook}"), "target": target("/missing-hook"), "connect_timeout_ms": 1000 },
            { "listen": format!("127.0.0.1:{unavailable_port}"), "target": unavailable, "connect_timeout_ms": 500, "max_connections": 1 }
            ]))?,
        )?;
        let log = root.join(format!("{label}.log"));
        let output = std::fs::File::create(&log)?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        // These children belong to this fixture, never the caller's fleet.
        for variable in [
            "CELLD_BUCKET",
            "CELLD_TEST_BUCKET",
            "CELLD_ADVERTISE",
            "S3_ENDPOINT",
            "CELLD_CLOUD",
            "CELLD_TCP_INGRESS_CONFIG",
            "CELLD_ADDR",
            "CELLD_INTERNAL_ADDR",
        ] {
            command.env_remove(variable);
        }
        let child = command
            .args([
                "--no-control-plane",
                "--bucket",
                "celld-dev",
                "--listen",
                &format!("127.0.0.1:{http}"),
                "--internal-listen",
                &format!("127.0.0.1:{internal}"),
                "--tcp-ingress",
                config.to_str().unwrap(),
            ])
            .env("CELLD_INTERNAL_DEV_STORE", bucket)
            .env("CELLD_WATCH", root.join(label))
            .env("CELLD_NODE", &name)
            .env("CELLD_IDLE_EVICT_S", "1")
            .env("CELLD_REBALANCE_INTERVAL_MS", "0")
            .env("CELLD_SHUTDOWN_TOTAL_MS", "10000")
            .env("RUST_LOG", "info,celld::tcp_ingress=debug")
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(output))
            .kill_on_drop(true)
            .spawn()?;
        let mut node = Self {
            child,
            http,
            internal,
            tcp,
            rejected_hook,
            unavailable_port,
            name,
            log,
        };
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                anyhow::ensure!(
                    node.child.try_wait()?.is_none(),
                    "node exited: {}",
                    std::fs::read_to_string(&node.log)?
                );
                if client
                    .get(format!("http://127.0.0.1:{http}/.well-known/celld/health"))
                    .send()
                    .await
                    .is_ok_and(|reply| reply.status().is_success())
                {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("node readiness timed out")??;
        Ok(node)
    }

    async fn status(&self) -> anyhow::Result<Value> {
        Ok(
            reqwest::get(format!("http://127.0.0.1:{}/status", self.http))
                .await?
                .error_for_status()?
                .json()
                .await?,
        )
    }

    async fn connect(&self) -> anyhow::Result<TcpStream> {
        let result = tokio::time::timeout(Duration::from_secs(35), async {
            let mut stream = TcpStream::connect(("127.0.0.1", self.tcp)).await?;
            let mut greeting = [0; 6];
            stream.read_exact(&mut greeting).await?;
            anyhow::ensure!(&greeting == b"READY\n", "wrong server-first greeting");
            Ok(stream)
        })
        .await
        .context("TCP greeting timed out")?;
        if result.is_err() {
            // Let the child's asynchronous log writer publish the failure
            // before fixture cleanup terminates the process.
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        result
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(pid) = self.child.id() {
            anyhow::ensure!(
                Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .await?
                    .success(),
                "send SIGTERM"
            );
        }
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await??;
        anyhow::ensure!(
            status.success(),
            "node shutdown failed: {}",
            std::fs::read_to_string(&self.log)?
        );
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        if std::thread::panicking() {
            eprintln!(
                "{}:\n{}",
                self.name,
                std::fs::read_to_string(&self.log).unwrap_or_default()
            );
        }
        // Only containers labelled with this test process's exact node name.
        if let Ok(output) = std::process::Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("label=celld.node={}", self.name),
            ])
            .output()
        {
            for id in String::from_utf8_lossy(&output.stdout).split_whitespace() {
                if let Ok(logs) = std::process::Command::new("docker")
                    .args(["logs", id])
                    .output()
                {
                    let _ = std::fs::write(
                        self.log.with_extension("container.log"),
                        [logs.stdout, logs.stderr].concat(),
                    );
                }
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", id])
                    .output();
            }
        }
    }
}

async fn assert_closed(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await?;
    match result {
        Ok(0) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        result => anyhow::bail!("expected closed connection, got {result:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker/Podman and esbuild; builds the TCP container example"]
async fn fleet_tcp_ingress_routes_pins_drains_and_reconnects() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let result = exercise_fleet(root.path()).await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            Err(error.context(format!("TCP fixture retained at {}", root.keep().display())))
        }
    }
}

async fn exercise_fleet(root: &Path) -> anyhow::Result<()> {
    let database = root.join("objects.sqlite3");
    let bucket = celld::dev::open_local_bucket(&database)?;
    celld::fleet::validate_bucket(&bucket).await?;
    celld::wake_format::ensure_ready(&bucket).await?;
    let project =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/tcp-container/wrangler.jsonc");
    let built = celld::deploy::build(&celld::deploy::Options {
        config: Some(project),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: Default::default(),
        local_images: true,
    })?;
    celld::deploy::write(&bucket, &built).await?;

    let mut owner = Node::start(root, "owner", &database).await?;
    let before = owner.status().await?; // Place primary on A before B joins.
    assert_eq!(before["connections"], 0);
    assert_eq!(before["running"], false);
    let mut ingress = Node::start(root, "ingress", &database).await?;

    // Cold container start through B must run A's named object, not a second
    // object. A long-lived TCP connection must survive ordinary idle eviction.
    let mut stream = ingress
        .connect()
        .await
        .context("cold connection through peer")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    stream.write_all(b"still alive").await?;
    let mut echoed = [0; 11];
    stream.read_exact(&mut echoed).await?;
    assert_eq!(&echoed, b"still alive");
    let payload: Vec<u8> = (0..262_144).map(|i| (i % 251) as u8).collect();
    let (mut read, mut write) = stream.into_split();
    let sent = payload.clone();
    let writer = tokio::spawn(async move {
        write.write_all(&sent).await?;
        write.shutdown().await
    });
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), read.read_to_end(&mut received)).await??;
    writer.await??;
    assert_eq!(&received[..payload.len()], payload.as_slice());
    assert_eq!(&received[payload.len()..], b"EOF\n");
    let after = owner.status().await?;
    assert_eq!(after["id"], before["id"]);
    assert_eq!(after["connections"], 1);
    assert_eq!(ingress.status().await?["id"], before["id"]);

    // Both local and peer arrivals consume the owner's cap, so using a
    // different ingress node cannot bypass it.
    let local = owner.connect().await?;
    assert_closed(TcpStream::connect(("127.0.0.1", ingress.tcp)).await?).await?;
    drop(local);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_closed(TcpStream::connect(("127.0.0.1", ingress.rejected_hook)).await?).await?;
    // A successful hook does not override the readiness deadline when the
    // selected container port never opens. No protocol error bytes escape.
    assert_closed(TcpStream::connect(("127.0.0.1", ingress.unavailable_port)).await?).await?;
    let denied = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/peer/tcp", owner.internal))
        .header("connection", "upgrade")
        .header("upgrade", "celld-container-tcp-v1")
        .body("{}")
        .send()
        .await?;
    assert_eq!(denied.status(), 401);

    // A fleet signature is necessary but cannot turn the endpoint into an
    // arbitrary container-port dialer. The signed body also binds the target.
    let auth = celld::peer_auth::PeerAuth::new(
        celld::peer_auth::load_or_create(&bucket).await?,
        "tcp-test-client",
    )?;
    let client = reqwest::Client::new();
    let endpoint = format!("http://127.0.0.1:{}/peer/tcp", owner.internal);
    let unconfigured = serde_json::to_vec(&json!({
        "target": {
            "script": "tcp-container", "class_name": "EchoContainer",
            "object_name": "primary", "port": 7003, "startup_path": "/start-tcp"
        },
        "capacity_handoff": false
    }))?;
    let signed = |target: &str| auth.signed_headers("POST", "/peer/tcp", &unconfigured, target);
    let headers = signed(&owner.name)?;
    let send = |headers, body: Vec<u8>| {
        client
            .post(&endpoint)
            .headers(headers)
            .header("connection", "upgrade")
            .header("upgrade", "celld-container-tcp-v1")
            .body(body)
            .send()
    };
    assert_eq!(
        send(headers.clone(), unconfigured.clone()).await?.status(),
        403
    );
    assert_eq!(send(headers, unconfigured.clone()).await?.status(), 409); // nonce replay
    assert_eq!(
        send(signed(&owner.name)?, b"{}".to_vec()).await?.status(),
        401
    );
    let stale = send(signed("previous-owner-session")?, unconfigured).await?;
    assert_eq!(stale.status(), 409);
    assert_eq!(
        stale.headers().get("x-cells-route-error").unwrap(),
        "stale-owner"
    );

    let forwarded = ingress.connect().await?;
    let (stopped, closed) = tokio::join!(ingress.stop(), assert_closed(forwarded));
    stopped?;
    closed?;

    let mut successor = Node::start(root, "successor", &database).await?;
    let forwarded = successor.connect().await?;
    let saved = owner.status().await?["connections"].as_u64().unwrap();
    // Owner handoff cancels the stream and starts a fresh container on B.
    let (stopped, closed) = tokio::join!(owner.stop(), assert_closed(forwarded));
    stopped?;
    closed?;
    let reconnected = successor.connect().await?;
    assert_eq!(successor.status().await?["connections"], saved + 1);
    drop(reconnected);
    successor.stop().await?;
    Ok(())
}
