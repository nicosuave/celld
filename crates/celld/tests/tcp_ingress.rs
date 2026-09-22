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
    host: &'static str,
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
    async fn start(
        root: &Path,
        label: &str,
        bucket: &Path,
        ports: [u16; 3],
    ) -> anyhow::Result<Self> {
        let http = port();
        let internal = port();
        let [tcp, rejected_hook, unavailable_port] = ports;
        // Two real nodes share deployment ports on separate loopback families.
        let host = if label == "owner" {
            "127.0.0.1"
        } else {
            "[::1]"
        };
        let name = format!("tcp-test-{}-{label}", std::process::id());
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
                &format!("{host}:{http}"),
                "--internal-listen",
                &format!("127.0.0.1:{internal}"),
            ])
            .env("CELLD_INTERNAL_DEV_STORE", bucket)
            .env("CELLD_WATCH", root.join(label))
            .env("CELLD_NODE", &name)
            .env("CELLD_IDLE_EVICT_S", "1")
            .env("CELLD_DEPLOY_POLL_S", "1")
            .env("CELLD_REBALANCE_INTERVAL_MS", "0")
            .env("CELLD_SHUTDOWN_TOTAL_MS", "10000")
            .env("RUST_LOG", "info,celld::tcp_ingress=debug")
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(output))
            .kill_on_drop(true)
            .spawn()?;
        let mut node = Self {
            child,
            host,
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
                    .get(format!("http://{host}:{http}/.well-known/celld/health"))
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
            reqwest::get(format!("http://{}:{}/status", self.host, self.http))
                .await?
                .error_for_status()?
                .json()
                .await?,
        )
    }

    async fn connect(&self) -> anyhow::Result<TcpStream> {
        let result = tokio::time::timeout(Duration::from_secs(35), async {
            let mut stream = TcpStream::connect(format!("{}:{}", self.host, self.tcp)).await?;
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
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/tcp-container");
    let project_dir = root.join("project");
    std::fs::create_dir(&project_dir)?;
    for file in ["Dockerfile", "index.js", "server.mjs", "wrangler.jsonc"] {
        std::fs::copy(source.join(file), project_dir.join(file))?;
    }
    let project = project_dir.join("wrangler.jsonc");
    let ports = [port(), port(), port()];
    let mut config: Value = serde_json::from_slice(&std::fs::read(&project)?)?;
    config["tcp"] = json!([
        {"listen_port": ports[0], "class_name": "EchoContainer", "object_name": "primary", "container_port": 7000, "max_connections": 1},
        {"listen_port": ports[1], "class_name": "EchoContainer", "object_name": "primary", "container_port": 7000, "startup_path": "/missing-hook", "connect_timeout_ms": 1000},
        {"listen_port": ports[2], "class_name": "EchoContainer", "object_name": "primary", "container_port": 7002, "connect_timeout_ms": 500}
    ]);
    std::fs::write(&project, serde_json::to_vec(&config)?)?;
    let options = celld::deploy::Options {
        config: Some(project.clone()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: Default::default(),
        local_images: true,
    };
    let built = celld::deploy::build(&options)?;
    celld::deploy::write(&bucket, &built).await?;

    let mut owner = Node::start(root, "owner", &database, ports).await?;
    let before = owner.status().await?; // Place primary on A before B joins.
    assert_eq!(before["connections"], 0);
    assert_eq!(before["running"], false);
    let mut ingress = Node::start(root, "ingress", &database, ports).await?;

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
    assert_closed(TcpStream::connect(format!("{}:{}", ingress.host, ingress.tcp)).await?).await?;
    drop(local);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_closed(TcpStream::connect(format!("{}:{}", ingress.host, ingress.rejected_hook)).await?)
        .await?;
    // A successful hook does not override the readiness deadline when the
    // selected container port never opens. No protocol error bytes escape.
    assert_closed(
        TcpStream::connect(format!("{}:{}", ingress.host, ingress.unavailable_port)).await?,
    )
    .await?;
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

    let mut successor = Node::start(root, "successor", &database, ports).await?;
    let forwarded = successor.connect().await?;
    let saved = owner.status().await?["connections"].as_u64().unwrap();
    // Owner handoff cancels the stream and starts a fresh container on B.
    let (stopped, closed) = tokio::join!(owner.stop(), assert_closed(forwarded));
    stopped?;
    closed?;
    let reconnected = successor.connect().await?;
    assert_eq!(successor.status().await?["connections"], saved + 1);
    drop(reconnected);
    let pid = successor.child.id();
    let occupied = tokio::net::TcpListener::bind("[::1]:0").await?;
    let conflict_port = occupied.local_addr()?.port();
    config["tcp"].as_array_mut().unwrap().push(json!({
        "listen_port": conflict_port, "class_name": "EchoContainer", "object_name": "other", "container_port": 7000
    }));
    std::fs::write(&project, serde_json::to_vec(&config)?)?;
    let conflict = celld::deploy::build(&options)?;
    assert_ne!(
        built.version, conflict.version,
        "TCP config participates in deployment identity"
    );
    assert!(conflict
        .manifest
        .required_features
        .iter()
        .any(|f| f == "tcp-ingress-v1"));
    celld::deploy::write(&bucket, &conflict).await?;
    let reply = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/reload", successor.internal))
        .send()
        .await?;
    assert_eq!(reply.status(), 422);
    assert!(reply.text().await?.contains("bind TCP ingress"));
    drop(successor.connect().await?); // Old deployment remains available.

    config["tcp"] = json!([{
        "listen_port": ports[0], "class_name": "EchoContainer", "object_name": "replacement", "container_port": 7000
    }]);
    std::fs::write(&project, serde_json::to_vec(&config)?)?;
    celld::deploy::write(&bucket, &celld::deploy::build(&options)?).await?;
    reload(&successor).await?;
    drop(successor.connect().await?); // Same listening socket, new named object.
    assert_eq!(successor.status().await?["connections"], saved + 2);
    assert!(
        TcpStream::connect(format!("{}:{}", successor.host, ports[1]))
            .await
            .is_err()
    );

    let new_port = port();
    config["tcp"][0]["listen_port"] = json!(new_port);
    std::fs::write(&project, serde_json::to_vec(&config)?)?;
    celld::deploy::write(&bucket, &celld::deploy::build(&options)?).await?;
    reload(&successor).await?;
    successor.tcp = new_port;
    drop(successor.connect().await?);
    assert!(
        TcpStream::connect(format!("{}:{}", successor.host, ports[0]))
            .await
            .is_err()
    );
    config["tcp"] = json!([]);
    std::fs::write(&project, serde_json::to_vec(&config)?)?;
    celld::deploy::write(&bucket, &celld::deploy::build(&options)?).await?;
    // No /reload nudge: the normal pointer watcher must remove the endpoint.
    tokio::time::timeout(Duration::from_secs(10), async {
        while TcpStream::connect(format!("{}:{}", successor.host, new_port))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("deployment watcher did not remove TCP listener")?;
    assert_eq!(
        successor.child.id(),
        pid,
        "reload must not restart the node"
    );
    successor.stop().await?;
    Ok(())
}

async fn reload(node: &Node) -> anyhow::Result<()> {
    let reply = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/reload", node.internal))
        .send()
        .await?;
    let status = reply.status();
    let body = reply.text().await?;
    anyhow::ensure!(status.is_success(), "reload failed: {body}");
    Ok(())
}

#[test]
fn deployment_tcp_config_validates_targets_ports_and_limits() {
    let classes = vec!["EchoContainer".to_string()];
    let config = json!({"tcp": [{"listen_port": 4543, "class_name": "EchoContainer", "object_name": "primary", "container_port": 7000}]});
    let routes = celld::tcp_config::read(&config, &classes).unwrap();
    assert_eq!(routes[0].startup_path, "/start-tcp");
    assert_eq!(routes[0].connect_timeout_ms, 30000);
    assert_eq!(routes[0].max_connections, 1024);
    assert!(celld::tcp_config::read(&json!({}), &classes)
        .unwrap()
        .is_empty());
    for (key, value) in [
        ("listen_port", json!(0)),
        ("container_port", json!(65536)),
        ("class_name", json!("Missing")),
        ("object_name", json!("x".repeat(1025))),
        ("startup_path", json!("//elsewhere")),
        ("startup_path", json!("/start?x=1")),
        ("connect_timeout_ms", json!(0)),
        ("max_connections", json!(65537)),
        ("unknown", json!(true)),
    ] {
        let mut invalid = config.clone();
        invalid["tcp"][0][key] = value;
        assert!(
            celld::tcp_config::read(&invalid, &classes).is_err(),
            "{invalid}"
        );
    }
    let mut duplicate = config.clone();
    duplicate["tcp"]
        .as_array_mut()
        .unwrap()
        .push(config["tcp"][0].clone());
    assert!(celld::tcp_config::read(&duplicate, &classes).is_err());
    duplicate["tcp"][1]["listen_port"] = json!(4545);
    assert!(celld::tcp_config::read(&duplicate, &classes).is_err());
}
