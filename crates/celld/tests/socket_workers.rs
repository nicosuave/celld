// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Exercise the public binary, real isolates, ownership routing, and HTTP/2.
// These tests own real host processes, files, and deadlines outside the actor.
#![allow(clippy::disallowed_methods)]
use bytes::Bytes;
use futures_util::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::{body::Frame, service::service_fn, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::json;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as PortReservation};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const WORKER: &str = r#"
import { DurableObject } from 'cloudflare:workers';
export class Echo extends DurableObject {
  async fetch() {
    await this.ctx.storage.put('warmed', true);
    return new Response('warmed');
  }
  async connect(socket) {
    const writer = socket.writable.getWriter();
    await writer.write(new TextEncoder().encode(await this.ctx.storage.get('warmed') ? 'warm:' : 'cold:'));
    writer.releaseLock();
    await socket.readable.pipeTo(socket.writable);
  }
}
export default {
  async connect(socket, env) {
    if (env.ROUTE === 'echo') {
      await socket.readable.pipeTo(socket.writable);
      return;
    }
    const target = env.ROUTE === 'service'
      ? env.SERVICE.connect('host:5432')
      : env.DO.getByName('my-server').connect('host:5432');
    await Promise.all([
      socket.readable.pipeTo(target.writable),
      target.readable.pipeTo(socket.writable),
    ]);
  },
  async fetch(request, env) {
    if (new URL(request.url).pathname === '/warm') return env.DO.getByName('my-server').fetch(request);
    if (env.ORIGIN) return fetch(env.ORIGIN, {
      method: 'POST', headers: request.headers, body: request.body, redirect: 'manual',
    });
    if (request.headers.get('content-type') !== 'application/grpc-web+proto') return new Response('incorrect content type', {status: 400});
    const bytes = new Uint8Array(await request.arrayBuffer());
    const trailers = new TextEncoder().encode('grpc-status: 7\r\ngrpc-message: denied\r\n');
    const trailerFrame = new Uint8Array(5 + trailers.length);
    trailerFrame[0] = 128;
    new DataView(trailerFrame.buffer).setUint32(1, trailers.length);
    trailerFrame.set(trailers, 5);
    return new Response(new ReadableStream({ start(controller) {
      controller.enqueue(bytes); controller.enqueue(trailerFrame); controller.close();
    }}), {headers: {'content-type': 'application/grpc-web+proto'}});
  },
};
"#;

fn init_host() {
    static HOST: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    HOST.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        celld::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    });
}

struct Node {
    child: Child,
    public: SocketAddr,
    internal: SocketAddr,
    log: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(&self.log) {
                eprintln!("node {} log:\n{log}", self.public);
            }
        }
        let _ = self.child.start_kill();
    }
}

fn free_port() -> u16 {
    PortReservation::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn deploy(
    directory: &Path,
    store: &celld::bucket::Bucket,
    port: u16,
    route: &str,
    origin: &str,
) {
    let config = json!({
        "name": "socket-test", "main": "index.js", "no_bundle": true,
        "socket_workers": [{"port": port}],
        "durable_objects": {"bindings": [{"name": "DO", "class_name": "Echo"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["Echo"]}],
        "services": [{"binding": "SERVICE", "service": "socket-service"}],
        "vars": {"ROUTE": route, "ORIGIN": origin}
    });
    std::fs::write(directory.join("index.js"), WORKER).unwrap();
    std::fs::write(
        directory.join("wrangler.jsonc"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let built = celld::deploy::build(&celld::deploy::Options {
        config: Some(directory.join("wrangler.jsonc")),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: BTreeMap::new(),
        local_images: true,
    })
    .unwrap();
    celld::deploy::write(store, &built).await.unwrap();
}

async fn deploy_service(directory: &Path, store: &celld::bucket::Bucket) {
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(
        directory.join("index.js"),
        r#"
export default { async connect(socket) {
  const writer = socket.writable.getWriter();
  await writer.write(new TextEncoder().encode('service:'));
  writer.releaseLock();
  await socket.readable.pipeTo(socket.writable);
}};
"#,
    )
    .unwrap();
    std::fs::write(
        directory.join("wrangler.jsonc"),
        r#"{"name":"socket-service","main":"index.js","no_bundle":true}"#,
    )
    .unwrap();
    let built = celld::deploy::build(&celld::deploy::Options {
        config: Some(directory.join("wrangler.jsonc")),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: BTreeMap::new(),
        local_images: true,
    })
    .unwrap();
    celld::deploy::write(store, &built).await.unwrap();
}

async fn start_node(directory: &Path, database: &Path, ip: &str, name: &str) -> Node {
    let public: SocketAddr = format!("{ip}:{}", free_port()).parse().unwrap();
    let internal: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let log = directory.join(format!("{name}.log"));
    let output = std::fs::File::create(&log).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
    for key in [
        "CELLD_BUCKET",
        "CELLD_CLOUD",
        "CELLD_ADDR",
        "CELLD_INTERNAL_ADDR",
        "CELLD_ADVERTISE",
        "S3_ENDPOINT",
        "CELLD_TEST_SCRIPT",
        "CELLD_UNSAFE_PUBLIC_ADVERTISE",
    ] {
        command.env_remove(key);
    }
    let child = command
        .args([
            "--no-control-plane",
            "--bucket",
            "celld-dev",
            "--listen",
            &public.to_string(),
            "--internal-listen",
            &internal.to_string(),
        ])
        .env("CELLD_INTERNAL_DEV_STORE", database)
        .env("CELLD_NODE", name)
        .env("CELLD_WATCH", directory.join(name))
        .env("CELLD_DURABILITY", "bucket")
        .env("CELLD_DEPLOY_POLL_S", "3600")
        .env("CELLD_SHUTDOWN_TOTAL_MS", "5000")
        .env(
            "RUST_LOG",
            "warn,celld::socket=debug,celld::runtime=debug,celld::js=debug",
        )
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut node = Node {
        child,
        public,
        internal,
        log,
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let ready = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node.child.try_wait().unwrap().is_some() {
                break false;
            }
            if client
                .get(format!("http://{public}/.well-known/celld/health"))
                .send()
                .await
                .is_ok()
            {
                break true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        ready,
        "node did not start: {}",
        std::fs::read_to_string(&node.log).unwrap()
    );
    node
}

async fn reload(node: &Node) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{}/reload", node.internal))
        .send()
        .await
        .unwrap()
}

async fn exchange(address: SocketAddr, prefix: &[u8]) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    if !prefix.is_empty() {
        let mut actual = vec![0; prefix.len()];
        stream.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, prefix);
    }
    // Payload exceeds the relay buffer, contains arbitrary binary bytes, and
    // must be echoed before the client shuts down its write half.
    let bytes: Vec<_> = (0..200_000).map(|value| value as u8).collect();
    let (mut read, mut write) = stream.split();
    let writing = async {
        write.write_all(&bytes).await.unwrap();
        write.shutdown().await.unwrap();
    };
    let reading = async {
        let mut actual = Vec::new();
        read.read_to_end(&mut actual).await.unwrap();
        assert_eq!(
            actual.len(),
            bytes.len(),
            "truncated echo at {address}, prefix {prefix:?}"
        );
        assert!(
            actual == bytes,
            "corrupt echo at {address}, prefix {prefix:?}"
        );
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(writing, reading);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_worker_service_and_remote_durable_object_with_listener_reload() {
    init_host();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("objects.sqlite3");
    let store = celld::dev::open_local_bucket(&database).unwrap();
    celld::wake_format::ensure_ready(&store).await.unwrap();
    deploy_service(&directory.path().join("service"), &store).await;
    let port = free_port();
    deploy(directory.path(), &store, port, "echo", "").await;
    let mut first = start_node(directory.path(), &database, "127.0.0.1", "socket-node-a").await;
    exchange(format!("127.0.0.1:{port}").parse().unwrap(), b"").await;
    deploy(directory.path(), &store, port, "service", "").await;
    assert!(reload(&first).await.status().is_success());
    exchange(format!("127.0.0.1:{port}").parse().unwrap(), b"service:").await;
    deploy(directory.path(), &store, port, "do", "").await;
    assert!(reload(&first).await.status().is_success());
    // IPv6 loopback avoids requiring a second IPv4 loopback alias on macOS.
    let second = start_node(directory.path(), &database, "[::1]", "socket-node-b").await;
    let warmed = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{}/warm", second.public))
        .send()
        .await
        .unwrap();
    assert_eq!(warmed.text().await.unwrap(), "warmed");
    exchange(format!("127.0.0.1:{port}").parse().unwrap(), b"warm:").await;
    // Peer-only sockets reject requests without the fleet signature.
    assert_eq!(
        reqwest::Client::new()
            .get(format!("http://{}/peer/socket", second.internal))
            .header("upgrade", "celld-socket-v1")
            .header("connection", "upgrade")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    let occupied = PortReservation::bind("127.0.0.1:0").unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    deploy(directory.path(), &store, occupied_port, "echo", "").await;
    assert!(!reload(&first).await.status().is_success());
    exchange(format!("127.0.0.1:{port}").parse().unwrap(), b"warm:").await;
    let next_port = free_port();
    deploy(directory.path(), &store, next_port, "echo", "").await;
    assert!(reload(&first).await.status().is_success());
    exchange(format!("127.0.0.1:{next_port}").parse().unwrap(), b"").await;
    assert!(TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .is_err());
    let mut active = TcpStream::connect(format!("127.0.0.1:{next_port}"))
        .await
        .unwrap();
    let shutdown = reqwest::Client::new()
        .post(format!(
            "http://{}/shutdown?handoff=preserve",
            first.internal
        ))
        .send()
        .await
        .unwrap();
    assert!(shutdown.status().is_success());
    let mut byte = [0];
    let closed = tokio::time::timeout(Duration::from_secs(5), active.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0) | Err(_)));
    tokio::time::timeout(Duration::from_secs(10), first.child.wait())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_grpc_through_worker_and_outbound_http2_translation() {
    init_host();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("objects.sqlite3");
    let store = celld::dev::open_local_bucket(&database).unwrap();
    celld::wake_format::ensure_ready(&store).await.unwrap();
    deploy_service(&directory.path().join("service"), &store).await;
    deploy(directory.path(), &store, free_port(), "echo", "").await;
    let node = start_node(directory.path(), &database, "127.0.0.1", "grpc-node").await;
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let data = Bytes::from_static(&[0, 0, 0, 0, 3, 1, 2, 3]);
    let url = format!("http://{}/hello.Greeter/SayHello", node.public);
    let reply = client
        .post(&url)
        .header("content-type", "application/grpc+proto")
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(reply.version(), hyper::Version::HTTP_2);
    assert_eq!(reply.headers()["content-type"], "application/grpc+proto");
    let reply: Response<reqwest::Body> = reply.into();
    let collected = reply.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["grpc-status"], "7");
    assert_eq!(collected.to_bytes(), data);

    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_address = origin.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = origin.accept().await.unwrap();
        let service = service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                assert_eq!(request.version(), hyper::Version::HTTP_2);
                assert_eq!(request.headers()["content-type"], "application/grpc+proto");
                let data = request.into_body().collect().await.unwrap().to_bytes();
                let mut trailers = hyper::HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                Ok::<_, Infallible>(
                    Response::builder()
                        .header("content-type", "application/grpc+proto")
                        .body(StreamBody::new(stream::iter(vec![
                            Ok::<_, Infallible>(Frame::data(data)),
                            Ok(Frame::trailers(trailers)),
                        ])))
                        .unwrap(),
                )
            },
        );
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await
            .unwrap();
    });
    deploy(
        directory.path(),
        &store,
        free_port(),
        "echo",
        &format!("http://{origin_address}/rpc"),
    )
    .await;
    assert!(reload(&node).await.status().is_success());
    let reply = client
        .post(url)
        .header("content-type", "application/grpc+proto")
        .body(data.clone())
        .send()
        .await
        .unwrap();
    let reply: Response<reqwest::Body> = reply.into();
    let collected = tokio::time::timeout(Duration::from_secs(5), reply.into_body().collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(collected.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(collected.to_bytes(), data);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a local Docker daemon and builds the container/fence images"]
async fn tcp_worker_forwards_through_durable_object_to_container() {
    init_host();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("objects.sqlite3");
    let store = celld::dev::open_local_bucket(&database).unwrap();
    celld::wake_format::ensure_ready(&store).await.unwrap();
    std::fs::write(
        directory.path().join("server.cjs"),
        r#"
const net = require('node:net');
net.createServer(socket => {
  socket.write('container:');
  socket.pipe(socket);
}).listen(8080, '0.0.0.0');
"#,
    )
    .unwrap();
    std::fs::write(directory.path().join("Dockerfile"), "FROM node:24-slim\nCOPY server.cjs /server.cjs\nEXPOSE 8080\nCMD [\"node\", \"/server.cjs\"]\n").unwrap();
    std::fs::write(
        directory.path().join("index.js"),
        r#"
import { DurableObject } from 'cloudflare:workers';
export class SocketContainer extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.ctx.container.start(); }
  async fetch() {
    // Keep the startup event alive and wait for the program's listening port.
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        const socket = this.ctx.container.getTcpPort(8080).connect('10.0.0.1:8080');
        await socket.opened;
        await socket.close();
        return new Response('ready');
      } catch {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    return new Response('container not ready', {status: 503});
  }
  async connect(socket) {
    const containerSocket = this.ctx.container.getTcpPort(8080).connect('10.0.0.1:8080');
    await containerSocket.opened;
    await Promise.all([
      socket.readable.pipeTo(containerSocket.writable),
      containerSocket.readable.pipeTo(socket.writable),
    ]);
  }
}
export default {
  async fetch(request, env) { return env.DO.getByName('my-server').fetch(request); },
  async connect(socket, env) {
    const target = env.DO.getByName('my-server').connect('host:5432');
    await Promise.all([
      socket.readable.pipeTo(target.writable),
      target.readable.pipeTo(socket.writable),
    ]);
  },
};
"#,
    )
    .unwrap();
    let port = free_port();
    let config = json!({
        "name": "socket-container-test", "main": "index.js", "no_bundle": true,
        "socket_workers": [{"port": port}],
        "durable_objects": {"bindings": [{"name": "DO", "class_name": "SocketContainer"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["SocketContainer"]}],
        "containers": [{"class_name": "SocketContainer", "image": "./Dockerfile", "instance_type": "basic"}],
    });
    let config_path = directory.path().join("wrangler.jsonc");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let built = celld::deploy::build(&celld::deploy::Options {
        config: Some(config_path),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: BTreeMap::new(),
        local_images: true,
    })
    .unwrap();
    celld::deploy::write(&store, &built).await.unwrap();
    let mut node = start_node(
        directory.path(),
        &database,
        "127.0.0.1",
        "socket-container-node",
    )
    .await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let ready = client
        .get(format!("http://{}/warm", node.public))
        .send()
        .await
        .unwrap();
    assert!(
        ready.status().is_success(),
        "container startup failed: {}",
        std::fs::read_to_string(&node.log).unwrap()
    );
    exchange(format!("127.0.0.1:{port}").parse().unwrap(), b"container:").await;
    client
        .post(format!("http://{}/shutdown", node.internal))
        .send()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), node.child.wait())
        .await
        .unwrap()
        .unwrap();
}
