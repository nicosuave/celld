//! Outbound TCP host ops for `cloudflare:sockets` `connect()`.
//!
//! A socket is a split tokio stream in a process-global registry: the
//! read and write halves live behind separate async mutexes so a
//! pending read never blocks a write. `startTls()` takes both halves
//! back, reunites them, runs the rustls handshake, and registers the
//! upgraded stream under a new id — the old id is gone, which is what
//! makes the old Socket's streams fail instead of leaking plaintext.
//!
//! Every op is an `asyncrt` promise. The ops do their own tokio I/O
//! instead of going through a connector task: unlike an outbound
//! WebSocket, a TCP socket never outlives the event that created it
//! (the request context frees it at retirement), so nothing has to
//! survive the request driver.

use super::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

pub trait TcpStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> TcpStream for T {}

pub type TcpConnection = Box<dyn TcpStream>;
type Duplex = TcpConnection;
type ReadHalf = tokio::io::ReadHalf<Duplex>;
type WriteHalf = tokio::io::WriteHalf<Duplex>;

struct TcpSocket {
    read: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
    write: Arc<tokio::sync::Mutex<Option<WriteHalf>>>,
    opened: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    closed: tokio_util::sync::CancellationToken,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

fn registry() -> &'static Mutex<HashMap<u64, TcpSocket>> {
    static SOCKETS: OnceLock<Mutex<HashMap<u64, TcpSocket>>> = OnceLock::new();
    SOCKETS.get_or_init(Default::default)
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Drop one socket's registry entry, which closes the connection. The
/// request context calls this for every socket the event opened, so an
/// abandoned socket cannot outlive its event. Idempotent.
pub(super) fn free(id: u64) {
    if let Some(socket) = registry().lock().unwrap().remove(&id) {
        socket.closed.cancel();
        if let Some(cancel) = socket.cancel {
            let _ = cancel.send(());
        }
        if let Some(opened) = socket.opened {
            let _ = opened.send(Err(anyhow!(
                "connect handler did not accept the connection"
            )));
        }
    }
}

fn insert(id: u64, stream: Duplex) {
    let (read, write) = tokio::io::split(stream);
    registry().lock().unwrap().insert(
        id,
        TcpSocket {
            read: Arc::new(tokio::sync::Mutex::new(Some(read))),
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            opened: None,
            closed: tokio_util::sync::CancellationToken::new(),
            cancel: None,
        },
    );
}

pub(super) fn accept_connection(connection: RuntimeConnect) -> u64 {
    let id = next_id();
    current_context().tcp_sockets.lock().unwrap().push(id);
    insert(id, connection.stream);
    registry().lock().unwrap().get_mut(&id).unwrap().opened = Some(connection.opened);
    id
}

pub(super) fn op_tcp_accept(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    if let Some(socket) = registry().lock().unwrap().get_mut(&id) {
        if let Some(opened) = socket.opened.take() {
            let _ = opened.send(Ok(()));
        }
    }
}

/// Mozilla's roots, the same choice `ws_client` makes and for the same
/// reason: a downloaded celld must reach TLS hosts on a machine with no
/// `/etc/ssl/certs`. Test TLS servers use a root injected through the
/// gated seam below.
#[cfg(celld_internal_tests)]
fn test_root() -> &'static Mutex<Option<Vec<u8>>> {
    static ROOT: OnceLock<Mutex<Option<Vec<u8>>>> = OnceLock::new();
    ROOT.get_or_init(Default::default)
}

/// Trust one extra DER root for outbound TLS, so test servers with
/// self-signed certificates verify. No compatibility guarantee.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn test_extra_tls_root(der: Vec<u8>) {
    *test_root().lock().unwrap() = Some(der);
}

// Rebuilt per handshake only under the internal-tests cfg, where the
// injected root must be visible; production caches the connector.
#[cfg(celld_internal_tests)]
fn tls_connector() -> tokio_rustls::TlsConnector {
    build_tls_connector(test_root().lock().unwrap().clone())
}

#[cfg(not(celld_internal_tests))]
fn tls_connector() -> tokio_rustls::TlsConnector {
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| build_tls_connector(None)).clone()
}

fn build_tls_connector(extra_root: Option<Vec<u8>>) -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Some(der) = extra_root {
        let _ = roots.add(rustls::pki_types::CertificateDer::from(der));
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn handshake(stream: Duplex, host: String) -> Result<Duplex, String> {
    let name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|error| format!("invalid TLS server name: {error}"))?;
    let tls = tls_connector()
        .connect(name, stream)
        .await
        .map_err(|error| format!("TLS handshake failed: {error}"))?;
    Ok(Box::new(tls))
}

#[derive(serde::Deserialize)]
struct ConnectArgs {
    hostname: String,
    port: u16,
    secure: bool,
    #[serde(default)]
    expected_host: Option<String>,
}

/// Binding destinations retain their routing authority; the address describes
/// the connection and never becomes an unmediated network destination.
pub(super) fn op_tcp_binding_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    if let Err(error) = context.charge_subrequest() {
        return loader_throw(scope, &error);
    }
    let kind = args.get(0).to_rust_string_lossy(scope);
    let target_name = args.get(1).to_rust_string_lossy(scope);
    let name = args
        .get(2)
        .is_string()
        .then(|| args.get(2).to_rust_string_lossy(scope));
    let request: ConnectArgs = match serde_json::from_str(&args.get(3).to_rust_string_lossy(scope))
    {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("connect(): {error}")),
    };
    let props = view_bytes(args.get(4)).unwrap_or_default();
    let target = if kind == "cell" {
        crate::host_channels::SocketTarget::Cell {
            order: Some(enter_call_order(context.clone(), &target_name)),
            parent: current_trace_context(scope),
            scope: target_name,
            name,
        }
    } else {
        crate::host_channels::SocketTarget::Service {
            script: target_name,
            entrypoint: name.map(|name| crate::WorkerFetchEntrypoint { name, props }),
        }
    };
    let id = next_id();
    context.tcp_sockets.lock().unwrap().push(id);
    let gate = egress_gate_request(&context, celld_logic::Channel::Tcp);
    let generation = current_generation(scope);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (opened, ready) = tokio::sync::oneshot::channel();
        let (cancel_sender, cancel) = tokio::sync::oneshot::channel();
        let address = format!("{}:{}", request.hostname, request.port);
        crate::host_channels::socket_call_tx()
            .ok_or("no socket binding channel")?
            .send(crate::host_channels::SocketCallReq {
                cancel: Some(cancel),
                generation,
                target,
                address: address.clone(),
                stream: Box::new(server),
                opened,
            })
            .map_err(|_| "socket binding channel closed")?;
        ready
            .await
            .map_err(|_| "socket destination dropped before accepting")?
            .map_err(|error| error.to_string())?;
        let mut stream: Duplex = Box::new(client);
        if request.secure {
            stream = handshake(stream, request.hostname).await?;
        }
        insert(id, stream);
        registry().lock().unwrap().get_mut(&id).unwrap().cancel = Some(cancel_sender);
        Ok::<String, String>(serde_json::json!({"id": id, "remoteAddress": address}).to_string())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    match &actor_runtime_state(scope).egress {
        EgressPolicy::Allow => {}
        EgressPolicy::Deny => {
            return loader_throw(
                scope,
                "This worker is not permitted to access the internet via global functions.",
            );
        }
        // A Fetcher is still an authority boundary for sockets. celld's
        // service protocol has no bidirectional CONNECT tunnel, so refusing is
        // required: a direct TcpStream here would silently bypass the broker.
        EgressPolicy::Broker(_) => {
            return loader_throw(
                scope,
                "A globalOutbound Fetcher cannot broker connect() in celld.",
            );
        }
    }
    let raw = args.get(0).to_rust_string_lossy(scope);
    let request: ConnectArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("connect(): {error}")),
    };
    // The id exists before the connection does, so the request context
    // can own the socket even if the event ends mid-connect.
    let id = next_id();
    current_context().tcp_sockets.lock().unwrap().push(id);
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let stream = tokio::net::TcpStream::connect((request.hostname.as_str(), request.port))
            .await
            .map_err(|error| {
                format!(
                    "connection to {}:{} failed: {error}",
                    request.hostname, request.port
                )
            })?;
        let remote = stream.peer_addr().map(|a| a.to_string()).ok();
        let local = stream.local_addr().map(|a| a.to_string()).ok();
        let mut stream: Duplex = Box::new(stream);
        if request.secure {
            let host = request.expected_host.unwrap_or(request.hostname);
            stream = handshake(stream, host).await?;
        }
        insert(id, stream);
        Ok::<String, String>(
            serde_json::json!({
                "id": id,
                "remoteAddress": remote,
                "localAddress": local,
            })
            .to_string(),
        )
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|s| (s.read.clone(), s.closed.clone()));
    let async_id = asyncrt::enqueue(async move {
        let (half, closed) = half.ok_or("socket is closed")?;
        let operation = async {
            let mut guard = half.lock().await;
            let read = guard.as_mut().ok_or("socket is closed")?;
            let mut buffer = vec![0u8; 16 * 1024];
            let n = read
                .read(&mut buffer)
                .await
                .map_err(|error| format!("read failed: {error}"))?;
            buffer.truncate(n);
            Ok::<Vec<u8>, String>(buffer)
        };
        crate::asyncrt::select_biased! {
            "closing a socket cancels pending I/O before releasing its stream";
            _ = closed.cancelled() => Err("socket is closed".to_string()),
            result = operation => result,
        }
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let Some(bytes) = view_bytes(args.get(1)) else {
        return loader_throw(scope, "socket write needs bytes");
    };
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|s| (s.write.clone(), s.closed.clone()));
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let (half, closed) = half.ok_or("socket is closed")?;
        let operation = async {
            let mut guard = half.lock().await;
            let write = guard.as_mut().ok_or("socket is closed")?;
            write
                .write_all(&bytes)
                .await
                .map_err(|error| format!("write failed: {error}"))?;
            write
                .flush()
                .await
                .map_err(|error| format!("flush failed: {error}"))?;
            Ok::<String, String>(String::new())
        };
        crate::asyncrt::select_biased! {
            "closing a socket cancels pending I/O before releasing its stream";
            _ = closed.cancelled() => Err("socket is closed".to_string()),
            result = operation => result,
        }
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_shutdown(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|s| (s.write.clone(), s.closed.clone()));
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let (half, closed) = half.ok_or("socket is closed")?;
        let operation = async {
            let mut guard = half.lock().await;
            let write = guard.as_mut().ok_or("socket is closed")?;
            write
                .shutdown()
                .await
                .map_err(|error| format!("shutdown failed: {error}"))?;
            Ok::<String, String>(String::new())
        };
        crate::asyncrt::select_biased! {
            "closing a socket cancels pending I/O before releasing its stream";
            _ = closed.cancelled() => Err("socket is closed".to_string()),
            result = operation => result,
        }
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    free(id);
}

#[derive(serde::Deserialize)]
struct StartTlsArgs {
    id: u64,
    host: String,
}

pub(super) fn op_tcp_starttls(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let raw = args.get(0).to_rust_string_lossy(scope);
    let request: StartTlsArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("startTls(): {error}")),
    };
    let new_id = next_id();
    current_context().tcp_sockets.lock().unwrap().push(new_id);
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        // The plaintext socket remains registered while the gate waits so the
        // event cleanup can close it. Consume it only when the TLS handshake can
        // start, and make every later operation on the old id fail.
        let taken = registry().lock().unwrap().remove(&request.id);
        let socket = taken.ok_or("socket is closed")?;
        // Upgrading transfers the binding's cancellation authority along with
        // the stream; dropping it here would cancel the destination handler.
        let cancel = socket.cancel;
        let read = socket.read.lock().await.take().ok_or("socket is closed")?;
        let write = socket.write.lock().await.take().ok_or("socket is closed")?;
        let stream = read.unsplit(write);
        let stream = handshake(stream, request.host).await?;
        insert(new_id, stream);
        registry().lock().unwrap().get_mut(&new_id).unwrap().cancel = cancel;
        Ok::<String, String>(serde_json::json!({ "id": new_id }).to_string())
    });
    rv.set(promise_for(scope, async_id));
}
