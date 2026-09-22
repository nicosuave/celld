// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Fixed TCP listeners address named container objects. Only establishment
//! speaks HTTP between nodes; after the authenticated 101 both directions are
//! opaque bytes. The owner's ordinary request guard pins the cell until EOF.

use super::*;
use celld::container::{CellContainer, ContainerEngine};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

const PEER_PATH: &str = "/peer/tcp";
const PROTOCOL: &str = "celld-container-tcp-v1";
const MAX_HANDSHAKE_BYTES: usize = 16 * 1024;
static STATE: OnceLock<Mutex<State>> = OnceLock::new();
static LISTEN_IP: OnceLock<std::net::IpAddr> = OnceLock::new();

#[derive(Default)]
struct State {
    stopped: bool,
    listeners: Vec<Listener>,
    servers: JoinSet<()>,
    mappings: Vec<Arc<Mapping>>,
    shutdown: Option<watch::Sender<bool>>,
}

pub(super) fn initialize(ip: std::net::IpAddr) {
    LISTEN_IP.set(ip).expect("TCP ingress initialized once");
    STATE
        .set(Mutex::new(State::default()))
        .unwrap_or_else(|_| panic!("TCP ingress initialized once"));
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Target {
    script: String,
    class_name: String,
    object_name: String,
    port: u16,
    /// Invoked as POST on the object before every connection. It must return
    /// an empty 204 after starting the container, without a streaming body.
    startup_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MappingConfig {
    listen: SocketAddr,
    target: Target,
    connect_timeout_ms: u64,
    max_connections: usize,
}

struct Mapping {
    config: MappingConfig,
    ingress_slots: Arc<Semaphore>,
    owner_slots: Arc<Semaphore>,
}

impl Mapping {
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.connect_timeout_ms)
    }

    fn scope(&self, app: &AppHandle) -> anyhow::Result<String> {
        let runtime = app
            .runtime
            .as_ref()
            .context("TCP ingress requires a runtime")?;
        let target = &self.config.target;
        runtime.generation().named_container_scope(
            &target.script,
            &target.class_name,
            &target.object_name,
        )
    }
}

#[derive(Clone)]
pub(super) struct Listener {
    socket: Arc<TcpListener>,
    mapping: Arc<Mapping>,
}

/// Reserve every new socket before changing the active generation. Existing
/// sockets are shared so a target change on the same port needs no rebind.
/// Dropping a failed preparation releases only its newly reserved ports.
pub(super) async fn prepare(generation: Option<&Generation>) -> anyhow::Result<Vec<Listener>> {
    let configs = generation
        .into_iter()
        .flat_map(|g| g.tcp_ingress())
        .map(|(script, route)| MappingConfig {
            listen: SocketAddr::new(
                *LISTEN_IP.get().expect("TCP initialized"),
                route.listen_port,
            ),
            target: Target {
                script: script.clone(),
                class_name: route.class_name.clone(),
                object_name: route.object_name.clone(),
                port: route.container_port,
                startup_path: route.startup_path.clone(),
            },
            connect_timeout_ms: route.connect_timeout_ms,
            max_connections: route.max_connections,
        })
        .collect::<Vec<_>>();
    if let Some(generation) = generation {
        for config in &configs {
            let target = &config.target;
            generation.named_container_scope(
                &target.script,
                &target.class_name,
                &target.object_name,
            )?;
            anyhow::ensure!(
                serde_json::to_vec(&Establish {
                    target: target.clone(),
                    capacity_handoff: false
                })?
                .len()
                    <= MAX_HANDSHAKE_BYTES,
                "TCP target exceeds the peer handshake size limit"
            );
        }
    }
    prepare_configs(configs).await
}

async fn prepare_configs(configs: Vec<MappingConfig>) -> anyhow::Result<Vec<Listener>> {
    let previous = STATE
        .get()
        .expect("TCP initialized")
        .lock()
        .unwrap()
        .listeners
        .clone();
    let mut listeners = Vec::new();
    for config in configs {
        let existing = previous
            .iter()
            .find(|old| old.mapping.config.listen == config.listen);
        let socket = match existing {
            Some(old) => old.socket.clone(),
            None => Arc::new(
                TcpListener::bind(config.listen)
                    .await
                    .with_context(|| format!("bind TCP ingress {}", config.listen))?,
            ),
        };
        let mapping = match existing.filter(|old| old.mapping.config == config) {
            Some(old) => old.mapping.clone(),
            None => Arc::new(Mapping {
                ingress_slots: Arc::new(Semaphore::new(config.max_connections)),
                owner_slots: Arc::new(Semaphore::new(config.max_connections)),
                config,
            }),
        };
        listeners.push(Listener { socket, mapping });
    }
    Ok(listeners)
}

/// Called without an await immediately after runtime adoption. New accepts and
/// peer authorizations use the new mappings. Deployment changes close existing
/// ingress streams; clients reconnect against the newly adopted generation.
pub(super) fn publish(listeners: Vec<Listener>, app: AppHandle) {
    let mut state = STATE.get().expect("TCP initialized").lock().unwrap();
    // A deployment build may have started before shutdown. It must never
    // restart accepting public connections after the shutdown cut.
    if state.stopped {
        return;
    }
    state.servers.abort_all();
    let (shutdown, receiver) = watch::channel(false);
    state.shutdown = Some(shutdown);
    state.mappings = listeners.iter().map(|l| l.mapping.clone()).collect();
    state.listeners = listeners.clone();
    state.servers = serve(listeners, app, receiver);
}

pub(super) async fn shutdown() {
    let mut servers = {
        let mut state = STATE.get().expect("TCP initialized").lock().unwrap();
        state.stopped = true;
        state.listeners.clear();
        if let Some(shutdown) = state.shutdown.take() {
            let _ = shutdown.send(true);
        }
        std::mem::take(&mut state.servers)
    };
    while servers.join_next().await.is_some() {}
}

pub(super) fn serve(
    listeners: Vec<Listener>,
    app: AppHandle,
    shutdown: watch::Receiver<bool>,
) -> JoinSet<()> {
    let mut servers = JoinSet::new();
    for listener in listeners {
        let app = app.clone();
        let mut shutdown = shutdown.clone();
        servers.spawn(async move {
            let mut connections = JoinSet::new();
            tracing::info!(event = "tcp_ingress_listening", listen = %listener.mapping.config.listen);
            loop {
                celld::asyncrt::select! {
                    _ = cancelled(&mut shutdown) => break,
                    accepted = listener.socket.accept() => {
                        let (client, peer) = match accepted {
                            Ok(accepted) => accepted,
                            Err(error) => {
                                tracing::warn!(event = "tcp_ingress_accept_failed", %error);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                continue;
                            }
                        };
                        let Some(admission) = app.admit_public() else { continue };
                        let Ok(slot) = listener.mapping.ingress_slots.clone().try_acquire_owned() else { continue };
                        let _ = client.set_nodelay(true);
                        let mapping = listener.mapping.clone();
                        let app = app.clone();
                        connections.spawn(async move {
                            let _admission = admission;
                            let _slot = slot;
                            let result = async {
                                let backend = tokio::time::timeout(mapping.timeout(), open(&app, &mapping))
                                    .await.context("TCP establishment timed out")??;
                                backend.relay(client).await
                            }.await;
                            if let Err(error) = result {
                                tracing::debug!(event = "tcp_ingress_closed", %peer, error = %format!("{error:#}"));
                            }
                        });
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            drop(listener);
            let _ = tokio::time::timeout(CONNECTION_DRAIN_GRACE, async {
                while connections.join_next().await.is_some() {}
            }).await;
        });
    }
    servers
}

trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

struct Backend {
    stream: Box<dyn Duplex>,
    activity: Option<ActivityGuard>,
    container: Option<(Arc<ContainerEngine>, Arc<CellContainer>, u64)>,
    _slot: Option<OwnedSemaphorePermit>,
}

impl Backend {
    async fn relay(mut self, mut client: impl Duplex) -> anyhow::Result<()> {
        let mut cancellation = self.activity.as_ref().map(ActivityGuard::cancellation);
        if let Some(activity) = &self.activity {
            activity.set_phase("tcp_stream", true, true);
        }
        celld::asyncrt::select! {
            result = tokio::io::copy_bidirectional(&mut client, &mut self.stream) => {
                let (received, sent) = result?;
                tracing::debug!(event = "tcp_stream_finished", received, sent);
            }
            _ = async {
                match &mut cancellation {
                    Some(signal) => cancelled(signal).await,
                    None => std::future::pending().await,
                }
            } => {
                tracing::debug!(event = "tcp_stream_cancelled");
            }
            exit = async {
                match &self.container {
                    Some((engine, cell, run)) => engine.monitor(cell, *run).await,
                    None => std::future::pending().await,
                }
            } => {
                tracing::debug!(event = "tcp_container_exited", ?exit);
            }
        }
        Ok(())
    }
}

async fn cancelled(signal: &mut watch::Receiver<bool>) {
    while !*signal.borrow_and_update() {
        if signal.changed().await.is_err() {
            break;
        }
    }
}

async fn open(app: &AppHandle, mapping: &Mapping) -> anyhow::Result<Backend> {
    let scope = mapping.scope(app)?;
    let mut dispatcher = celld_logic::routing::Dispatcher::default();
    loop {
        let routed = app
            .request(scope.clone())
            .await
            .map_err(|e| anyhow::anyhow!("TCP route: {e:?}"))?;
        let (node, addr, epoch, peer_protocol) = match routed.route {
            Route::Local => return open_local(app, mapping, scope, routed.request).await,
            Route::Remote {
                node,
                addr,
                epoch,
                peer_protocol,
            } => (node, addr, epoch, peer_protocol),
        };
        anyhow::ensure!(
            peer_protocol == peer_auth::PROTOCOL_VERSION,
            "incompatible TCP peer protocol"
        );
        match open_remote(app, mapping, &node, &addr, epoch == 0).await {
            Ok(backend) => return Ok(backend),
            Err(error) => {
                let attempt = if error.is::<peer_tunnel::StaleTunnelRoute>() {
                    celld_logic::routing::Attempt::NotOwner
                } else if error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(reqwest::Error::is_connect)
                {
                    celld_logic::routing::Attempt::NeverConnected
                } else {
                    celld_logic::routing::Attempt::Ambiguous
                };
                if !dispatcher.redispatch(attempt) {
                    return Err(error);
                }
                app.invalidate_remote(scope.clone(), node, epoch).await;
            }
        }
    }
}

async fn open_local(
    app: &AppHandle,
    mapping: &Mapping,
    scope: String,
    request: u64,
) -> anyhow::Result<Backend> {
    let request_id = Some(celld::js::next_do_request_id());
    let local = app.local_request(request, scope.clone(), request_id, "tcp_ingress");
    let mut cancellation = local.cancellation();
    let slot = mapping
        .owner_slots
        .clone()
        .try_acquire_owned()
        .context("TCP owner connection limit reached")?;
    let runtime = app.runtime.as_ref().context("no cell runtime")?;
    let mut abort = AbortPeerFetchOnHangUp {
        runtime: runtime.clone(),
        scope: scope.clone(),
        request_id,
        drain_pins: app.drain_pins.clone(),
        handler_active: true,
    };
    let completed = celld::asyncrt::select! {
        _ = cancelled(&mut cancellation) => anyhow::bail!("TCP startup cancelled"),
        completed = local.run(runtime.fetch_cell(
            scope.clone(), Some(mapping.config.target.object_name.clone()), RuntimeFetch {
                url: format!("http://celld.internal{}", mapping.config.target.startup_path),
                method: "POST".to_string(),
                body: celld::js::RequestBody::Bytes(Bytes::new()),
                headers: Vec::new(), request_id, order: None, parent: None,
            }, None,
        )) => completed,
    };
    abort.handler_answered();
    let answer = completed.result.map_err(local_request_error)?;
    anyhow::ensure!(
        answer.status == 204
            && answer.body.is_empty()
            && answer.websocket.is_none()
            && answer.stream.is_none(),
        "TCP startup hook must return an empty 204 response (received {})",
        answer.status
    );
    abort.disarm();
    let engine = celld::container::engine().await?;
    let cell = engine.cell(&scope).context("TCP target has no container")?;
    let run = cell.current_run();
    // The hook can initiate start() without waiting for the engine's network
    // address. Readiness retries only dialing, never the application hook.
    let stream = loop {
        anyhow::ensure!(
            cell.running() && cell.current_run() == run,
            "container stopped during TCP establishment"
        );
        let connect = async {
            if let Ok(address) = cell.address(mapping.config.target.port) {
                if let Ok(stream) = TcpStream::connect(address).await {
                    return Some(stream);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            None
        };
        celld::asyncrt::select! {
            _ = cancelled(&mut cancellation) => anyhow::bail!("TCP readiness cancelled"),
            stream = connect => if let Some(stream) = stream { break stream; },
        }
    };
    anyhow::ensure!(
        cell.running() && cell.current_run() == run,
        "container restarted during TCP establishment"
    );
    let _ = stream.set_nodelay(true);
    tracing::debug!(event = "tcp_container_connected", %scope, run);
    Ok(Backend {
        stream: Box::new(stream),
        activity: Some(completed.activity),
        container: Some((engine, cell, run)),
        _slot: Some(slot),
    })
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Establish {
    target: Target,
    capacity_handoff: bool,
}

async fn open_remote(
    app: &AppHandle,
    mapping: &Mapping,
    node: &str,
    addr: &str,
    capacity_handoff: bool,
) -> anyhow::Result<Backend> {
    let body = serde_json::to_vec(&Establish {
        target: mapping.config.target.clone(),
        capacity_handoff,
    })?;
    let request = app
        .peer_http
        .post(format!("http://{addr}{PEER_PATH}"))
        .header(hyper::header::CONNECTION, "upgrade")
        .header(hyper::header::UPGRADE, PROTOCOL)
        .header(
            peer_auth::RESPONSE_VERSION_HEADER,
            peer_auth::PROTOCOL_VERSION_TEXT,
        )
        .body(body.clone());
    let reply = app
        .peer_auth
        .sign(request, "POST", PEER_PATH, &body, node)?
        .send()
        .await?;
    peer_auth::validate_response(reply.headers())?;
    if reply.status() == StatusCode::CONFLICT
        && reply
            .headers()
            .get(STALE_ROUTE_HEADER)
            .is_some_and(|v| v == STALE_ROUTE_VALUE)
    {
        return Err(peer_tunnel::StaleTunnelRoute.into());
    }
    anyhow::ensure!(
        reply.status() == StatusCode::SWITCHING_PROTOCOLS
            && reply
                .headers()
                .get(hyper::header::UPGRADE)
                .is_some_and(|v| v == PROTOCOL),
        "TCP peer establishment failed: {}",
        reply.status()
    );
    Ok(Backend {
        stream: Box::new(reply.upgrade().await?),
        activity: None,
        container: None,
        _slot: None,
    })
}

pub(super) async fn accept_peer(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    if request.method() != hyper::Method::POST
        || request.uri().query().is_some()
        || !request
            .headers()
            .get(hyper::header::UPGRADE)
            .is_some_and(|v| v == PROTOCOL)
    {
        return peer_response(response(
            StatusCode::BAD_REQUEST,
            "TCP establishment requires POST and upgrade",
        ));
    }
    let upgrade = hyper::upgrade::on(&mut request);
    let (parts, body) = request.into_parts();
    let body = match tokio::time::timeout(
        Duration::from_secs(5),
        collect_limited_body(body, MAX_HANDSHAKE_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        _ => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid TCP handshake body",
            ))
        }
    };
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        PEER_PATH,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            return stale_reply();
        }
        return peer_response(response(error.status(), error.message()));
    }
    let establish: Establish = match serde_json::from_slice(&body) {
        Ok(establish) => establish,
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid TCP target")),
    };
    let mapping = STATE
        .get()
        .expect("TCP initialized")
        .lock()
        .unwrap()
        .mappings
        .iter()
        .find(|mapping| mapping.config.target == establish.target)
        .cloned();
    let Some(mapping) = mapping else {
        return peer_response(response(
            StatusCode::FORBIDDEN,
            "TCP target is not configured on this node",
        ));
    };
    let preparing = async {
        let scope = mapping.scope(&app)?;
        let routed = if establish.capacity_handoff {
            app.capacity_request(scope.clone()).await
        } else {
            app.request(scope.clone()).await
        };
        match routed {
            Ok(Routed {
                request,
                route: Route::Local,
            }) => open_local(&app, &mapping, scope, request).await,
            Ok(Routed {
                route: Route::Remote { .. },
                ..
            })
            | Err(RequestError::CapacityExhausted) => Err(peer_tunnel::StaleTunnelRoute.into()),
            Err(error) => anyhow::bail!("TCP owner unavailable: {error:?}"),
        }
    };
    let backend = match tokio::time::timeout(mapping.timeout(), preparing).await {
        Ok(Ok(backend)) => backend,
        Ok(Err(error)) if error.is::<peer_tunnel::StaleTunnelRoute>() => {
            return stale_reply();
        }
        Ok(Err(error)) => {
            tracing::debug!(event = "tcp_peer_start_failed", error = %format!("{error:#}"));
            return peer_response(response(
                StatusCode::BAD_GATEWAY,
                "TCP target did not become ready",
            ));
        }
        Err(_) => {
            return peer_response(response(
                StatusCode::GATEWAY_TIMEOUT,
                "TCP establishment timed out",
            ))
        }
    };
    tokio::spawn(async move {
        match tokio::time::timeout(Duration::from_secs(5), upgrade).await {
            Ok(Ok(stream)) => {
                if let Err(error) = backend.relay(TokioIo::new(stream)).await {
                    tracing::debug!(event = "tcp_peer_stream_closed", error = %format!("{error:#}"));
                }
            }
            result => {
                tracing::debug!(event = "tcp_peer_upgrade_failed", ?result);
            }
        }
    });
    let mut reply = response(StatusCode::SWITCHING_PROTOCOLS, "");
    reply.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("upgrade"),
    );
    reply.headers_mut().insert(
        hyper::header::UPGRADE,
        hyper::header::HeaderValue::from_static(PROTOCOL),
    );
    peer_response(reply)
}

fn stale_reply() -> HttpReply {
    let mut reply = response(StatusCode::CONFLICT, "stale TCP route");
    reply.headers_mut().insert(
        STALE_ROUTE_HEADER,
        hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
    );
    peer_response(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn raw_relay_preserves_server_first_bytes_and_half_close_under_backpressure() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (mut client, ingress) = tokio::io::duplex(32);
            let (backend, mut server) = tokio::io::duplex(32);
            let relay = tokio::spawn(
                Backend {
                    stream: Box::new(backend),
                    activity: None,
                    container: None,
                    _slot: None,
                }
                .relay(ingress),
            );
            let server = tokio::spawn(async move {
                server.write_all(b"READY\n").await.unwrap();
                let mut bytes = Vec::new();
                server.read_to_end(&mut bytes).await.unwrap();
                // Respond only after client FIN: closing both directions on
                // that FIN would lose this whole response.
                server.write_all(&bytes).await.unwrap();
                server.shutdown().await.unwrap();
            });
            let mut greeting = [0; 6];
            client.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"READY\n");
            let payload: Vec<u8> = (0..131_072).map(|i| (i % 251) as u8).collect();
            client.write_all(&payload).await.unwrap();
            client.shutdown().await.unwrap();
            let mut reply = Vec::new();
            client.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, payload);
            server.await.unwrap();
            relay.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancellation_observes_an_already_published_stop() {
        let (sender, mut receiver) = watch::channel(false);
        sender.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(100), cancelled(&mut receiver))
            .await
            .unwrap();
        let (sender, mut receiver) = watch::channel(false);
        drop(sender);
        tokio::time::timeout(Duration::from_millis(100), cancelled(&mut receiver))
            .await
            .unwrap();
    }
}
