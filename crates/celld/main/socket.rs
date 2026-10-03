// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Spectrum's celld counterpart: deployment-owned TCP listeners and an
//! authenticated peer upgrade. Application routing remains in connect handlers.

use super::*;
use celld::host_channels::{SocketCallReq, SocketTarget};
use celld::js::RuntimeConnect;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpListener;

const SOCKET_PROTOCOL: &str = "celld-socket-v1";
const BUFFER_BYTES: usize = 64 * 1024;
const OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

struct ListenerTask {
    stop: watch::Sender<bool>,
}

impl Drop for ListenerTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

pub(super) struct Listeners {
    ip: IpAddr,
    tasks: std::sync::Mutex<BTreeMap<u16, ListenerTask>>,
    stopped: std::sync::atomic::AtomicBool,
}

pub(super) struct Prepared {
    ports: BTreeSet<u16>,
    new: BTreeMap<u16, TcpListener>,
}

impl Listeners {
    pub(super) fn new(ip: IpAddr) -> Arc<Self> {
        Arc::new(Self {
            ip,
            tasks: Default::default(),
            stopped: Default::default(),
        })
    }

    /// Bind every new port before changing the serving generation. A conflict
    /// drops the prepared sockets and leaves all existing listeners intact.
    pub(super) async fn prepare(&self, generation: &Generation) -> anyhow::Result<Prepared> {
        anyhow::ensure!(
            !self.stopped.load(std::sync::atomic::Ordering::Acquire),
            "socket listeners are shutting down"
        );
        let existing: BTreeSet<_> = self.tasks.lock().unwrap().keys().copied().collect();
        let ports: BTreeSet<_> = generation
            .socket_workers()
            .iter()
            .map(|socket| socket.port)
            .collect();
        let mut new = BTreeMap::new();
        for port in ports.difference(&existing) {
            let listener = TcpListener::bind(SocketAddr::new(self.ip, *port))
                .await
                .with_context(|| format!("bind Socket Worker TCP port {port}"))?;
            new.insert(*port, listener);
        }
        Ok(Prepared { ports, new })
    }

    pub(super) fn commit(&self, prepared: Prepared, app: AppHandle) {
        let mut tasks = self.tasks.lock().unwrap();
        if self.stopped.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        tasks.retain(|port, _| prepared.ports.contains(port));
        for (port, listener) in prepared.new {
            let (stop, stopped) = watch::channel(false);
            tasks.insert(port, ListenerTask { stop });
            tokio::spawn(serve_listener(listener, app.clone(), stopped));
            tracing::info!(port, "Socket Worker TCP listener started");
        }
    }

    pub(super) fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        self.tasks.lock().unwrap().clear();
    }
}

async fn serve_listener(listener: TcpListener, app: AppHandle, mut stop: watch::Receiver<bool>) {
    let mut sessions = tokio::task::JoinSet::new();
    loop {
        celld::asyncrt::select_biased! {
            "listener shutdown wins over accepting another connection";
            _ = stop.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let Some(admission) = app.admit_public() else { continue };
                    let Some(runtime) = app.runtime.clone() else { continue };
                    let address = stream.peer_addr().map(|address| address.to_string()).unwrap_or_default();
                    let _ = stream.set_nodelay(true);
                    let mut session_stop = stop.clone();
                    sessions.spawn(async move {
                        let _admission = admission;
                        let (opened, opening) = oneshot::channel();
                        // Ingress has no binding caller waiting for admission.
                        // Still consume the acknowledgement so failure is observed.
                        let request = RuntimeConnect {
                            stream: Box::new(stream), address,
                            request_id: Some(celld::js::next_request_id()),
                            order: None, parent: None, opened, cancel: None,
                        };
                        celld::asyncrt::select! {
                            result = runtime.connect_worker(request) => {
                                if let Err(error) = result {
                                    tracing::debug!(%error, "Socket Worker connection ended");
                                }
                            }
                            _ = session_stop.changed() => {}
                        }
                        let _ = opening.await;
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "Socket Worker accept failed");
                    celld::asyncrt::select! {
                        _ = stop.changed() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    }
                }
            },
            _ = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
}

pub(super) async fn dispatch(app: AppHandle, call: SocketCallReq) {
    let SocketCallReq {
        generation,
        target,
        address,
        stream,
        opened,
        cancel,
    } = call;
    let request = RuntimeConnect {
        stream,
        address,
        opened,
        request_id: Some(celld::js::next_request_id()),
        order: None,
        parent: None,
        cancel,
    };
    let result = match target {
        SocketTarget::Service { script, entrypoint } => match app.runtime.as_ref() {
            Some(runtime) => {
                runtime
                    .connect_service(generation, &script, entrypoint, request)
                    .await
            }
            None => Err(anyhow::anyhow!("no Worker runtime")),
        },
        SocketTarget::Cell {
            scope,
            name,
            order,
            parent,
        } => {
            let request = RuntimeConnect {
                order,
                parent,
                ..request
            };
            dispatch_cell(&app, scope, name, request).await
        }
    };
    if let Err(error) = result {
        tracing::debug!(%error, "socket binding connection ended");
    }
}

async fn dispatch_cell(
    app: &AppHandle,
    scope: String,
    name: Option<String>,
    mut connection: RuntimeConnect,
) -> anyhow::Result<()> {
    let cancel = connection.cancel.take();
    let operation = dispatch_cell_uncancelled(app, scope, name, connection);
    match cancel {
        Some(mut cancel) => celld::asyncrt::select_biased! {
            "caller cancellation prevents starting another routed socket event";
            _ = &mut cancel => Err(anyhow::anyhow!("socket caller disconnected")),
            result = operation => result,
        },
        None => operation.await,
    }
}

async fn dispatch_cell_uncancelled(
    app: &AppHandle,
    scope: String,
    name: Option<String>,
    mut connection: RuntimeConnect,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        celld_logic::cell::valid_cell_scope(&scope),
        "malformed socket cell scope"
    );
    let mut retry = RemoteRouteRetry::new(app.operation_deadline_ms);
    loop {
        let Routed { request, route } = app
            .request(scope.clone())
            .await
            .map_err(|error| anyhow::anyhow!("route socket: {error:?}"))?;
        match route {
            Route::Local => {
                return run_local(app, request, scope, name, connection).await;
            }
            Route::Remote {
                node, addr, epoch, ..
            } => {
                let _activity = app.activity(request, scope.clone());
                if retry.observe(&node, epoch) {
                    match retry.wait(None).await {
                        RemoteRetryOutcome::Ready => {}
                        _ => anyhow::bail!("socket route did not converge"),
                    }
                }
                let path = peer_path(&scope, name.as_deref(), &connection.address);
                let request = app
                    .peer_http
                    .get(format!("http://{addr}{path}"))
                    .header(hyper::header::UPGRADE, SOCKET_PROTOCOL)
                    .header(hyper::header::CONNECTION, "upgrade");
                let response = tokio::time::timeout(
                    OPEN_TIMEOUT,
                    app.peer_auth
                        .sign(request, "GET", &path, b"", &node)?
                        .send(),
                )
                .await
                .context("peer socket admission timed out")??;
                peer_auth::validate_response(response.headers())?;
                if response.status() == StatusCode::CONFLICT
                    && response
                        .headers()
                        .get(STALE_ROUTE_HEADER)
                        .is_some_and(|value| value == STALE_ROUTE_VALUE)
                {
                    app.invalidate_remote(scope.clone(), node, epoch).await;
                    continue;
                }
                anyhow::ensure!(
                    response.status() == StatusCode::SWITCHING_PROTOCOLS,
                    "peer socket admission answered {}",
                    response.status()
                );
                anyhow::ensure!(
                    response
                        .headers()
                        .get(hyper::header::UPGRADE)
                        .is_some_and(|value| value == SOCKET_PROTOCOL),
                    "invalid peer socket upgrade"
                );
                let mut remote = response.upgrade().await?;
                // Once admitted, bytes and handler execution cannot be replayed.
                let _ = connection.opened.send(Ok(()));
                tokio::io::copy_bidirectional(&mut connection.stream, &mut remote).await?;
                return Ok(());
            }
        }
    }
}

async fn run_local(
    app: &AppHandle,
    request: u64,
    scope: String,
    name: Option<String>,
    connection: RuntimeConnect,
) -> anyhow::Result<()> {
    let _activity = app.activity_for(request, scope.clone(), connection.request_id, "socket");
    app.runtime
        .as_ref()
        .context("no cell runtime")?
        .connect_cell(scope, name, connection)
        .await
}

fn peer_path(scope: &str, name: Option<&str>, address: &str) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("scope", scope)
        .append_pair("address", address);
    if let Some(name) = name {
        query.append_pair("name", name);
    }
    format!("/peer/socket?{}", query.finish())
}

/// Admit locally before returning 101. The signature covers all routing
/// fields in the query; a peer can never alter them after authentication.
pub(super) async fn accept(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let path = request
        .uri()
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/peer/socket");
    if request.method() != hyper::Method::GET
        || !request
            .headers()
            .get(hyper::header::UPGRADE)
            .is_some_and(|value| value == SOCKET_PROTOCOL)
    {
        return peer_response(response(StatusCode::BAD_REQUEST, "socket upgrade required"));
    }
    if let Err(error) = app.peer_auth.verify(
        request.method(),
        path,
        request.headers(),
        b"",
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    let mut scope = None;
    let mut name = None;
    let mut address = None;
    for (key, value) in
        url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
    {
        let target = match key.as_ref() {
            "scope" => &mut scope,
            "name" => &mut name,
            "address" => &mut address,
            _ => {
                return peer_response(response(
                    StatusCode::BAD_REQUEST,
                    "unknown socket routing field",
                ))
            }
        };
        if target.replace(value.into_owned()).is_some() {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "duplicate socket routing field",
            ));
        }
    }
    let Some(scope) = scope.filter(|scope| celld_logic::cell::valid_cell_scope(scope)) else {
        return peer_response(malformed_scope());
    };
    let Some(address) = address else {
        return peer_response(response(StatusCode::BAD_REQUEST, "missing socket address"));
    };
    let Routed {
        request: routed,
        route,
    } = match app.request(scope.clone()).await {
        Ok(routed) => routed,
        Err(error) => {
            return peer_response(response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("route socket: {error:?}"),
            ))
        }
    };
    if route != Route::Local {
        drop(app.activity(routed, scope));
        let mut stale = response(StatusCode::CONFLICT, "stale socket route");
        stale.headers_mut().insert(
            hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
            hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
        );
        return peer_response(stale);
    }
    let (mut relay, receiving) = tokio::io::duplex(BUFFER_BYTES);
    let (opened, opening) = oneshot::channel();
    let connection = RuntimeConnect {
        stream: Box::new(receiving),
        address,
        opened,
        request_id: Some(celld::js::next_request_id()),
        order: None,
        parent: None,
        cancel: None,
    };
    let upgrade = hyper::upgrade::on(&mut request);
    let (accepted, admission) = oneshot::channel();
    tokio::spawn(async move {
        let handler = run_local(&app, routed, scope, name, connection);
        let transport = async {
            match tokio::time::timeout(OPEN_TIMEOUT, opening).await {
                Ok(Ok(Ok(()))) => {
                    if accepted.send(Ok(())).is_err() {
                        return;
                    }
                }
                Ok(Ok(Err(error))) => {
                    let _ = accepted.send(Err(error));
                    return;
                }
                _ => {
                    let _ = accepted.send(Err(anyhow::anyhow!("socket handler admission failed")));
                    return;
                }
            }
            if let Ok(upgraded) = upgrade.await {
                let mut upgraded = TokioIo::new(upgraded);
                let _ = tokio::io::copy_bidirectional(&mut relay, &mut upgraded).await;
            }
        };
        tokio::pin!(handler, transport);
        celld::asyncrt::select! {
            _ = &mut handler => { let _ = transport.await; },
            _ = &mut transport => {},
        }
    });
    match admission.await {
        Ok(Ok(())) => {
            let mut reply = response(StatusCode::SWITCHING_PROTOCOLS, "");
            reply.headers_mut().insert(
                hyper::header::UPGRADE,
                hyper::header::HeaderValue::from_static(SOCKET_PROTOCOL),
            );
            reply.headers_mut().insert(
                hyper::header::CONNECTION,
                hyper::header::HeaderValue::from_static("upgrade"),
            );
            peer_response(reply)
        }
        Ok(Err(error)) => peer_response(response(StatusCode::BAD_GATEWAY, format!("{error:#}"))),
        Err(_) => peer_response(response(
            StatusCode::BAD_GATEWAY,
            "socket handler dropped admission",
        )),
    }
}
