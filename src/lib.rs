//! Server for the legacy `StoreHub` API.

pub mod shutdown;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;

use re_byte_size::{MemUsageNode, MemUsageTree, SizeBytes};
use re_log_channel::{DataSourceMessage, DataSourceUiCommand};
use re_log_encoding::{ToApplication as _, ToTransport as _};
use re_log_types::TableMsg;
use re_protos::common::v1alpha1::{
    DataframePart as DataframePartProto, StoreId as StoreIdProto, StoreKind as StoreKindProto,
    TableId as TableIdProto,
};
use re_protos::log_msg::v1alpha1::{ArrowMsg as ArrowMsgProto, LogMsg as LogMsgProto};
use re_protos::sdk_comms::v1alpha1::{
    ReadMessagesRequest, ReadMessagesResponse, ReadTablesRequest, ReadTablesResponse,
    WriteMessagesRequest, WriteMessagesResponse, WriteTableRequest, WriteTableResponse,
    message_proxy_service_server,
};
use re_quota_channel::{async_broadcast_channel, async_mpsc_channel};
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::priority_stream::PriorityMerge;

mod priority_stream;
mod viewer_control;

pub use viewer_control::ViewerControl;

pub use re_memory::MemoryLimit;

/// Default port of the OSS /proxy server.
pub const DEFAULT_SERVER_PORT: u16 = 9876;

pub const MAX_DECODING_MESSAGE_SIZE: usize = u32::MAX as usize;
pub const MAX_ENCODING_MESSAGE_SIZE: usize = MAX_DECODING_MESSAGE_SIZE;

/// Maximum number of messages in the input queue.
const CHANNEL_SIZE_MESSAGES: usize = 1024; // TODO(emilk): move into `ServerOptions` after the patch release.

/// Make sure we can handle a quick burst of messages without blocking,
/// even if the server has a [`ServerOptions::memory_limit`] of zero.
const CHANNEL_SIZE_BYTES: u64 = 128 * 1024 * 1024; // TODO(emilk): move into `ServerOptions` after the patch release.

/// Options for the gRPC Proxy Server
#[derive(Clone, Debug)]
pub struct ServerOptions {
    /// When a client connect, should they be sent the oldest data first, or the newest?
    pub playback_behavior: PlaybackBehavior,

    /// Limit on how much history the server saves.
    ///
    /// It will start garbage collecting old data when we reach this.
    pub memory_limit: MemoryLimit, // TODO(emilk): rename `history_limit`

    /// Additional origin patterns allowed to make cross-origin requests to the server.
    ///
    /// By default, only `localhost`, `127.0.0.1`, and `rerun.io` are allowed.
    /// Patterns are matched against the full `Origin` header value (e.g. `https://example.com:8080`),
    /// using glob-style matching where `*` matches any sequence of characters.
    ///
    /// Examples:
    /// - `"https://*.example.com"` — all subdomains on the default port (443)
    /// - `"https://example.com:8080"` — exact origin with a specific port
    /// - `"https://example.com:*"` — any port on example.com
    pub cors_allowed_origins: Vec<String>,

    /// CERULION PATCH (CER-858): when `true`, the history buffer NEVER retains
    /// disposable/temporal messages (recording data) — only `persistent`
    /// (`SetStoreInfo` + blueprint + `BlueprintActivationCommand`) and `static_`
    /// (`is_static` chunks). A fresh client's connect-history then carries the
    /// SCENE SKELETON with ZERO temporal replay, at ANY producer bandwidth
    /// (Cerulion Studio's instant-only live viz). Live subscribers still receive
    /// live temporal data via the broadcast; it is only the per-client REPLAY
    /// buffer that drops it. Default `false` (stock `re_grpc_server` behavior).
    /// See `CERULION-PATCH.md` at this crate's root.
    pub drop_temporal_history: bool,

    /// CERULION PATCH (CER-959): a byte budget for TEMPORAL (disposable) data
    /// sitting in the LIVE broadcast queue. `Some(n)` ⇒ when the queue already
    /// holds more than `n` bytes, a further temporal message is DROPPED rather
    /// than awaited; `None` (the default) is stock behaviour.
    ///
    /// This gates a different buffer from [`Self::drop_temporal_history`]'s. That
    /// one is the per-client REPLAY history (what a late joiner is sent on
    /// connect). This one is the LIVE path every already-connected viewer reads,
    /// which is byte-quota'd at `CHANNEL_SIZE_BYTES` (128 MiB) and **awaited**
    /// when full — so a viewer that renders slower than the producer publishes
    /// accumulates frames there. For a 30 Hz camera decoded to raw RGB8 (a
    /// 1280x720 frame is 2.6 MiB) that is ~47 frames ≈ 1.6 s of stale video the
    /// viewer must play through before it shows the present, measured climbing
    /// at ~4 frames/s against an undrained receiver.
    ///
    /// Under a budget, the live queue stays at most `n` + one message deep, so
    /// what a viewer sees is bounded to the present regardless of how far behind
    /// it falls. Only TEMPORAL messages are eligible: `persistent`
    /// (`SetStoreInfo` / blueprint / activation) and `static_` chunks are the
    /// scene skeleton every viewer needs and always take the reliable awaiting
    /// path, so a busy live stream can never cost a client its scene.
    ///
    /// **The drop is TOTAL, on both buffers.** A dropped message is not written
    /// to the replay history either — the two buffers are separate, but a message
    /// the live path judged not worth delivering NOW is not worth replaying to a
    /// viewer that connects LATER, and writing it to history would grow a buffer
    /// this option exists to keep bounded. So `capture_memory`'s `disposable`
    /// under-reports what the writer produced by exactly `live_dropped`.
    ///
    /// The budget must exceed the largest single message or that message could
    /// never cross; the drop compares against the CURRENT occupancy, so one
    /// message always fits an empty queue whatever its size.
    ///
    /// **Small messages are exempt from THIS axis.** A message under
    /// [`LIVE_SMALL_MESSAGE_FLOOR_BYTES`] (8 KiB) is never dropped for the queue
    /// being over budget: the comparison is against OCCUPANCY, so without the
    /// floor a queue held over budget by megabyte-sized frames would shed every
    /// kilobyte-sized plot sample, `/tf` update or `Clear` sharing it — saving the
    /// viewer under a millisecond while permanently losing one-shot state
    /// transitions. See that constant for the measured band and the worst-case
    /// bound. The MESSAGE axis below still applies to small messages.
    ///
    /// **Sane band.** The budget is only meaningful for
    /// `LIVE_SMALL_MESSAGE_FLOOR_BYTES < n < CHANNEL_SIZE_BYTES` (8 KiB ..
    /// 128 MiB). At or below the floor nothing is ever over-budget-dropped; at or
    /// above `CHANNEL_SIZE_BYTES` occupancy can never reach the budget (the
    /// channel awaits first), so the byte axis silently never fires and only the
    /// message axis remains. Both ends are warned about once at server start
    /// rather than clamped — a budget outside the band is a configuration
    /// mistake, and silently rewriting it would hide it.
    ///
    /// **The gate watches BOTH quotas, not just this one.** The channel is
    /// bounded by `CHANNEL_SIZE_BYTES` *and* `CHANNEL_SIZE_MESSAGES` (1024), and
    /// `send_async` awaits when EITHER is reached — so a byte-only gate would let
    /// a stream of small messages (plots, `/tf`, telemetry) reach the message
    /// quota at a few MiB, await, and stall the event loop while this budget
    /// never fired. See [`LIVE_MESSAGE_RESERVE`].
    ///
    /// Dropping (rather than shrinking either quota) is deliberate: shrinking
    /// would make the event loop AWAIT sooner, and that loop also serves
    /// `Event::NewClient`, so a wedged viewer would stop new viewers from
    /// connecting at all.
    ///
    /// Drops are reported, never silent: the first drop of a regime is a `warn!`,
    /// repeats are `debug!` carrying running totals, and recovery is one `info!`.
    /// The unconditional running total is additionally exposed as `live_dropped`
    /// by [`MessageProxyHandle::capture_memory`] — note that a host reaching this
    /// server through `re_sdk`'s `GrpcServerSink` has no handle to call it with,
    /// so for those hosts the LOG is the whole window.
    pub live_temporal_budget_bytes: Option<u64>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            playback_behavior: PlaybackBehavior::OldestFirst,
            memory_limit: MemoryLimit::from_bytes(1024 * 1024 * 1024), // Be very conservative by default
            cors_allowed_origins: Vec::new(),
            drop_temporal_history: false,     // CERULION PATCH (CER-858)
            live_temporal_budget_bytes: None, // CERULION PATCH (CER-959)
        }
    }
}

/// What happens when a client connects to a gRPC server?
#[derive(Clone, Copy, Debug)]
pub enum PlaybackBehavior {
    /// Start playing back all the old data first,
    /// and only after start sending anything that happened since.
    OldestFirst,

    /// Prioritize the newest arriving messages,
    /// replaying the history later, starting with the newest.
    NewestFirst,
}

impl PlaybackBehavior {
    pub fn from_newest_first(newest_first: bool) -> Self {
        if newest_first {
            Self::NewestFirst
        } else {
            Self::OldestFirst
        }
    }
}

/// Wrapper with a nicer error message
#[derive(Debug)]
pub struct TonicStatusError(pub tonic::Status);

impl std::fmt::Display for TonicStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: duplicated in `re_grpc_client` and `re_redap_client`
        fmt_tonic_status(f, &self.0)
    }
}

fn fmt_tonic_status(f: &mut std::fmt::Formatter<'_>, status: &tonic::Status) -> std::fmt::Result {
    if status.message().is_empty() {
        write!(f, "gRPC error")?;
    } else {
        write!(f, "{}", status.message())?;
    }

    if status.code() != tonic::Code::Unknown {
        write!(f, " ({})", status.code())?;
    }

    if !status.metadata().is_empty() {
        write!(
            f,
            "{} metadata: {:?}",
            re_error::DETAILS_SEPARATOR,
            status.metadata().as_ref()
        )?;
    }
    Ok(())
}

impl From<tonic::Status> for TonicStatusError {
    fn from(value: tonic::Status) -> Self {
        Self(value)
    }
}

const DEFAULT_CORS_PATTERNS: &[&str] = &[
    "*://localhost",
    "*://localhost:*",
    "*://127.0.0.1",
    "*://127.0.0.1:*",
    "*://rerun.io",
    "*://rerun.io:*",
];

/// Returns true if the given origin is allowed by the given patterns.
fn is_origin_allowed(origin: &str, patterns: &[wildmatch::WildMatch]) -> bool {
    patterns.iter().any(|pat| pat.matches(origin))
}

/// Build a CORS layer that allows only localhost, 127.0.0.1, rerun.io,
/// and any additional user-specified origin patterns.
///
/// Patterns are matched against the full `Origin` header value,
/// using glob-style matching where `*` matches any sequence of characters.
pub fn cors_layer(extra_allowed_origins: &[String]) -> CorsLayer {
    let allowed_origin_patterns: Vec<wildmatch::WildMatch> = std::iter::chain(
        DEFAULT_CORS_PATTERNS.iter().copied(),
        extra_allowed_origins.iter().map(String::as_str),
    )
    .map(wildmatch::WildMatch::new)
    .collect();
    CorsLayer::very_permissive().allow_origin(AllowOrigin::predicate(
        move |origin, _request_parts| {
            let Ok(origin) = origin.to_str() else {
                return false;
            };
            is_origin_allowed(origin, &allowed_origin_patterns)
        },
    ))
}

/// Interceptor that rejects any request whose peer is not on the local machine.
#[derive(Clone, Copy, Default)]
pub struct LoopbackOnly;

impl tonic::service::Interceptor for LoopbackOnly {
    fn call(&mut self, request: tonic::Request<()>) -> tonic::Result<tonic::Request<()>> {
        if request
            .remote_addr()
            .is_some_and(|addr| addr.ip().is_loopback())
        {
            Ok(request)
        } else {
            Err(tonic::Status::permission_denied(
                "Only connections from the local machine are allowed",
            ))
        }
    }
}

/// gRPC services to serve alongside the proxy, each restricted to connections from the local machine.
///
/// Pass to [`spawn_with_recv_and_services`]. Every added service is wrapped with [`LoopbackOnly`].
#[derive(Default)]
pub struct LoopbackServices {
    builder: tonic::service::RoutesBuilder,
}

impl LoopbackServices {
    /// Add a gRPC service that may only be reached from the local machine.
    pub fn add_service<S>(&mut self, svc: S) -> &mut Self
    where
        S: tonic::codegen::Service<
                tonic::codegen::http::Request<tonic::body::Body>,
                Response = tonic::codegen::http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.builder
            .add_service(tonic::service::interceptor::InterceptedService::new(
                svc,
                LoopbackOnly,
            ));
        self
    }

    fn into_routes(self) -> tonic::service::Routes {
        self.builder.routes()
    }
}

// TODO(jan): Refactor `serve`/`spawn` variants into a builder?

/// Start a Rerun server, listening on `addr`.
///
/// A Rerun server is an in-memory implementation of a Storage Node.
///
/// The returned future must be polled for the server to make progress.
///
/// Currently, the only RPCs supported by the server are `WriteMessages` and `ReadMessages`.
///
/// Clients send data to the server via `WriteMessages`. Any sent messages will be stored
/// in the server's message queue. Messages are only removed if the server hits its configured
/// memory limit.
///
/// Clients receive data from the server via `ReadMessages`. Upon establishing the stream,
/// the server sends all messages stored in its message queue, and subscribes the client
/// to the queue. Any messages sent to the server through `WriteMessages` will be proxied
/// to the open `ReadMessages` stream.
pub async fn serve(
    addr: SocketAddr,
    options: ServerOptions,
    shutdown: shutdown::Shutdown,
) -> anyhow::Result<()> {
    let message_proxy = MessageProxy::new(options.clone());
    serve_impl(
        addr,
        options,
        message_proxy,
        shutdown,
        tonic::service::Routes::default(),
    )
    .await
}

async fn serve_impl(
    addr: SocketAddr,
    options: ServerOptions,
    message_proxy: MessageProxy,
    shutdown: shutdown::Shutdown,
    extra_services: tonic::service::Routes,
) -> anyhow::Result<()> {
    // TODO(rust-lang/rust#130668): When listening on `::` we want to listen to both ipv6 `::` and ipv4 `0.0.0.0`
    // On Mac & Linux this happens automatically since all sockets are dual-stack by default.
    // On Windows, the dual stack behavior is opt-in, but `TcpListener::bind` does not expose the option.
    // To work around this, we explicitly listen on both ipv4 & ipv6 if an unspecified ipv6 address is used.
    let dual_stack_windows = cfg!(target_os = "windows")
        && matches!(addr.ip(), std::net::IpAddr::V6(ipv6) if ipv6.is_unspecified());

    let incoming: Pin<Box<dyn Stream<Item = _> + Send>> = if dual_stack_windows {
        let ipv6_addr = addr;
        let ipv4_addr = SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            addr.port(),
        ));

        let tcp_listener_ipv6 = TcpListener::bind(ipv6_addr).await?;
        let tcp_listener_ipv4 = TcpListener::bind(ipv4_addr).await?;

        let incoming_ipv6 = TcpIncoming::from(tcp_listener_ipv6).with_nodelay(Some(true));
        let incoming_ipv4 = TcpIncoming::from(tcp_listener_ipv4).with_nodelay(Some(true));

        // Merge both streams into a single stream
        let merged = tokio_stream::StreamExt::merge(incoming_ipv6, incoming_ipv4);

        let connect_addr = format!("rerun+http://127.0.0.1:{}/proxy", addr.port());

        re_log::info!(
            "Listening for gRPC connections on {ipv6_addr} and {ipv4_addr}. Connect by running `rerun --connect {connect_addr}`",
        );

        Box::pin(merged)
    } else {
        let tcp_listener = TcpListener::bind(addr).await?;
        let incoming = TcpIncoming::from(tcp_listener).with_nodelay(Some(true));

        let connect_addr = if addr.ip().is_loopback() || addr.ip().is_unspecified() {
            format!("rerun+http://127.0.0.1:{}/proxy", addr.port())
        } else {
            format!("rerun+http://{addr}/proxy")
        };

        re_log::info!(
            "Listening for gRPC connections on {addr}. Connect by running `rerun --connect {connect_addr}`",
        );

        Box::pin(incoming)
    };

    re_log::debug!("Server memory limit set at {}", options.memory_limit);

    let cors = cors_layer(&options.cors_allowed_origins);
    let grpc_web = tonic_web::GrpcWebLayer::new();

    let routes = extra_services.add_service(
        re_protos::sdk_comms::v1alpha1::message_proxy_service_server::MessageProxyServiceServer::new(
            message_proxy,
        )
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE),
    );

    Server::builder()
        .accept_http1(true) // Support `grpc-web` clients
        .layer(cors) // Allow CORS requests from web clients
        .layer(grpc_web) // Support `grpc-web` clients
        .add_routes(routes)
        .serve_with_incoming_shutdown(incoming, shutdown.wait())
        .await?;

    Ok(())
}

/// Start a Rerun server, listening on `addr`.
///
/// The returned future must be polled for the server to make progress.
///
/// This function additionally accepts a smart channel, through which messages
/// can be sent to the server directly. It is similar to creating a client
/// and sending messages through `WriteMessages`, but without the overhead
/// of a localhost connection.
///
/// See [`serve`] for more information about what a Rerun server is.
pub async fn serve_from_channel(
    addr: SocketAddr,
    options: ServerOptions,
    shutdown: shutdown::Shutdown,
    channel_rx: re_log_channel::LogReceiver,
) {
    let message_proxy = MessageProxy::new(options.clone());
    let event_tx = message_proxy.event_tx.clone();

    tokio::task::spawn_blocking(move || {
        use re_log_channel::SmartMessagePayload;

        loop {
            let msg = if let Ok(msg) = channel_rx.recv() {
                match msg.payload {
                    SmartMessagePayload::Msg(msg) => msg,
                    SmartMessagePayload::Flush { on_flush_done } => {
                        on_flush_done(); // we don't buffer
                        continue;
                    }
                    SmartMessagePayload::Quit(err) => {
                        if let Some(err) = err {
                            re_log::debug!("smart channel sender quit: {err}");
                        } else {
                            re_log::debug!("smart channel sender quit");
                        }
                        break;
                    }
                }
            } else {
                re_log::debug!("smart channel sender closed, closing receiver");
                break;
            };

            match msg {
                DataSourceMessage::LogMsg(msg) => {
                    let msg = match msg.to_transport(re_log_encoding::rrd::Compression::LZ4) {
                        Ok(msg) => msg,
                        Err(err) => {
                            re_log::error!("failed to encode message: {err}");
                            continue;
                        }
                    };

                    if event_tx
                        .blocking_send(Event::Message(LogOrTableMsgProto::LogMsg(msg.into())))
                        .is_err()
                    {
                        re_log::debug!("shut down, closing sender");
                        break;
                    }
                }
                unsupported => {
                    re_log::error_once!(
                        "Not implemented: re_grpc_server support for {}",
                        unsupported.variant_name()
                    );
                }
            }
        }
    });

    if let Err(err) = serve_impl(
        addr,
        options,
        message_proxy,
        shutdown,
        tonic::service::Routes::default(),
    )
    .await
    {
        re_log::error!("message proxy server crashed: {err}");
    }
}

/// Start a Rerun server, listening on `addr`.
///
/// This function additionally accepts a [`re_log_channel::LogReceiverSet`], from which the
/// server will read all messages. It is similar to creating a client
/// and sending messages through `WriteMessages`, but without the overhead
/// of a localhost connection.
///
/// See [`serve`] for more information about what a Rerun server is.
pub fn spawn_from_rx_set(
    addr: SocketAddr,
    options: ServerOptions,
    shutdown: shutdown::Shutdown,
    rxs: re_log_channel::LogReceiverSet,
) -> MessageProxyHandle {
    let message_proxy = MessageProxy::new(options.clone());
    let handle = message_proxy.handle();
    let event_tx = handle.event_tx.clone();

    tokio::spawn(async move {
        if let Err(err) = serve_impl(
            addr,
            options,
            message_proxy,
            shutdown,
            tonic::service::Routes::default(),
        )
        .await
        {
            re_log::error!("message proxy server crashed: {err}");
        }
    });

    tokio::task::spawn_blocking(move || {
        use re_log_channel::SmartMessagePayload;

        loop {
            let msg = if let Ok(msg) = rxs.recv() {
                match msg.payload {
                    SmartMessagePayload::Msg(msg) => msg,
                    SmartMessagePayload::Flush { on_flush_done } => {
                        on_flush_done(); // we don't buffer
                        continue;
                    }
                    SmartMessagePayload::Quit(err) => {
                        if let Some(err) = err {
                            re_log::debug!("smart channel sender quit: {err}");
                        } else {
                            re_log::debug!("smart channel sender quit");
                        }
                        if rxs.is_empty() {
                            // We won't ever receive more data:
                            break;
                        }
                        continue;
                    }
                }
            } else {
                if rxs.is_empty() {
                    // We won't ever receive more data:
                    break;
                }
                continue;
            };

            match msg {
                DataSourceMessage::LogMsg(msg) => {
                    let msg = match msg.to_transport(re_log_encoding::rrd::Compression::LZ4) {
                        Ok(msg) => msg,
                        Err(err) => {
                            re_log::error!("failed to encode message: {err}");
                            continue;
                        }
                    };

                    if event_tx
                        .blocking_send(Event::Message(LogOrTableMsgProto::LogMsg(msg.into())))
                        .is_err()
                    {
                        re_log::debug!("shut down, closing sender");
                        break;
                    }
                }
                unsupported => {
                    re_log::error_once!(
                        "gRPC proxy server cannot forward {}",
                        unsupported.variant_name()
                    );
                }
            }
        }
    });

    handle
}

/// Start a Rerun server, listening on `addr`.
///
/// This function additionally creates a smart channel, and returns its receiving end.
/// Any messages received by the server are sent through the channel. This is similar
/// to creating a client and calling `ReadMessages`, but without the overhead of a
/// localhost connection.
///
/// The server is spawned as a task on a `tokio` runtime. This function panics if the
/// runtime is not available.
///
/// See [`serve`] for more information about what a Rerun server is.
pub fn spawn_with_recv(
    addr: SocketAddr,
    options: ServerOptions,
    shutdown: shutdown::Shutdown,
) -> (re_log_channel::LogReceiver, MessageProxyHandle) {
    spawn_with_recv_and_services(addr, options, shutdown, LoopbackServices::default())
}

/// Like [`spawn_with_recv`], but additionally serves `extra_services` on the same port.
///
/// The extra services are restricted to connections from the local machine (see
/// [`LoopbackServices`]). The message proxy remains reachable according to the bound address.
pub fn spawn_with_recv_and_services(
    addr: SocketAddr,
    options: ServerOptions,
    shutdown: shutdown::Shutdown,
    mut loopback_services: LoopbackServices,
) -> (re_log_channel::LogReceiver, MessageProxyHandle) {
    let uri = re_uri::ProxyUri::new(re_uri::Origin::from_scheme_and_socket_addr(
        re_uri::Scheme::RerunHttp,
        addr,
    ));

    let (channel_log_tx, channel_log_rx) =
        re_log_channel::log_channel(re_log_channel::LogSource::MessageProxy(uri));

    let (message_proxy, mut broadcast_log_rx) = MessageProxy::new_with_recv(options.clone());
    let handle = message_proxy.handle();

    // Serve the viewer-control service alongside the proxy, restricted to loopback connections:
    // it drives the local viewer (e.g. from the MCP server), which only ever connects over 127.0.0.1.
    loopback_services.add_service(
        re_protos::sdk_comms::v1alpha1::viewer_control_service_server::ViewerControlServiceServer::new(
            message_proxy.viewer_control(),
        )
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE),
    );

    tokio::spawn(async move {
        if let Err(err) = serve_impl(
            addr,
            options,
            message_proxy,
            shutdown,
            loopback_services.into_routes(),
        )
        .await
        {
            re_log::error!("message proxy server crashed: {err}");
        }
    });

    tokio::spawn(async move {
        let mut app_id_cache = re_log_encoding::CachingApplicationIdInjector::default();

        loop {
            let msg: anyhow::Result<DataSourceMessage> = match broadcast_log_rx.recv().await {
                Ok(inner) => match inner {
                    LogOrTableMsgProto::LogMsg(msg) => match msg.msg {
                        Some(msg) => msg
                            .to_application((&mut app_id_cache, None))
                            .map(DataSourceMessage::LogMsg)
                            .map_err(|err| err.into()),
                        None => Err(re_protos::missing_field!(
                            re_protos::log_msg::v1alpha1::LogMsg,
                            "msg"
                        )
                        .into()),
                    },

                    LogOrTableMsgProto::Table(msg) => match msg.data.try_into() {
                        Ok(data) => Ok(DataSourceMessage::TableMsg(TableMsg {
                            id: msg.id.into(),
                            data,
                        })),
                        Err(err) => {
                            re_log::error!("Dropping LogMsg::Table due to failed decode: {err}");
                            continue;
                        }
                    },

                    LogOrTableMsgProto::UiCommand(cmd) => Ok(DataSourceMessage::UiCommand(cmd)),
                },

                Err(async_broadcast_channel::RecvError::Closed) => {
                    re_log::debug!("message proxy server shut down, closing receiver");
                    channel_log_tx.quit(None).ok();
                    break;
                }
            };
            match msg {
                Ok(mut log_msg) => {
                    if let Some(metadata_key) =
                        re_sorbet::TimestampLocation::IPCDecode.metadata_key()
                    {
                        // Insert the timestamp metadata into the Arrow message for accurate e2e latency measurements.
                        // Note that this function is only called by the viewer
                        // (that's what the message-receiver is connected to).
                        log_msg.insert_arrow_record_batch_metadata(
                            metadata_key.to_owned(),
                            re_sorbet::timestamp_metadata::now_timestamp(),
                        );
                    }

                    if channel_log_tx.send(log_msg).is_err() {
                        re_log::debug!(
                            "message proxy smart channel receiver closed, closing sender"
                        );
                        break;
                    }
                }
                Err(err) => {
                    re_log::error!("dropping LogMsg due to failed decode: {err}");
                }
            }
        }
    });

    (channel_log_rx, handle)
}

enum Event {
    /// New client connected, requesting full history and subscribing to new messages.
    NewClient(
        oneshot::Sender<(
            Vec<LogOrTableMsgProto>,
            async_broadcast_channel::Receiver<LogOrTableMsgProto>,
        )>,
    ),

    /// A client sent a message.
    Message(LogOrTableMsgProto),

    /// Request that the event loop refresh the cached `MemUsageTree` snapshot.
    ///
    /// The result is written to the shared `MemorySnapshot` held by every
    /// [`MessageProxyHandle`]; the requester reads it from there.
    CaptureMemory,
}

#[derive(Clone, re_byte_size::SizeBytes)]
struct TableMsgProto {
    id: TableIdProto,
    data: DataframePartProto,
}
// -----------------------------------------------------------------------------------

#[derive(Clone, re_byte_size::SizeBytes)]
enum LogOrTableMsgProto {
    LogMsg(LogMsgProto),
    Table(TableMsgProto),
    UiCommand(DataSourceUiCommand),
}

impl From<LogMsgProto> for LogOrTableMsgProto {
    fn from(value: LogMsgProto) -> Self {
        Self::LogMsg(value)
    }
}

impl From<TableMsgProto> for LogOrTableMsgProto {
    fn from(value: TableMsgProto) -> Self {
        Self::Table(value)
    }
}

impl From<DataSourceUiCommand> for LogOrTableMsgProto {
    fn from(value: DataSourceUiCommand) -> Self {
        Self::UiCommand(value)
    }
}

// -----------------------------------------------------------------------------------

#[derive(Default)]
struct MsgQueue {
    /// Messages stored in order of arrival, and garbage collected if the server hits the memory limit.
    queue: VecDeque<LogOrTableMsgProto>,

    /// Total size of [`Self::queue`] in bytes.
    size_bytes: u64,
}

impl MsgQueue {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &LogOrTableMsgProto> {
        self.queue.iter()
    }

    pub fn push_back(&mut self, msg: LogOrTableMsgProto) {
        self.size_bytes += msg.total_size_bytes();
        self.queue.push_back(msg);
    }

    pub fn pop_front(&mut self) -> Option<LogOrTableMsgProto> {
        if let Some(msg) = self.queue.pop_front() {
            self.size_bytes -= msg.total_size_bytes();
            Some(msg)
        } else {
            None
        }
    }

    /// CERULION PATCH (CER-858, F1/F2): retain only the messages for which `keep`
    /// returns `true`, preserving relative order and keeping [`Self::size_bytes`]
    /// exact. Used by the drop-temporal eviction paths (superseded blueprints and
    /// entity-level static dedup) — both of which run under the flag, where the
    /// stock `gc()` is OFF.
    pub fn retain(&mut self, mut keep: impl FnMut(&LogOrTableMsgProto) -> bool) {
        let mut removed_bytes = 0u64;
        self.queue.retain(|msg| {
            if keep(msg) {
                true
            } else {
                removed_bytes += msg.total_size_bytes();
                false
            }
        });
        self.size_bytes -= removed_bytes;
    }
}

// -----------------------------------------------------------------------------------

/// Contains all messages received so far,
/// minus some that are garbage collected when needed.
#[derive(Default)]
struct MessageBuffer {
    /// Normal data messages.
    ///
    /// First to be garbage collected if we run into the memory limit.
    disposable: MsgQueue,

    /// "Static" (non-temporal) data messages.
    ///
    /// Our chunk-store already keeps static messages forever,
    /// and it makes sense: you usually log them once,
    /// and then expect them to stay around.
    ///
    /// We keep the static messages for as long as we can, but if [`Self::disposable`]
    /// is empty and we're still over our memory budget, we start throwing
    /// away the oldest messages from here too.
    /// This is because some users use static logging for camera images,
    /// which adds up very quickly.
    ///
    /// Ideally we would keep exactly one static message per entity/component stream
    /// (like the `ChunkStore` does), but we'll save that for:
    /// TODO(#5531): replace this with `ChunkStore`
    static_: MsgQueue,

    /// These are never garbage collected.
    persistent: MsgQueue,

    /// CERULION PATCH (CER-858): when `true`, [`Self::add_msg`] DROPS disposable
    /// (temporal) messages instead of buffering them, so the per-client
    /// connect-history carries only `persistent` + `static_` (the scene
    /// skeleton) with zero temporal replay. Set from
    /// [`ServerOptions::drop_temporal_history`].
    drop_temporal: bool,
}

impl MessageBuffer {
    fn size_bytes(&self) -> u64 {
        let Self {
            disposable,
            static_,
            persistent,
            drop_temporal: _, // CERULION PATCH (CER-858)
        } = self;
        disposable.size_bytes + static_.size_bytes + persistent.size_bytes
    }

    fn all(&self, playback_behavior: PlaybackBehavior) -> Vec<LogOrTableMsgProto> {
        re_tracing::profile_function!();

        let Self {
            disposable,
            static_,
            persistent,
            drop_temporal: _, // CERULION PATCH (CER-858)
        } = self;

        // Note: we ALWAYS send the persistent and static data before the disposable,
        // regardless of PlaybackBehavior!

        match playback_behavior {
            PlaybackBehavior::OldestFirst => {
                itertools::chain!(persistent.iter(), static_.iter(), disposable.iter())
                    .cloned()
                    .collect()
            }
            PlaybackBehavior::NewestFirst => itertools::chain!(
                persistent.iter().rev(),
                static_.iter().rev(),
                disposable.iter().rev()
            )
            .cloned()
            .collect(),
        }
    }

    fn add_msg(&mut self, msg: LogOrTableMsgProto) {
        match msg {
            LogOrTableMsgProto::LogMsg(msg) => self.add_log_msg(msg),
            LogOrTableMsgProto::Table(msg) => {
                // CERULION PATCH (CER-858): tables/ui-commands are disposable.
                if !self.drop_temporal {
                    self.disposable.push_back(msg.into());
                }
            }
            LogOrTableMsgProto::UiCommand(msg) => {
                if !self.drop_temporal {
                    self.disposable.push_back(msg.into());
                }
            }
        }
    }

    fn add_log_msg(&mut self, msg: LogMsgProto) {
        let Some(inner) = &msg.msg else {
            re_log::error!(
                "{}",
                re_protos::missing_field!(re_protos::log_msg::v1alpha1::LogMsg, "msg")
            );
            return;
        };

        // We put store info, blueprint data, and blueprint activation commands
        // in a separate queue that does *not* get garbage collected.
        use re_protos::log_msg::v1alpha1::log_msg::Msg;
        match inner {
            // Store info (recording or blueprint).
            Msg::SetStoreInfo(..) => {
                self.persistent.push_back(msg.into());
            }

            // Blueprint activation command.
            Msg::BlueprintActivationCommand(cmd) => {
                // CERULION PATCH (CER-858, F1): under `drop_temporal` the stock
                // `gc()` is OFF, so `persistent` would grow forever under Studio
                // `set_blueprint` churn — each layout change mints a FRESH
                // blueprint store, and every new client would replay ALL
                // superseded blueprints. Capture the just-activated blueprint id
                // (before `msg` is moved), push the new activation, then evict
                // every OTHER blueprint store's messages below.
                let active = if self.drop_temporal {
                    cmd.blueprint_id.clone()
                } else {
                    None
                };
                self.persistent.push_back(msg.into());
                if let Some(active) = active {
                    self.evict_superseded_blueprints(&active);
                }
            }

            Msg::ArrowMsg(inner) => {
                let is_blueprint = inner
                    .store_id
                    .as_ref()
                    .is_some_and(|id| id.kind() == StoreKindProto::Blueprint);

                if is_blueprint {
                    // Persist blueprint messages forever.
                    self.persistent.push_back(msg.into());
                } else if inner.is_static == Some(true) {
                    // CERULION PATCH (CER-858, F2): under `drop_temporal`, dedup
                    // statics entity-level (latest-wins, matching rerun's own
                    // static semantics) so a reconnecting client re-logging the
                    // same statics does not grow `static_` unboundedly (the stock
                    // `gc()` that would otherwise cap it is OFF under the flag).
                    // A decode failure yields `None` → plain append (never a
                    // wrong-key drop).
                    let new_key = if self.drop_temporal {
                        Self::static_key_from_arrow(inner)
                    } else {
                        None
                    };
                    if let Some(new_key) = new_key {
                        self.static_
                            .retain(|m| Self::static_key_from_msg(m).as_ref() != Some(&new_key));
                    }
                    self.static_.push_back(msg.into());
                } else if !self.drop_temporal {
                    // Recording data (temporal). CERULION PATCH (CER-858): dropped
                    // (never buffered) when `drop_temporal` is set — the per-client
                    // connect-history stays scene-skeleton-only.
                    self.disposable.push_back(msg.into());
                }
            }
        }
    }

    /// CERULION PATCH (CER-858, F1): the store a persistent message belongs to
    /// (`SetStoreInfo` → its store, `ArrowMsg` → its store, activation command →
    /// its blueprint), or `None` for a message with no store attribution. Cheap:
    /// reads proto fields only, no payload decode.
    fn message_store_id(msg: &LogOrTableMsgProto) -> Option<&StoreIdProto> {
        let LogOrTableMsgProto::LogMsg(log_msg) = msg else {
            return None;
        };
        use re_protos::log_msg::v1alpha1::log_msg::Msg;
        match log_msg.msg.as_ref()? {
            Msg::SetStoreInfo(set) => set.info.as_ref()?.store_id.as_ref(),
            Msg::ArrowMsg(arrow) => arrow.store_id.as_ref(),
            Msg::BlueprintActivationCommand(cmd) => cmd.blueprint_id.as_ref(),
        }
    }

    /// CERULION PATCH (CER-858, F1): evict every blueprint store OTHER than the
    /// just-activated `active` from `persistent`, preserving relative order + exact
    /// byte accounting. A recording-data `SetStoreInfo` is not blueprint-kind, so
    /// it always survives; `active`'s own `SetStoreInfo` + chunks + the new
    /// activation command (already pushed) all share `active`'s recording id, so
    /// they survive too.
    fn evict_superseded_blueprints(&mut self, active: &StoreIdProto) {
        let keep_recording_id = &active.recording_id;
        self.persistent
            .retain(|msg| match Self::message_store_id(msg) {
                Some(id) if id.kind() == StoreKindProto::Blueprint => {
                    &id.recording_id == keep_recording_id
                }
                // Non-blueprint (recording `SetStoreInfo`) or unattributed: keep.
                _ => true,
            });
    }

    /// CERULION PATCH (CER-858, F2): the `(recording_id, entity_path)` a static
    /// chunk belongs to, or `None` if it is not a static chunk or the entity path
    /// cannot be extracted. Extraction requires decoding the arrow payload
    /// (decompress + IPC schema) — done ONLY for the infrequent static path; a
    /// decode failure returns `None` so the caller falls back to a plain append
    /// (never a wrong-key drop).
    fn static_key_from_arrow(arrow: &ArrowMsgProto) -> Option<(String, String)> {
        if arrow.is_static != Some(true) {
            return None;
        }
        let recording_id = arrow.store_id.as_ref()?.recording_id.clone();
        let app = arrow.to_application(()).ok()?;
        let entity_path = app
            .batch
            .schema_ref()
            .metadata()
            .get(re_sorbet::metadata::SORBET_ENTITY_PATH)?
            .clone();
        Some((recording_id, entity_path))
    }

    /// CERULION PATCH (CER-858, F2): [`Self::static_key_from_arrow`] for a buffered
    /// message (used to find the prior static of the same entity to evict).
    fn static_key_from_msg(msg: &LogOrTableMsgProto) -> Option<(String, String)> {
        let LogOrTableMsgProto::LogMsg(log_msg) = msg else {
            return None;
        };
        use re_protos::log_msg::v1alpha1::log_msg::Msg;
        let Some(Msg::ArrowMsg(arrow)) = log_msg.msg.as_ref() else {
            return None;
        };
        Self::static_key_from_arrow(arrow)
    }

    pub fn gc(&mut self, max_bytes: u64) {
        if self.size_bytes() <= max_bytes {
            // We're not using too much memory.
            return;
        }

        re_tracing::profile_scope!("Drop messages");
        re_log::info_once!(
            "Exceeded gRPC proxy server memory limit ({}). Dropping the oldest log messages. Clients connecting after this will not see the full history.",
            re_format::format_bytes(max_bytes as _)
        );

        let start_size = self.size_bytes();
        let mut messages_dropped = 0;

        while self.disposable.pop_front().is_some() {
            messages_dropped += 1;
            if self.size_bytes() < max_bytes {
                break;
            }
        }

        if max_bytes < self.size_bytes() {
            re_log::info_once!(
                "Exceeded gRPC proxy server memory limit ({}). Dropping old *static* log messages as well. Clients connecting after this will no longer see the complete set of static data.",
                re_format::format_bytes(max_bytes as _)
            );
            while self.static_.pop_front().is_some() {
                messages_dropped += 1;
                if self.size_bytes() < max_bytes {
                    break;
                }
            }
        }

        let bytes_dropped = start_size - self.size_bytes();

        re_log::trace!(
            "Dropped {} bytes in {messages_dropped} message(s)",
            re_format::format_bytes(bytes_dropped as _)
        );

        if max_bytes < self.size_bytes() {
            re_log::warn_once!(
                "The gRPC server is using more memory than the given memory limit ({}), despite having garbage-collected all non-persistent messages.",
                re_format::format_bytes(max_bytes as _)
            );
        }
    }
}

// -----------------------------------------------------------------------------------

/// A wrapper that converts an `async_broadcast_channel::Receiver` into a `Stream`.
///
/// This uses `async_stream` internally to bridge the async recv method to Stream.
/// The stream yields the inner value (unwrapped from `Tracked`).
struct BackPressureReceiverStream<T: Clone + SizeBytes + Send + Sync + 'static> {
    inner: Pin<Box<dyn Stream<Item = Result<T, async_broadcast_channel::RecvError>> + Send>>,
}

impl<T: Clone + SizeBytes + Send + Sync + 'static> BackPressureReceiverStream<T> {
    fn new(mut receiver: async_broadcast_channel::Receiver<T>) -> Self {
        let stream = async_stream::stream! {
            while let Ok(value) = receiver.recv().await {
                yield Ok(value);
            }
        };
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T: Clone + SizeBytes + Send + Sync + 'static> Stream for BackPressureReceiverStream<T> {
    type Item = Result<T, async_broadcast_channel::RecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

// -----------------------------------------------------------------------------------

/// Main event loop for the server, which runs in its own task.
///
/// Handles message history, and broadcasts messages to clients.
/// Shared cell that holds the latest memory snapshot the event loop has produced.
///
/// Written by `EventLoop` when it handles [`Event::CaptureMemory`], read by
/// [`MessageProxyHandle::capture_memory`]. `None` until the loop has produced
/// at least one snapshot.
type MemorySnapshot = std::sync::Arc<parking_lot::Mutex<Option<MemUsageTree>>>;

struct EventLoop {
    options: ServerOptions,

    /// New log messages are broadcast to all clients.
    /// Uses a back-pressure channel that blocks senders when the byte limit is exceeded.
    broadcast_log_tx: async_broadcast_channel::Sender<LogOrTableMsgProto>,

    /// Channel for incoming events.
    event_rx: async_mpsc_channel::Receiver<Event>,

    /// All messages received so far, minus those that have been garbage collected.
    history: MessageBuffer,

    /// Latest memory snapshot, refreshed on `Event::CaptureMemory`.
    memory_snapshot: MemorySnapshot,

    /// CERULION PATCH (CER-959): running total of TEMPORAL bytes dropped because
    /// the live queue was over [`ServerOptions::live_temporal_budget_bytes`].
    /// Reported as `live_dropped` by `capture_memory` so the drop is observable,
    /// never silent — and so a test can tell "the queue is small because frames
    /// were dropped" from "the queue is small because nothing was sent".
    live_dropped_bytes: u64,

    /// CERULION PATCH (CER-959): bytes dropped in the CURRENT drop regime, or
    /// `None` when not dropping.
    ///
    /// Drives the loud-once / suppressed-repeat / recovery cycle: a viewer that
    /// falls behind gets ONE `warn!`, repeats are `debug!` carrying the running
    /// total, and catching up emits one `info!` reporting what the regime cost —
    /// then re-arms. Without this, a choppy stream is either silent at default
    /// log levels or floods one line per dropped frame.
    dropping_since: Option<u64>,
}

/// CERULION PATCH (CER-959): message slots reserved for the SCENE SKELETON.
///
/// The live channel has TWO quotas — [`CHANNEL_SIZE_BYTES`] and
/// [`CHANNEL_SIZE_MESSAGES`] — and `send_async` awaits when EITHER is reached.
/// A byte-only gate is therefore not enough to keep the event loop unblocked:
/// a stream of small messages (plots, `/tf`, telemetry — kilobytes each) reaches
/// 1024 messages at only a few MiB, so a byte budget of 8 MiB never fires, the
/// send awaits, and the loop — which also serves `Event::NewClient` — stalls,
/// which is exactly the wedge dropping exists to avoid.
///
/// So the gate watches the MESSAGE axis too, keeping this many slots free.
/// Skeleton messages are never dropped, so the reserve is what guarantees they
/// still have somewhere to go while temporal traffic is being shed.
const LIVE_MESSAGE_RESERVE: usize = 64;

/// CERULION PATCH (CER-959): a message smaller than this is never dropped on the
/// BYTE axis — it cannot be what pushed the queue over its byte budget.
///
/// [`ServerOptions::live_temporal_budget_bytes`] is compared against the queue's
/// CURRENT OCCUPANCY, not against the arriving message. That is what bounds the
/// backlog (a viewer is served the present, not a queue it must play through),
/// but taken alone it makes the drop indiscriminate: while a few megabyte-sized
/// camera frames hold occupancy over budget, EVERY temporal message is shed at
/// any size, from any stream, because they all share one broadcast queue.
///
/// For image and point-cloud frames that is exactly right — the next frame
/// supersedes the dropped one, so the viewer heals the moment it catches up. It
/// is wrong for the kilobyte-scale traffic that shares the queue, on both halves
/// of the trade:
///
/// - **It saves nothing.** Measured through the production encode path
///   (`to_transport(LZ4)` → the proto the queue accounts): a `Scalars` sample is
///   1229 B, a recursive `Clear` 1223 B, a `TextLog` line 1282 B, a `Transform3D`
///   1537 B — against `696_500` B for the SMALLEST camera rendition (640x360
///   RGB8) and `2_779_467` B for 1280x720. Shedding a 1.2 KB message off a queue holding
///   megabytes buys the viewer well under a millisecond of catch-up.
/// - **It can cost a message that never comes again.** A frame is a LATEST-VALUE
///   sample; a small message often is not. A `rerun::Clear` is a one-shot STATE
///   TRANSITION — drop it and the deleted entity stays rendered for the rest of
///   the session, because nothing re-sends it and (under
///   [`ServerOptions::drop_temporal_history`]) no replay history holds it. That
///   converts a latency problem into a persistently WRONG picture, which is the
///   one outcome strictly worse than the lag the budget removes.
///
/// **8 KiB — measured, not picked.** 5.3x the largest control-class message above
/// and 85x below the smallest image-class one, so the band it separates is two
/// orders of magnitude wide and nothing realistic sits in the gap. Point clouds
/// cross it at **~1700 points**: `Points3D` measures 3288 B at 500 points, 5300 B
/// at 1000 and 9308 B at 2000, i.e. ~4.0 B/point over a ~1.28 KB chunk floor, so
/// 8 KiB lands at `(8192 - 1278) / 4.01`. The "no buffer for clouds" half of the
/// ruling therefore keeps holding for clouds of any consequence.
///
/// **This does NOT re-open the wedge, and it is bounded.** The MESSAGE axis is
/// untouched: a flood of small messages is still dropped at
/// [`LIVE_MESSAGE_RESERVE`], which is what keeps `send_async` from awaiting and
/// the event loop (which also serves `Event::NewClient`) from stalling. So
/// floor-admitted traffic can add at most
/// `(CHANNEL_SIZE_MESSAGES - LIVE_MESSAGE_RESERVE) * LIVE_SMALL_MESSAGE_FLOOR_BYTES`
/// = 960 x 8 KiB = 7.5 MiB on top of the byte budget — and only if EVERY one of
/// those 960 slots holds a message just under the floor, which no real small-
/// message stream produces (the classes above are ~1.2-1.5 KB, i.e. ~1.4 MiB for
/// a full 960 slots).
///
/// A budget at or below this floor makes the byte axis inert; see
/// [`ServerOptions::live_temporal_budget_bytes`], which warns about it at
/// startup.
pub const LIVE_SMALL_MESSAGE_FLOOR_BYTES: u64 = 8 * 1024;

/// CERULION PATCH (CER-959): why a configured
/// [`ServerOptions::live_temporal_budget_bytes`] cannot do what its setter meant,
/// or `None` when it is inside the usable band.
///
/// The byte axis can only fire for
/// `LIVE_SMALL_MESSAGE_FLOOR_BYTES < n < CHANNEL_SIZE_BYTES`. Outside it the
/// option is accepted and then silently does nothing on that axis — the failure a
/// caller is least able to see, since the field is set and the message axis still
/// works, so the server looks configured. Pure, so the diagnostic is oracle-
/// testable without a running server.
fn live_budget_band_complaint(budget: Option<u64>) -> Option<&'static str> {
    match budget {
        Some(n) if n <= LIVE_SMALL_MESSAGE_FLOOR_BYTES => Some(
            "it is at or below the small-message floor, so NO message is ever dropped for being \
             over budget (only the message-count axis remains active)",
        ),
        Some(n) if CHANNEL_SIZE_BYTES <= n => Some(
            "it is at or above the live channel's own byte quota, which the sender awaits at — so \
             occupancy can never reach this budget and the byte axis never fires (only the \
             message-count axis remains active)",
        ),
        None | Some(_) => None,
    }
}

/// CERULION PATCH (CER-959): what the LIVE queue ACCOUNTS for one message — the
/// exact quantity [`ServerOptions::live_temporal_budget_bytes`] and
/// [`LIVE_SMALL_MESSAGE_FLOOR_BYTES`] are compared against.
///
/// A host carries two documented sizing obligations it otherwise has no way to
/// check: the budget must exceed its largest single message, and it may need to
/// know which of its own message classes fall under the floor. Both are about the
/// ENCODED, compressed proto the queue holds, which is not the payload the host
/// built — for a raw image the encoded message measures ~0.5% LARGER, and for a
/// one-row chunk the schema overhead is most of it (a 1-value `Scalars` sample
/// encodes to ~1.2 KB).
///
/// Returns `None` if the message cannot be encoded, which is the same condition
/// under which the server would fail to transport it. This performs the full
/// encode, so it is a sizing/diagnostic call — not something to run per message.
pub fn live_queue_size_bytes(msg: &re_log_types::LogMsg) -> Option<u64> {
    let proto: LogMsgProto = msg
        .to_transport(re_log_encoding::rrd::Compression::LZ4)
        .ok()?
        .into();
    Some(LogOrTableMsgProto::LogMsg(proto).total_size_bytes())
}

/// CERULION PATCH (CER-959): the live queue's occupancy on both quota axes.
#[derive(Clone, Copy, Debug)]
struct LiveOccupancy {
    bytes: u64,
    messages: usize,
    max_messages: usize,
}

/// CERULION PATCH (CER-959): what the live-queue gate decided for one message.
#[derive(Debug, PartialEq, Eq)]
enum LiveDecision {
    /// Send it. `closes_regime` carries the finished regime's dropped-byte total
    /// when this admission ENDS a drop regime (⇒ report recovery + re-arm).
    Admit { closes_regime: Option<u64> },
    /// Drop it. `regime_total` is the running total for the open regime; `loud`
    /// marks the regime's head (the one line an operator must see).
    Drop { regime_total: u64, loud: bool },
}

/// CERULION PATCH (CER-959): the WHOLE live-queue policy as a pure function —
/// admission, the loud-once/suppressed-repeat cycle, and the recovery re-arm.
///
/// Pure so that every branch is oracle-testable without a running server. That
/// matters most for the re-arm: a regime that never closes turns every later
/// incident into a suppressed `debug!`, i.e. permanent silence after the first
/// one, and a behaviour-free implementation would let that mutation through.
///
/// # Hysteresis
///
/// The regime closes only once the queue is COMFORTABLY back under budget (half,
/// on both axes), not the instant one message fits. Without that, a viewer
/// rendering marginally slower than the producer — the expected steady state at
/// the boundary, not a corner — oscillates across the threshold and pays a
/// `warn!` + an `info!` per oscillation, which at frame rate is the log-flood
/// class the regime cycle exists to prevent. In the band between the two marks a
/// message is admitted while the regime stays open: no drop, no log.
///
/// # Small-message floor
///
/// The BYTE axis compares the budget against the queue's OCCUPANCY, so on its own
/// it would shed every temporal message at any size while a few camera frames
/// hold the queue over budget. A message under
/// [`LIVE_SMALL_MESSAGE_FLOOR_BYTES`] is therefore exempt from that axis: it
/// cannot be what pushed the queue over, dropping it saves the viewer nothing
/// measurable, and it may be a one-shot state transition that never comes again.
/// The MESSAGE axis still applies to it — that is what bounds a small-message
/// flood and keeps the event loop unblocked.
fn decide_live(
    budget: Option<u64>,
    occ: LiveOccupancy,
    temporal: bool,
    msg_bytes: u64,
    open_regime: Option<u64>,
) -> LiveDecision {
    // No budget, or a message the budget may never touch (the scene skeleton,
    // tables, UI commands) ⇒ always admit, and never let it close a regime: only
    // a TEMPORAL message getting through proves the viewer caught up.
    let (Some(budget), true) = (budget, temporal) else {
        return LiveDecision::Admit {
            closes_regime: None,
        };
    };

    // The BYTE axis skips messages under the floor — see the fn docs. The MESSAGE
    // axis deliberately does not: it is what keeps the loop from wedging, and a
    // flood of small messages is exactly what would wedge it.
    let over_bytes = LIVE_SMALL_MESSAGE_FLOOR_BYTES <= msg_bytes && budget < occ.bytes;
    let over_messages = occ.max_messages <= occ.messages.saturating_add(LIVE_MESSAGE_RESERVE);
    if over_bytes || over_messages {
        return match open_regime {
            Some(total) => LiveDecision::Drop {
                regime_total: total.saturating_add(msg_bytes),
                loud: false,
            },
            None => LiveDecision::Drop {
                regime_total: msg_bytes,
                loud: true,
            },
        };
    }

    // Admitted. Close an open regime only past the LOW-WATER mark on both axes.
    let recovered_bytes = occ.bytes < budget / 2;
    let recovered_messages = occ
        .messages
        .saturating_mul(2)
        .saturating_add(LIVE_MESSAGE_RESERVE)
        < occ.max_messages;
    LiveDecision::Admit {
        closes_regime: match open_regime {
            Some(total) if recovered_bytes && recovered_messages => Some(total),
            _ => None,
        },
    }
}

/// CERULION PATCH (CER-959): is this message TEMPORAL (recording data on a
/// timeline), i.e. eligible for the live-queue drop?
///
/// For `LogMsg` this mirrors [`MessageBuffer::add_log_msg`]'s classification
/// exactly: a blueprint chunk and an `is_static` chunk are skeleton; every other
/// `ArrowMsg` is temporal; `SetStoreInfo` and `BlueprintActivationCommand` are
/// skeleton.
///
/// **Tables and UI commands are NOT temporal**, deliberately parting from
/// `add_msg`, which files them under `disposable`. Those two words mean different
/// things: `disposable` there means "not worth REPLAYING to a late joiner", which
/// does not imply "safe to never deliver at all".
///
/// - A [`TableMsg`] is a ONE-SHOT dataframe from `send_table`. Dropping it loses
///   the whole table, and the writer still gets `Ok(WriteTableResponse)` — a
///   silent success for data that never arrived.
/// - A [`DataSourceUiCommand`] is CONTROL (`SetTimeCursor`, `SaveScreenshot`,
///   `GetViewerState`, …), each carrying an `on_done` responder. Dropping it
///   drops the responder, and the caller gets "viewer dropped the … request
///   before responding (is a viewer running?)" — a diagnostic blaming the viewer
///   for a proxy-side budget decision.
///
/// Neither is high-rate sensor data, so exempting them costs nothing the budget
/// exists to bound.
///
/// Cheap: reads proto fields only, no payload decode.
fn is_temporal(msg: &LogOrTableMsgProto) -> bool {
    let LogOrTableMsgProto::LogMsg(log_msg) = msg else {
        // Tables (one-shot data) + UI commands (control, with a responder) are
        // never dropped — see the fn docs.
        return false;
    };
    use re_protos::log_msg::v1alpha1::log_msg::Msg;
    match log_msg.msg.as_ref() {
        // Store info + activation commands are skeleton. `None` (a payload-less,
        // malformed message that `add_log_msg` logs and drops) rides the same arm
        // deliberately: classifying it skeleton keeps THIS gate from being the
        // thing that silently eats it, so the existing diagnostic still fires.
        None | Some(Msg::SetStoreInfo(..) | Msg::BlueprintActivationCommand(..)) => false,
        Some(Msg::ArrowMsg(arrow)) => {
            let is_blueprint = arrow
                .store_id
                .as_ref()
                .is_some_and(|id| id.kind() == StoreKindProto::Blueprint);
            !is_blueprint && arrow.is_static != Some(true)
        }
    }
}

impl EventLoop {
    fn new(
        options: ServerOptions,
        event_rx: async_mpsc_channel::Receiver<Event>,
        broadcast_log_tx: async_broadcast_channel::Sender<LogOrTableMsgProto>,
        memory_snapshot: MemorySnapshot,
    ) -> Self {
        // CERULION PATCH (CER-858): thread the drop-temporal mode into the buffer.
        let history = MessageBuffer {
            drop_temporal: options.drop_temporal_history,
            ..Default::default()
        };
        Self {
            options,
            broadcast_log_tx,
            event_rx,
            history,
            memory_snapshot,
            live_dropped_bytes: 0, // CERULION PATCH (CER-959)
            dropping_since: None,  // CERULION PATCH (CER-959)
        }
    }

    async fn run_in_place(mut self) {
        loop {
            let Some(event) = self.event_rx.recv().await else {
                break;
            };

            match event {
                Event::NewClient(channel) => {
                    channel
                        .send((
                            self.history.all(self.options.playback_behavior),
                            self.broadcast_log_tx.subscribe(),
                        ))
                        .ok();
                }
                Event::Message(msg) => self.handle_msg(msg).await,
                Event::CaptureMemory => {
                    *self.memory_snapshot.lock() = Some(self.capture_mem_usage_tree());
                }
            }
        }
    }

    /// Snapshot the proxy's history and broadcast queue sizes as a `MemUsageTree`.
    ///
    /// Cheap: reads the already-maintained `MsgQueue::size_bytes` counters and the
    /// broadcast channel's atomic byte counter — no traversal.
    fn capture_mem_usage_tree(&self) -> MemUsageTree {
        MemUsageTree::Node(
            MemUsageNode::new()
                .with_child("disposable", self.history.disposable.size_bytes)
                .with_child("static", self.history.static_.size_bytes)
                .with_child("persistent", self.history.persistent.size_bytes)
                .with_child("broadcast", self.broadcast_log_tx.bytes_in_flight())
                // CERULION PATCH (CER-959): observable drop accounting.
                .with_child("live_dropped", self.live_dropped_bytes),
        )
    }

    async fn handle_msg(&mut self, msg: LogOrTableMsgProto) {
        // CERULION PATCH (CER-959): a TEMPORAL message that would push the LIVE
        // queue past its budget is DROPPED, not awaited — a viewer that is
        // already behind is served the present rather than a backlog it has to
        // play through. Persistent/static (the scene skeleton) are never
        // eligible; see `ServerOptions::live_temporal_budget_bytes`.
        let bytes = msg.total_size_bytes();
        match decide_live(
            self.options.live_temporal_budget_bytes,
            self.live_occupancy(),
            is_temporal(&msg),
            bytes,
            self.dropping_since,
        ) {
            LiveDecision::Drop { regime_total, loud } => {
                self.report_live_drop(bytes, regime_total, loud);
                return;
            }
            LiveDecision::Admit { closes_regime } => {
                if let Some(regime_total) = closes_regime {
                    self.report_live_recovery(regime_total);
                }
            }
        }

        // This will block if the broadcast channel is full, applying back-pressure
        self.broadcast_log_tx.send_async(msg.clone()).await.ok();

        if !self.is_history_enabled() {
            // no need to gc or maintain history
            return;
        }

        self.gc_if_using_too_much_ram();

        self.history.add_msg(msg);
    }

    /// CERULION PATCH (CER-959): account for + report one dropped temporal
    /// message, on the loud-once / suppressed-repeat cycle.
    ///
    /// A dropped frame must never be SILENT (an operator watching choppy video
    /// has to be able to learn why) and must never FLOOD (this fires at frame
    /// rate for as long as the viewer is behind). So the first drop of a regime
    /// is one `warn!` and every repeat is a `debug!` carrying the running totals;
    /// [`Self::report_live_recovery`] closes the regime and re-arms.
    fn report_live_drop(&mut self, bytes: u64, regime_total: u64, loud: bool) {
        self.live_dropped_bytes = self.live_dropped_bytes.saturating_add(bytes);
        self.dropping_since = Some(regime_total);
        // Structured fields, not interpolated numbers, so an operator can filter
        // and grep these the way they do every other Cerulion drop counter.
        if loud {
            re_log::warn!(
                "Live viz is dropping frames: a viewer is not consuming as fast as this stream \
                 produces. Frames are dropped so the viewer keeps showing the PRESENT instead of \
                 playing through a growing backlog; the scene (blueprint, statics) is never \
                 dropped. live_budget_bytes={} queue_bytes={} queue_messages={} \
                 dropped_bytes={bytes} total_dropped_bytes={}",
                self.options
                    .live_temporal_budget_bytes
                    .map_or_else(|| "none".to_owned(), |b| b.to_string()),
                self.broadcast_log_tx.bytes_in_flight(),
                self.broadcast_log_tx.messages_in_flight(),
                self.live_dropped_bytes,
            );
        } else {
            re_log::debug!(
                "Live viz still dropping. regime_dropped_bytes={regime_total} \
                 dropped_bytes={bytes} total_dropped_bytes={}",
                self.live_dropped_bytes
            );
        }
    }

    /// CERULION PATCH (CER-959): close an open drop regime — report what it cost,
    /// once, and re-arm so the next one is loud again.
    ///
    /// The decision to close is [`decide_live`]'s (past a low-water mark on both
    /// axes, not the instant one message fits); this only reports it. The wording
    /// claims only what is established: the queue drained back under the mark.
    fn report_live_recovery(&mut self, regime_total: u64) {
        self.dropping_since = None;
        re_log::info!(
            "Live viz is no longer dropping: the live queue drained back under its budget. \
             regime_dropped_bytes={regime_total} total_dropped_bytes={}",
            self.live_dropped_bytes
        );
    }

    /// CERULION PATCH (CER-959): the live queue's occupancy on both quota axes.
    fn live_occupancy(&self) -> LiveOccupancy {
        LiveOccupancy {
            bytes: self.broadcast_log_tx.bytes_in_flight(),
            messages: self.broadcast_log_tx.messages_in_flight(),
            max_messages: self.broadcast_log_tx.max_messages(),
        }
    }

    fn is_history_enabled(&self) -> bool {
        // CERULION PATCH (CER-858): drop-temporal mode retains the scene skeleton
        // (persistent + static_) regardless of `memory_limit` — so history is
        // "enabled" for statics even when the byte budget is ZERO.
        self.options.memory_limit != MemoryLimit::ZERO || self.options.drop_temporal_history
    }

    fn gc_if_using_too_much_ram(&mut self) {
        // CERULION PATCH (CER-858): under drop-temporal, the buffer holds ONLY
        // statics + persistent (temporal is never buffered) — all of which are
        // needed by every late joiner — so `memory_limit`-driven GC is OFF (it
        // would only ever evict needed statics). The mode is self-contained;
        // `memory_limit` is moot under it.
        if !self.options.drop_temporal_history && self.options.memory_limit.is_limited() {
            self.history.gc(self.options.memory_limit.as_bytes());
        }
    }
}

/// A cloneable handle to a running [`MessageProxy`].
///
/// Used to read the proxy's most recent memory snapshot from outside the tokio
/// runtime (e.g. from the viewer's UI thread).
#[derive(Clone)]
pub struct MessageProxyHandle {
    event_tx: async_mpsc_channel::Sender<Event>,
    memory_snapshot: MemorySnapshot,
}

impl MessageProxyHandle {
    /// Return the latest recent memory snapshot the proxy has produced.
    pub fn capture_memory(&self) -> Option<MemUsageTree> {
        let res = self.event_tx.try_send(Event::CaptureMemory);

        match res {
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) | Ok(()) => {
                Some(self.memory_snapshot.lock().clone().unwrap_or_default())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => None,
        }
    }
}

pub struct MessageProxy {
    options: ServerOptions,
    _queue_task_handle: tokio::task::JoinHandle<()>,
    event_tx: async_mpsc_channel::Sender<Event>,
    memory_snapshot: MemorySnapshot,
}

impl MessageProxy {
    pub fn new(options: ServerOptions) -> Self {
        Self::new_with_recv(options).0
    }

    fn new_with_recv(
        options: ServerOptions,
    ) -> (Self, async_broadcast_channel::Receiver<LogOrTableMsgProto>) {
        // CERULION PATCH (CER-959): a budget outside the usable band is accepted
        // and then inert on the byte axis. Say so once, at start, rather than
        // clamping it — a silent rewrite would hide the mistake.
        if let Some(why) = live_budget_band_complaint(options.live_temporal_budget_bytes) {
            re_log::warn!(
                "live_temporal_budget_bytes={} is outside the usable band ({} < n < {}): {why}.",
                options.live_temporal_budget_bytes.unwrap_or(0),
                LIVE_SMALL_MESSAGE_FLOOR_BYTES,
                CHANNEL_SIZE_BYTES,
            );
        }

        let (broadcast_log_tx, broadcast_log_rx) = async_broadcast_channel::channel(
            "re_grpc_server broadcast",
            CHANNEL_SIZE_MESSAGES,
            CHANNEL_SIZE_BYTES,
        );

        let (event_tx, event_rx) = {
            // TODO(emilk): this could also use a size-based backpressure mechanism.
            let message_queue_capacity = 32; // Apply backpressure early
            async_mpsc_channel::channel("re_grpc_server events", message_queue_capacity)
        };

        let memory_snapshot: MemorySnapshot = Default::default();

        let task_handle = tokio::spawn({
            let options = options.clone();
            let memory_snapshot = memory_snapshot.clone();
            async move {
                EventLoop::new(options, event_rx, broadcast_log_tx, memory_snapshot)
                    .run_in_place()
                    .await;
            }
        });

        (
            Self {
                options,
                _queue_task_handle: task_handle,
                event_tx,
                memory_snapshot,
            },
            broadcast_log_rx,
        )
    }

    pub fn handle(&self) -> MessageProxyHandle {
        MessageProxyHandle {
            event_tx: self.event_tx.clone(),
            memory_snapshot: self.memory_snapshot.clone(),
        }
    }

    pub fn viewer_control(&self) -> ViewerControl {
        ViewerControl {
            event_tx: self.event_tx.clone(),
        }
    }

    async fn push_message(&self, message: impl Into<LogOrTableMsgProto>) {
        let message = message.into();
        self.event_tx.send(Event::Message(message)).await.ok();
    }

    async fn new_client_message_stream(&self) -> ReadMsgStream {
        let (sender, receiver) = oneshot::channel();
        if let Err(err) = self.event_tx.send(Event::NewClient(sender)).await {
            re_log::error!("Error accepting new client: {err}");
            return Box::pin(tokio_stream::empty());
        }
        let (history, msg_channel) = match receiver.await {
            Ok(v) => v,
            Err(err) => {
                re_log::error!("Error accepting new client: {err}");
                return Box::pin(tokio_stream::empty());
            }
        };

        let history = tokio_stream::iter(
            history
                .into_iter()
                .map(ReadLogOrTableMsgResponse::from)
                .map(Ok),
        );

        // Convert our backpressure receiver into a Stream
        let channel = BackPressureReceiverStream::new(msg_channel).map(|result| {
            result.map(ReadLogOrTableMsgResponse::from).map_err(|err| {
                re_log::error!("Error reading message from broadcast channel: {err}");
                tonic::Status::internal(format!("internal channel error: {err}"))
            })
        });

        match self.options.playback_behavior {
            PlaybackBehavior::OldestFirst => Box::pin(history.chain(channel)), // NOLINT: Stream::chain
            PlaybackBehavior::NewestFirst => Box::pin(PriorityMerge::new(channel, history)),
        }
    }

    async fn new_client_log_stream(&self) -> ReadLogStream {
        Box::pin(
            self.new_client_message_stream()
                .await
                .filter_map(|msg| match msg {
                    Ok(ReadLogOrTableMsgResponse::LogMsg(msg)) => Some(Ok(msg)),
                    Ok(ReadLogOrTableMsgResponse::TableMsg(_)) => {
                        re_log::warn_once!("A log stream got a TableMsg");
                        None
                    }
                    Ok(ReadLogOrTableMsgResponse::UiCommand) => {
                        re_log::warn_once!("A log stream got a UiCommandMsg");
                        None
                    }
                    Err(err) => Some(Err(err)),
                }),
        )
    }

    async fn new_client_table_stream(&self) -> ReadTablesStream {
        Box::pin(
            self.new_client_message_stream()
                .await
                .filter_map(|msg| match msg {
                    Ok(ReadLogOrTableMsgResponse::LogMsg(_)) => {
                        re_log::warn_once!("A table stream got a LogMsg");
                        None
                    }
                    Ok(ReadLogOrTableMsgResponse::TableMsg(msg)) => Some(Ok(msg)),
                    Ok(ReadLogOrTableMsgResponse::UiCommand) => {
                        re_log::warn_once!("A log stream got a UiCommandMsg");
                        None
                    }
                    Err(err) => Some(Err(err)),
                }),
        )
    }
}

enum ReadLogOrTableMsgResponse {
    LogMsg(ReadMessagesResponse),
    TableMsg(ReadTablesResponse),
    UiCommand,
}

impl From<LogOrTableMsgProto> for ReadLogOrTableMsgResponse {
    fn from(proto: LogOrTableMsgProto) -> Self {
        match proto {
            LogOrTableMsgProto::LogMsg(log_msg) => Self::LogMsg(ReadMessagesResponse {
                log_msg: Some(log_msg),
            }),
            LogOrTableMsgProto::Table(table_msg) => Self::TableMsg(ReadTablesResponse {
                id: Some(table_msg.id),
                data: Some(table_msg.data),
            }),
            LogOrTableMsgProto::UiCommand(_ui_command) => Self::UiCommand,
        }
    }
}

type ReadLogStream = Pin<Box<dyn Stream<Item = tonic::Result<ReadMessagesResponse>> + Send>>;
type ReadTablesStream = Pin<Box<dyn Stream<Item = tonic::Result<ReadTablesResponse>> + Send>>;

type ReadMsgStream = Pin<Box<dyn Stream<Item = tonic::Result<ReadLogOrTableMsgResponse>> + Send>>;

#[tonic::async_trait]
impl message_proxy_service_server::MessageProxyService for MessageProxy {
    async fn write_messages(
        &self,
        request: tonic::Request<tonic::Streaming<WriteMessagesRequest>>,
    ) -> tonic::Result<tonic::Response<WriteMessagesResponse>> {
        let mut stream = request.into_inner();
        loop {
            match stream.message().await {
                Ok(Some(WriteMessagesRequest {
                    log_msg: Some(log_msg),
                })) => {
                    self.push_message(log_msg).await;
                }

                Ok(Some(WriteMessagesRequest { log_msg: None })) => {
                    re_log::warn!("missing log_msg in WriteMessagesRequest");
                }

                Ok(None) => {
                    // Connection was closed
                    break;
                }

                Err(err) => {
                    re_log::error!("Error while receiving messages: {}", TonicStatusError(err));
                    break;
                }
            }
        }

        Ok(tonic::Response::new(WriteMessagesResponse {}))
    }

    type ReadMessagesStream = ReadLogStream;

    async fn read_messages(
        &self,
        _: tonic::Request<ReadMessagesRequest>,
    ) -> tonic::Result<tonic::Response<Self::ReadMessagesStream>> {
        Ok(tonic::Response::new(self.new_client_log_stream().await))
    }

    type ReadTablesStream = ReadTablesStream;

    async fn write_table(
        &self,
        request: tonic::Request<WriteTableRequest>,
    ) -> tonic::Result<tonic::Response<WriteTableResponse>> {
        if let WriteTableRequest {
            id: Some(id),
            data: Some(data),
        } = request.into_inner()
        {
            self.push_message(TableMsgProto { id, data }).await;
        } else {
            re_log::warn!("malformed `WriteTableRequest`");
        }

        Ok(tonic::Response::new(WriteTableResponse {}))
    }

    async fn read_tables(
        &self,
        _: tonic::Request<ReadTablesRequest>,
    ) -> tonic::Result<tonic::Response<Self::ReadTablesStream>> {
        Ok(tonic::Response::new(self.new_client_table_stream().await))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::{Itertools as _, chain};
    use re_chunk::RowId;
    use re_log_encoding::rrd::Compression;
    use re_log_types::{LogMsg, SetStoreInfo, StoreId, StoreInfo, StoreKind, StoreSource};
    use re_protos::sdk_comms::v1alpha1::message_proxy_service_client::MessageProxyServiceClient;
    use re_protos::sdk_comms::v1alpha1::message_proxy_service_server::MessageProxyServiceServer;
    use similar_asserts::assert_eq;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Channel, Endpoint};

    use super::*;

    #[test]
    fn loopback_only_rejects_non_loopback_peers() {
        use tonic::service::Interceptor as _;
        use tonic::transport::server::TcpConnectInfo;

        fn request_from(remote_addr: Option<SocketAddr>) -> tonic::Request<()> {
            let mut request = tonic::Request::new(());
            if let Some(remote_addr) = remote_addr {
                request.extensions_mut().insert(TcpConnectInfo {
                    local_addr: None,
                    remote_addr: Some(remote_addr),
                });
            }
            request
        }

        let mut interceptor = LoopbackOnly;

        assert!(
            interceptor
                .call(request_from(Some("127.0.0.1:5000".parse().unwrap())))
                .is_ok()
        );
        assert!(
            interceptor
                .call(request_from(Some("[::1]:5000".parse().unwrap())))
                .is_ok()
        );

        assert_eq!(
            interceptor
                .call(request_from(Some("10.0.0.1:5000".parse().unwrap())))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            interceptor.call(request_from(None)).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[derive(Clone)]
    struct Completion(Arc<CancellationToken>);

    impl Drop for Completion {
        fn drop(&mut self) {
            self.finish();
        }
    }

    impl Completion {
        fn new() -> Self {
            Self(Arc::new(CancellationToken::new()))
        }

        fn finish(&self) {
            self.0.cancel();
        }

        async fn wait(&self) {
            self.0.cancelled().await;
        }
    }

    fn set_store_info_msg(store_id: &StoreId) -> LogMsg {
        LogMsg::SetStoreInfo(SetStoreInfo {
            row_id: *RowId::new(),
            info: StoreInfo::new(
                store_id.clone(),
                StoreSource::RustSdk {
                    rustc_version: String::new(),
                    llvm_version: String::new(),
                },
            ),
        })
    }

    /// Generates `n` log messages wrapped in a `SetStoreInfo` at the start and `BlueprintActivationCommand` at the end,
    /// to exercise message ordering.
    fn fake_log_stream_blueprint(n: usize) -> Vec<LogMsg> {
        let store_id = StoreId::random(StoreKind::Blueprint, "test_app");

        let mut messages = Vec::new();
        messages.push(set_store_info_msg(&store_id));
        for _ in 0..n {
            messages.push(LogMsg::ArrowMsg(
                store_id.clone(),
                re_chunk::Chunk::builder("test_entity")
                    .with_archetype(
                        re_chunk::RowId::new(),
                        re_log_types::TimePoint::default().with(
                            re_log_types::Timeline::new_sequence("blueprint"),
                            re_log_types::TimeInt::from_millis(re_log_types::NonMinI64::MIN),
                        ),
                        &re_sdk_types::blueprint::archetypes::Background::new(
                            re_sdk_types::blueprint::components::BackgroundKind::SolidColor,
                        )
                        .with_color([255, 0, 0]),
                    )
                    .build()
                    .unwrap()
                    .to_arrow_msg()
                    .unwrap(),
            ));
        }
        messages.push(LogMsg::BlueprintActivationCommand(
            re_log_types::BlueprintActivationCommand {
                blueprint_id: store_id,
                make_active: true,
                make_default: true,
            },
        ));

        messages
    }

    #[derive(Clone, Copy)]
    enum Temporalness {
        Static,
        Temporal,
    }

    fn fake_log_stream_recording(n: usize) -> Vec<LogMsg> {
        let store_id = StoreId::random(StoreKind::Recording, "test_app");

        chain!(
            [set_store_info_msg(&store_id)],
            generate_log_messages(&store_id, n, Temporalness::Temporal)
        )
        .collect()
    }

    fn generate_log_messages(
        store_id: &StoreId,
        n: usize,
        temporalness: Temporalness,
    ) -> Vec<LogMsg> {
        let mut messages = Vec::new();
        for _ in 0..n {
            let timepoint = match temporalness {
                Temporalness::Static => re_log_types::TimePoint::STATIC,
                Temporalness::Temporal => re_log_types::TimePoint::default().with(
                    re_log_types::Timeline::new_sequence("log_time"),
                    re_log_types::TimeInt::from_millis(re_log_types::NonMinI64::MIN),
                ),
            };

            messages.push(LogMsg::ArrowMsg(
                store_id.clone(),
                re_chunk::Chunk::builder("test_entity")
                    .with_archetype(
                        re_chunk::RowId::new(),
                        timepoint,
                        &re_sdk_types::archetypes::Points2D::new([
                            (0.0, 0.0),
                            (1.0, 1.0),
                            (2.0, 2.0),
                        ]),
                    )
                    .build()
                    .unwrap()
                    .to_arrow_msg()
                    .unwrap(),
            ));
        }
        messages
    }

    // CERULION PATCH (CER-858, F2): a single STATIC chunk for a NAMED entity path,
    // so tests can exercise the entity-level static dedup (`generate_log_messages`
    // hardcodes one entity, which would collapse under the dedup).
    fn static_msg_for_entity(store_id: &StoreId, entity: &str) -> LogMsg {
        LogMsg::ArrowMsg(
            store_id.clone(),
            re_chunk::Chunk::builder(entity)
                .with_archetype(
                    re_chunk::RowId::new(),
                    re_log_types::TimePoint::STATIC,
                    &re_sdk_types::archetypes::Points2D::new([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]),
                )
                .build()
                .unwrap()
                .to_arrow_msg()
                .unwrap(),
        )
    }

    // CERULION PATCH (CER-858, F1): a full blueprint stream for an EXPLICIT store
    // id (`SetStoreInfo` + `n_chunks` blueprint chunks + `BlueprintActivationCommand`),
    // so tests can assert per-store which blueprint survived eviction.
    fn blueprint_stream_for(store_id: &StoreId, n_chunks: usize) -> Vec<LogMsg> {
        let mut messages = vec![set_store_info_msg(store_id)];
        for _ in 0..n_chunks {
            messages.push(LogMsg::ArrowMsg(
                store_id.clone(),
                re_chunk::Chunk::builder("test_entity")
                    .with_archetype(
                        re_chunk::RowId::new(),
                        re_log_types::TimePoint::default().with(
                            re_log_types::Timeline::new_sequence("blueprint"),
                            re_log_types::TimeInt::from_millis(re_log_types::NonMinI64::MIN),
                        ),
                        &re_sdk_types::blueprint::archetypes::Background::new(
                            re_sdk_types::blueprint::components::BackgroundKind::SolidColor,
                        )
                        .with_color([255, 0, 0]),
                    )
                    .build()
                    .unwrap()
                    .to_arrow_msg()
                    .unwrap(),
            ));
        }
        messages.push(LogMsg::BlueprintActivationCommand(
            re_log_types::BlueprintActivationCommand {
                blueprint_id: store_id.clone(),
                make_active: true,
                make_default: true,
            },
        ));
        messages
    }

    // CERULION PATCH (CER-959): a TEMPORAL chunk on a timeline, sized by its
    // point count, so a test can straddle `LIVE_SMALL_MESSAGE_FLOOR_BYTES` with
    // real archetypes rather than synthetic byte counts. MEASURED encoded sizes
    // (`to_transport(LZ4)` → the proto the queue accounts): 1 point ≈ 1.2 KB
    // (chunk schema overhead dominates), 2000 points = 9308 B.
    fn temporal_points_msg(store_id: &StoreId, entity: &str, seq: i64, points: usize) -> LogMsg {
        let tp = re_log_types::TimePoint::default().with(
            re_log_types::Timeline::new_sequence("frame"),
            re_log_types::TimeInt::new_temporal(seq),
        );
        LogMsg::ArrowMsg(
            store_id.clone(),
            re_chunk::Chunk::builder(entity)
                .with_archetype(
                    RowId::new(),
                    tp,
                    &re_sdk_types::archetypes::Points3D::new(
                        (0..points)
                            .map(|i| (i as f32, 0.0, 0.0))
                            .collect::<Vec<_>>(),
                    ),
                )
                .build()
                .unwrap()
                .to_arrow_msg()
                .unwrap(),
        )
    }

    // CERULION PATCH (CER-858, F1/F2): encode an application `LogMsg` into the
    // transport proto the `MessageBuffer` actually stores (the exact production
    // encode path — `to_transport(LZ4)` then `.into()`), for direct buffer-level
    // unit tests.
    fn proto_of(msg: &LogMsg) -> LogOrTableMsgProto {
        use re_log_encoding::ToTransport as _;
        LogOrTableMsgProto::LogMsg(
            msg.to_transport(re_log_encoding::rrd::Compression::LZ4)
                .expect("encode LogMsg to transport proto")
                .into(),
        )
    }

    // CERULION PATCH (CER-858, F2): the inner transport `Msg` (which derives
    // `PartialEq`) of a buffered message, for byte-exact latest-wins assertions.
    fn inner_proto_msg(msg: &LogOrTableMsgProto) -> re_protos::log_msg::v1alpha1::log_msg::Msg {
        match msg {
            LogOrTableMsgProto::LogMsg(log_msg) => {
                log_msg.msg.clone().expect("log msg has an inner msg")
            }
            _ => panic!("expected a LogMsg variant"),
        }
    }

    async fn setup() -> (Completion, SocketAddr) {
        setup_opt(ServerOptions {
            playback_behavior: PlaybackBehavior::OldestFirst,
            memory_limit: MemoryLimit::UNLIMITED,
            cors_allowed_origins: vec![],
            drop_temporal_history: false,     // CERULION PATCH (CER-858)
            live_temporal_budget_bytes: None, // CERULION PATCH (CER-959)
        })
        .await
    }

    async fn setup_with_memory_limit(memory_limit: MemoryLimit) -> (Completion, SocketAddr) {
        setup_opt(ServerOptions {
            playback_behavior: PlaybackBehavior::OldestFirst,
            memory_limit,
            cors_allowed_origins: vec![],
            drop_temporal_history: false,     // CERULION PATCH (CER-858)
            live_temporal_budget_bytes: None, // CERULION PATCH (CER-959)
        })
        .await
    }

    // CERULION PATCH (CER-858): a proxy that DROPS temporal history (retain
    // persistent + static_ only) at an unlimited byte budget — the Studio
    // instant-only live-viz mode.
    async fn setup_drop_temporal() -> (Completion, SocketAddr) {
        setup_opt(ServerOptions {
            playback_behavior: PlaybackBehavior::NewestFirst,
            memory_limit: MemoryLimit::UNLIMITED,
            cors_allowed_origins: vec![],
            drop_temporal_history: true,
            live_temporal_budget_bytes: None, // CERULION PATCH (CER-959)
        })
        .await
    }

    async fn setup_opt(options: ServerOptions) -> (Completion, SocketAddr) {
        let completion = Completion::new();

        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();

        tokio::spawn({
            let completion = completion.clone();
            async move {
                tonic::transport::Server::builder()
                    // NOTE: This NODELAY very likely does nothing because of the call to
                    // `serve_with_incoming_shutdown` below, but we better be on the defensive here so
                    // we don't get surprised when things inevitably change.
                    .tcp_nodelay(true)
                    .accept_http1(true)
                    .http2_adaptive_window(Some(true)) // Optimize for throughput
                    .add_service(
                        MessageProxyServiceServer::new(super::MessageProxy::new(options))
                            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
                            .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE),
                    )
                    .serve_with_incoming_shutdown(
                        TcpIncoming::from(tcp_listener).with_nodelay(Some(true)),
                        completion.wait(),
                    )
                    .await
                    .unwrap();
            }
        });

        (completion, addr)
    }

    async fn make_client(addr: SocketAddr) -> MessageProxyServiceClient<Channel> {
        MessageProxyServiceClient::new(
            Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
        .max_decoding_message_size(crate::MAX_DECODING_MESSAGE_SIZE)
    }

    async fn write_messages(
        client: &mut MessageProxyServiceClient<Channel>,
        messages: Vec<LogMsg>,
    ) {
        client
            .write_messages(tokio_stream::iter(
                messages
                    .clone()
                    .into_iter()
                    .map(|msg| msg.to_transport(Compression::Off).unwrap())
                    .map(|msg| WriteMessagesRequest {
                        log_msg: Some(msg.into()),
                    }),
            ))
            .await
            .unwrap();
    }

    async fn read_log_stream(
        log_stream: &mut tonic::Response<tonic::Streaming<ReadMessagesResponse>>,
        n: usize,
    ) -> Vec<LogMsg> {
        let mut app_id_cache = re_log_encoding::CachingApplicationIdInjector::default();

        let mut stream_ref = log_stream.get_mut().map(|result| {
            let msg = result.unwrap().log_msg.unwrap().msg.unwrap();
            msg.to_application((&mut app_id_cache, None)).unwrap()
        });

        let mut messages = Vec::new();
        for _ in 0..n {
            messages.push(stream_ref.next().await.unwrap());
        }
        messages
    }

    #[tokio::test]
    async fn pubsub_basic() {
        let (completion, addr) = setup().await;
        let mut client = make_client(addr).await; // We use the same client for both producing and consuming
        let messages = fake_log_stream_blueprint(3);

        // start reading
        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();

        write_messages(&mut client, messages.clone()).await;

        // the messages should be echoed to us
        let actual = read_log_stream(&mut log_stream, messages.len()).await;

        assert_eq!(messages, actual);

        // While `SetStoreInfo` is sent first in `fake_log_stream`,
        // we can observe that it's also received first,
        // even though it is actually stored out of order in `persistent_message_queue`.
        assert!(matches!(messages[0], LogMsg::SetStoreInfo(..)));
        assert!(matches!(actual[0], LogMsg::SetStoreInfo(..)));

        completion.finish();
    }

    #[tokio::test]
    async fn pubsub_history() {
        let (completion, addr) = setup().await;
        let mut client = make_client(addr).await; // We use the same client for both producing and consuming
        let messages = fake_log_stream_blueprint(3);

        // don't read anything yet - these messages should be sent to us as part of history when we call `read_messages` later

        write_messages(&mut client, messages.clone()).await;

        // Start reading now - we should receive full history at this point:
        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();

        let actual = read_log_stream(&mut log_stream, messages.len()).await;
        assert_eq!(messages, actual);

        completion.finish();
    }

    #[tokio::test]
    async fn one_producer_many_consumers() {
        let (completion, addr) = setup().await;
        let mut producer = make_client(addr).await; // We use separate clients for producing and consuming
        let mut consumers = vec![make_client(addr).await, make_client(addr).await];
        let messages = fake_log_stream_blueprint(3);

        // Initialize multiple read streams:
        let mut log_streams = vec![];
        for consumer in &mut consumers {
            log_streams.push(
                consumer
                    .read_messages(ReadMessagesRequest {})
                    .await
                    .unwrap(),
            );
        }

        write_messages(&mut producer, messages.clone()).await;

        // Each consumer should've received them:
        for log_stream in &mut log_streams {
            let actual = read_log_stream(log_stream, messages.len()).await;
            assert_eq!(messages, actual);
        }

        completion.finish();
    }

    #[tokio::test]
    async fn many_producers_many_consumers() {
        let (completion, addr) = setup().await;
        let mut producers = vec![make_client(addr).await, make_client(addr).await];
        let mut consumers = vec![make_client(addr).await, make_client(addr).await];
        let messages = fake_log_stream_blueprint(3);

        // Initialize multiple read streams:
        let mut log_streams = vec![];
        for consumer in &mut consumers {
            log_streams.push(
                consumer
                    .read_messages(ReadMessagesRequest {})
                    .await
                    .unwrap(),
            );
        }

        // Write a few messages using each producer:
        for producer in &mut producers {
            write_messages(producer, messages.clone()).await;
        }

        let expected = [messages.clone(), messages.clone()].concat();

        // Each consumer should've received one set of messages from each producer.
        // Note that in this test we also guarantee the order of messages across producers,
        // due to the `write_messages` calls being sequential.

        for log_stream in &mut log_streams {
            let actual = read_log_stream(log_stream, expected.len()).await;
            assert_eq!(actual, expected);
        }

        completion.finish();
    }

    #[tokio::test]
    async fn memory_limit_drops_messages() {
        // Use an absurdly low memory limit to force all messages to be dropped immediately from history
        let (completion, addr) = setup_with_memory_limit(MemoryLimit::from_bytes(1)).await;
        let mut client = make_client(addr).await;
        let messages = fake_log_stream_recording(3);

        write_messages(&mut client, messages.clone()).await;

        // Start reading
        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();
        let mut actual = vec![];
        loop {
            let timeout_stream = log_stream.get_mut().timeout(Duration::from_millis(100));
            tokio::pin!(timeout_stream);
            let timeout_result = timeout_stream.try_next().await;
            let mut app_id_cache = re_log_encoding::CachingApplicationIdInjector::default();
            match timeout_result {
                Ok(Some(value)) => {
                    let msg = value.unwrap().log_msg.unwrap().msg.unwrap();
                    actual.push(msg.to_application((&mut app_id_cache, None)).unwrap());
                }

                // Stream closed | Timed out
                Ok(None) | Err(_) => break,
            }
        }

        // The GC runs _before_ a message is stored, so we should see the persistent message, and the last message sent.
        assert_eq!(actual.len(), 2);
        assert_eq!(&actual[0], &messages[0]);
        assert_eq!(&actual[1], messages.last().unwrap());

        completion.finish();
    }

    #[tokio::test]
    async fn memory_limit_does_not_drop_blueprint() {
        // Use an absurdly low memory limit to force all messages to be dropped immediately from history
        let (completion, addr) = setup_with_memory_limit(MemoryLimit::from_bytes(1)).await;
        let mut client = make_client(addr).await;
        let messages = fake_log_stream_blueprint(3);

        // Write some messages
        write_messages(&mut client, messages.clone()).await;

        // Start reading
        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();
        let mut actual = vec![];
        loop {
            let timeout_stream = log_stream.get_mut().timeout(Duration::from_millis(100));
            tokio::pin!(timeout_stream);
            let timeout_result = timeout_stream.try_next().await;
            let mut app_id_cache = re_log_encoding::CachingApplicationIdInjector::default();
            match timeout_result {
                Ok(Some(value)) => {
                    let msg = value.unwrap().log_msg.unwrap().msg.unwrap();
                    actual.push(msg.to_application((&mut app_id_cache, None)).unwrap());
                }

                // Stream closed | Timed out
                Ok(None) | Err(_) => break,
            }
        }

        // The stream in this case only contains SetStoreInfo, ArrowMsg with StoreKind::Blueprint,
        // and BlueprintActivationCommand. None of these things should be GC'd:
        assert_eq!(messages, actual);

        completion.finish();
    }

    // CERULION PATCH (CER-858): with `drop_temporal_history`, a fresh client's
    // connect-history carries the SetStoreInfo + static frames but NONE of the
    // temporal (recording) frames — zero temporal replay, at unlimited budget.
    #[tokio::test]
    async fn drop_temporal_history_retains_statics_not_temporal() {
        let (completion, addr) = setup_drop_temporal().await;
        let mut client = make_client(addr).await;

        let store_id = StoreId::random(StoreKind::Recording, "test_app");
        // CERULION PATCH (CER-858, F2): two statics under DISTINCT entity paths so
        // both survive the entity-level dedup (same-entity statics would collapse
        // to one — that dedup is pinned by `drop_temporal_dedups_static_relog_per_entity`).
        let statics = vec![
            static_msg_for_entity(&store_id, "entity_a"),
            static_msg_for_entity(&store_id, "entity_b"),
        ];
        let temporal = generate_log_messages(&store_id, 3, Temporalness::Temporal);
        let mut messages = vec![set_store_info_msg(&store_id)];
        messages.extend(statics.clone());
        messages.extend(temporal.clone());

        // Write everything with NO client reading yet → the temporal frames must
        // be dropped from history immediately (never buffered).
        write_messages(&mut client, messages).await;

        // Start reading → we should receive ONLY the persistent + static frames.
        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();
        let mut actual = vec![];
        loop {
            let timeout_stream = log_stream.get_mut().timeout(Duration::from_millis(100));
            tokio::pin!(timeout_stream);
            let mut app_id_cache = re_log_encoding::CachingApplicationIdInjector::default();
            match timeout_stream.try_next().await {
                Ok(Some(value)) => {
                    let msg = value.unwrap().log_msg.unwrap().msg.unwrap();
                    actual.push(msg.to_application((&mut app_id_cache, None)).unwrap());
                }
                Ok(None) | Err(_) => break,
            }
        }

        // Hand oracle: SetStoreInfo (persistent) + the 2 static frames survive;
        // every temporal frame is absent (dropped, not buffered).
        assert_eq!(
            actual.len(),
            3,
            "connect-history = SetStoreInfo + 2 statics, temporal dropped: {actual:?}"
        );
        assert!(matches!(actual[0], LogMsg::SetStoreInfo(..)));
        for t in &temporal {
            assert!(
                !actual.contains(t),
                "a temporal frame leaked into the drop-temporal connect-history"
            );
        }
        for s in &statics {
            assert!(actual.contains(s), "a static frame was wrongly dropped");
        }

        completion.finish();
    }

    // CERULION PATCH (CER-959): the loud-once / suppressed-repeat / recovery
    // policy for dropped live frames, against a hand-written oracle. A dropped
    // frame must never be SILENT (choppy video with no explanation) and never
    // FLOOD (this fires at frame rate for as long as a viewer is behind), so the
    // FIRST drop of each regime is loud and the rest are suppressed repeats
    // carrying the running total.
    /// Occupancy helper: `bytes` used, `messages` used, out of 1024 slots.
    fn occ(bytes: u64, messages: usize) -> LiveOccupancy {
        LiveOccupancy {
            bytes,
            messages,
            max_messages: CHANNEL_SIZE_MESSAGES,
        }
    }

    const BUDGET: u64 = 8 * 1024 * 1024;

    /// MEASURED encoded sizes of the two camera renditions a Cerulion attach
    /// interleaves, through the production encode path (`to_transport(LZ4)` → the
    /// proto the queue accounts). Both are far above
    /// [`LIVE_SMALL_MESSAGE_FLOOR_BYTES`], which is what makes them eligible for
    /// the byte axis at all — the drop-regime arms below use real frame sizes so
    /// the vectors describe traffic the gate is actually for.
    const FRAME_1280X720: u64 = 2_779_467;
    const FRAME_640X360: u64 = 696_500;

    #[test]
    fn a_drop_regime_is_loud_once_then_accumulates_until_it_closes() {
        // Regime 1: two drops. Only the first is loud; the total accumulates.
        assert_eq!(
            decide_live(Some(BUDGET), occ(BUDGET + 1, 4), true, FRAME_640X360, None),
            LiveDecision::Drop {
                regime_total: FRAME_640X360,
                loud: true
            },
            "the first drop opens the regime and is LOUD"
        );
        assert_eq!(
            decide_live(
                Some(BUDGET),
                occ(BUDGET + 1, 4),
                true,
                FRAME_1280X720,
                Some(FRAME_640X360)
            ),
            LiveDecision::Drop {
                regime_total: FRAME_640X360 + FRAME_1280X720,
                loud: false
            },
            "a repeat is suppressed and accumulates"
        );
        let regime = FRAME_640X360 + FRAME_1280X720;

        // HYSTERESIS: back under budget but ABOVE the low-water mark => admitted,
        // regime stays OPEN, nothing logged. Without this a viewer sitting at the
        // boundary pays a warn! + an info! per oscillation, at frame rate.
        assert_eq!(
            decide_live(
                Some(BUDGET),
                occ(BUDGET - 1, 4),
                true,
                FRAME_1280X720,
                Some(regime)
            ),
            LiveDecision::Admit {
                closes_regime: None
            },
            "inside the hysteresis band: admit, but do NOT declare recovery"
        );

        // Comfortably drained => the regime CLOSES, reporting what it cost.
        assert_eq!(
            decide_live(
                Some(BUDGET),
                occ(BUDGET / 4, 4),
                true,
                FRAME_1280X720,
                Some(regime)
            ),
            LiveDecision::Admit {
                closes_regime: Some(regime)
            },
            "past the low-water mark the regime closes and reports its total"
        );

        // RE-ARM: the next drop after a close is LOUD again and counts from zero.
        // A viewer that flaps must not go permanently silent after its first bad
        // patch — a regime that never closes turns every later incident into a
        // suppressed debug!.
        assert_eq!(
            decide_live(Some(BUDGET), occ(BUDGET + 1, 4), true, FRAME_640X360, None),
            LiveDecision::Drop {
                regime_total: FRAME_640X360,
                loud: true
            },
            "after recovery, the next drop is LOUD again"
        );

        // Saturates rather than wrapping.
        assert_eq!(
            decide_live(
                Some(BUDGET),
                occ(BUDGET + 1, 4),
                true,
                FRAME_1280X720,
                Some(u64::MAX)
            ),
            LiveDecision::Drop {
                regime_total: u64::MAX,
                loud: false
            },
            "saturates rather than wrapping"
        );
    }

    /// The MESSAGE axis. `send_async` awaits when EITHER quota is reached, and
    /// the event loop that would block also serves `Event::NewClient` — so a
    /// byte-only gate lets a stream of small messages (plots, `/tf`, telemetry)
    /// wedge the loop at a few MiB, far under any sane byte budget.
    #[test]
    fn the_message_quota_is_guarded_not_just_the_byte_budget() {
        let at_reserve = CHANNEL_SIZE_MESSAGES - LIVE_MESSAGE_RESERVE;
        // Tiny messages, trivial byte occupancy — the byte gate would never fire.
        assert_eq!(
            decide_live(Some(BUDGET), occ(4096, at_reserve), true, 2048, None),
            LiveDecision::Drop {
                regime_total: 2048,
                loud: true
            },
            "at the message reserve the gate must DROP even though bytes are trivial — otherwise \
             send_async awaits and the event loop (which serves Event::NewClient) wedges"
        );
        // One slot below the reserve: still admitted.
        assert_eq!(
            decide_live(Some(BUDGET), occ(4096, at_reserve - 1), true, 2048, None),
            LiveDecision::Admit {
                closes_regime: None
            },
            "one slot below the reserve is still admitted — a threshold, not a taper"
        );
        // Recovery needs BOTH axes: bytes fine, messages still high => stays open.
        assert_eq!(
            decide_live(Some(BUDGET), occ(0, at_reserve - 1), true, 8, Some(999)),
            LiveDecision::Admit {
                closes_regime: None
            },
            "a queue still deep in MESSAGES has not recovered, however few bytes it holds"
        );
    }

    /// The classes the budget may never touch, and the no-budget default.
    #[test]
    fn the_budget_never_touches_skeleton_control_or_an_unbudgeted_server() {
        // A hopelessly over-budget queue on both axes.
        let jammed = occ(BUDGET * 4, CHANNEL_SIZE_MESSAGES);

        assert_eq!(
            decide_live(Some(BUDGET), jammed, false, 1024, None),
            LiveDecision::Admit {
                closes_regime: None
            },
            "the scene skeleton (and tables / UI commands) is never dropped"
        );
        // ...and a non-temporal admission must NOT close a regime: only a temporal
        // message getting through proves the viewer caught up.
        assert_eq!(
            decide_live(Some(BUDGET), occ(0, 0), false, 1024, Some(500)),
            LiveDecision::Admit {
                closes_regime: None
            },
            "a skeleton message crossing an empty queue is not evidence the viewer recovered"
        );
        assert_eq!(
            decide_live(None, jammed, true, 1024, None),
            LiveDecision::Admit {
                closes_regime: None
            },
            "with no budget the gate is inert (stock re_grpc_server)"
        );
    }

    /// WIRING — the gate driven through the REAL [`EventLoop::handle_msg`] with a
    /// budget actually set, over a real broadcast channel whose receiver is never
    /// drained (the slowest possible viewer).
    ///
    /// Every other `ServerOptions` in this file sets `live_temporal_budget_bytes:
    /// None`, so before this arm the fork's own suite never executed the gate,
    /// `live_occupancy()`, `report_live_drop` or the floor even once — its whole
    /// CER-959 coverage was the pure `decide_live` oracles, which are handed a
    /// hand-written `LiveOccupancy` instead of reading the live channel. A wiring
    /// regression (a swapped axis, a floor compared against the wrong quantity, a
    /// budget never threaded through) left the suite green.
    /// A budget inside the usable band, small enough that a handful of 2000-point
    /// clouds (9308 B each, measured) crosses it.
    const WIRING_BUDGET: u64 = 16 * 1024;

    /// A REAL [`EventLoop`] with a budget set, over a real broadcast channel whose
    /// receiver is held and NEVER drained, driven until its live queue is over
    /// budget. Returns the loop, its store, the next unused sequence and the
    /// never-drained receiver (which must be kept alive: dropping it would make
    /// every send fail `NoReceivers`).
    async fn over_budget_event_loop() -> (
        EventLoop,
        StoreId,
        i64,
        async_broadcast_channel::Receiver<LogOrTableMsgProto>,
    ) {
        let (broadcast_tx, broadcast_rx) = async_broadcast_channel::channel(
            "cer959 wiring test",
            CHANNEL_SIZE_MESSAGES,
            CHANNEL_SIZE_BYTES,
        );
        let (_event_tx, event_rx) = async_mpsc_channel::channel("cer959 wiring events", 32);
        let mut loop_ = EventLoop::new(
            ServerOptions {
                playback_behavior: PlaybackBehavior::OldestFirst,
                memory_limit: MemoryLimit::ZERO,
                cors_allowed_origins: vec![],
                drop_temporal_history: true,
                live_temporal_budget_bytes: Some(WIRING_BUDGET),
            },
            event_rx,
            broadcast_tx,
            Default::default(),
        );

        let store = StoreId::random(StoreKind::Recording, "cer959wire");
        loop_
            .handle_msg(proto_of(&set_store_info_msg(&store)))
            .await;

        // Drive the queue over budget with CLOUD-class messages (above the floor).
        let mut seq = 0;
        while loop_.live_occupancy().bytes <= WIRING_BUDGET {
            loop_
                .handle_msg(proto_of(&temporal_points_msg(&store, "cloud", seq, 2000)))
                .await;
            seq += 1;
            assert!(seq < 64, "the queue should be over budget long before this");
        }
        assert_eq!(
            loop_.live_dropped_bytes, 0,
            "nothing may be dropped while the queue was still WITHIN budget"
        );
        (loop_, store, seq, broadcast_rx)
    }

    #[tokio::test]
    async fn the_live_budget_gate_is_wired_to_the_real_broadcast_queue() {
        let (mut loop_, store, seq, _rx) = over_budget_event_loop().await;

        // Over budget: a cloud is dropped. Its size is read off the
        // message that is actually handed over — the encoded size varies by a few
        // bytes with the row id, so a separately-built twin is not the same number.
        let dropped_log = temporal_points_msg(&store, "cloud", seq, 2000);
        // The public sizing helper must report EXACTLY what the queue accounts,
        // because a host uses it to check its own classes against the floor and
        // the budget. Tying it to the production accounting here (rather than to
        // its own encode) is what makes it a pin rather than a restatement.
        let advertised = live_queue_size_bytes(&dropped_log).expect("the message encodes");
        let dropped = proto_of(&dropped_log);
        let cloud_bytes = dropped.total_size_bytes();
        assert_eq!(
            advertised, cloud_bytes,
            "live_queue_size_bytes must report the same number the live queue accounts"
        );
        assert!(
            LIVE_SMALL_MESSAGE_FLOOR_BYTES <= cloud_bytes,
            "precondition: the cloud message must be at or above the floor, else this arm would \
             prove nothing about the byte axis. Got {cloud_bytes}"
        );
        loop_.handle_msg(dropped).await;
        assert_eq!(
            loop_.live_dropped_bytes, cloud_bytes,
            "an over-floor temporal message arriving at an over-budget queue must be dropped, and \
             counted"
        );
    }

    /// The two EXEMPTIONS at the production call site: a sub-floor message and the
    /// scene skeleton both cross an over-budget queue. Split from the arm above
    /// only because one body exceeded the crate's `too_many_lines` lint; it shares
    /// that arm's setup and its reason for existing.
    #[tokio::test]
    async fn the_floor_and_the_skeleton_exemption_are_wired_at_the_call_site() {
        let (mut loop_, store, seq, _rx) = over_budget_event_loop().await;
        // Establish the drop regime first, so the assertions below distinguish
        // "not dropped" from "nothing was ever dropped".
        let dropped = proto_of(&temporal_points_msg(&store, "cloud", seq, 2000));
        let cloud_bytes = dropped.total_size_bytes();
        loop_.handle_msg(dropped).await;
        assert_eq!(loop_.live_dropped_bytes, cloud_bytes);

        // A SMALL message still crosses (the floor)...
        let before = loop_.live_occupancy().bytes;
        let small = proto_of(&temporal_points_msg(&store, "plot", 1, 1));
        let small_bytes = small.total_size_bytes();
        assert!(
            small_bytes < LIVE_SMALL_MESSAGE_FLOOR_BYTES,
            "precondition: the plot message must be under the floor. Got {small_bytes}"
        );
        loop_.handle_msg(small).await;
        assert_eq!(
            loop_.live_dropped_bytes, cloud_bytes,
            "a SUB-FLOOR temporal message must still cross an over-budget queue — nothing further \
             may be counted as dropped"
        );
        assert_eq!(
            loop_.live_occupancy().bytes,
            before + small_bytes,
            "and it must really be in the live queue, not merely uncounted"
        );

        let before = loop_.live_occupancy().bytes;
        // A BIG static (a robot mesh / a static point cloud is the real shape), so
        // that its delivery is explained by the skeleton exemption and not by the
        // small-message floor. `static_msg_for_entity`'s 3-point chunk is 1175 B,
        // i.e. under the floor, which would make this arm ambiguous.
        let stat = proto_of(&LogMsg::ArrowMsg(
            store.clone(),
            re_chunk::Chunk::builder("scene/model")
                .with_archetype(
                    RowId::new(),
                    re_log_types::TimePoint::STATIC,
                    &re_sdk_types::archetypes::Points3D::new(
                        (0..2000).map(|i| (i as f32, 0.0, 0.0)).collect::<Vec<_>>(),
                    ),
                )
                .build()
                .unwrap()
                .to_arrow_msg()
                .unwrap(),
        ));
        let static_bytes = stat.total_size_bytes();
        assert!(
            LIVE_SMALL_MESSAGE_FLOOR_BYTES <= static_bytes,
            "precondition: the static must be at or ABOVE the floor, else its delivery would be \
             explained by the floor rather than by the skeleton exemption. Got {static_bytes}"
        );
        loop_.handle_msg(stat).await;
        assert_eq!(
            loop_.live_dropped_bytes, cloud_bytes,
            "the scene skeleton is never eligible for the drop, at any size"
        );
        assert_eq!(
            loop_.live_occupancy().bytes,
            before + static_bytes,
            "and the static must really be in the live queue"
        );
    }

    /// The SMALL-MESSAGE FLOOR, on both sides and on both axes.
    ///
    /// The byte axis compares the budget against OCCUPANCY, so without a floor a
    /// queue held over budget by megabyte-sized camera frames sheds every
    /// kilobyte-sized plot sample, `/tf` update and `Clear` sharing it — saving
    /// the viewer nothing measurable while permanently losing one-shot state
    /// transitions. The floor exempts those from THAT axis only.
    #[test]
    fn the_small_message_floor_exempts_the_byte_axis_and_only_the_byte_axis() {
        // Hopelessly over the BYTE budget, nowhere near the message reserve.
        let over_bytes = occ(BUDGET * 4, 4);

        assert_eq!(
            decide_live(
                Some(BUDGET),
                over_bytes,
                true,
                LIVE_SMALL_MESSAGE_FLOOR_BYTES - 1,
                None
            ),
            LiveDecision::Admit {
                closes_regime: None
            },
            "a message under the floor is never dropped for the QUEUE being over budget — it \
             cannot be what pushed it over, and it may be a one-shot state transition (a `Clear`) \
             that never comes again"
        );
        assert_eq!(
            decide_live(
                Some(BUDGET),
                over_bytes,
                true,
                LIVE_SMALL_MESSAGE_FLOOR_BYTES,
                None
            ),
            LiveDecision::Drop {
                regime_total: LIVE_SMALL_MESSAGE_FLOOR_BYTES,
                loud: true
            },
            "AT the floor the byte axis bites — a threshold, not a taper"
        );
        assert_eq!(
            decide_live(Some(BUDGET), over_bytes, true, 2_779_467, None),
            LiveDecision::Drop {
                regime_total: 2_779_467,
                loud: true
            },
            "an image-class frame (1280x720 RGB8, measured encoded size) is still dropped — the \
             floor must not have disarmed the axis this budget exists for"
        );

        // The floor is BYTE-axis only. The message axis is what keeps `send_async`
        // from awaiting and the event loop (which serves `Event::NewClient`) from
        // wedging, so a flood of small messages must still be shed.
        let at_reserve = occ(4096, CHANNEL_SIZE_MESSAGES - LIVE_MESSAGE_RESERVE);
        assert_eq!(
            decide_live(Some(BUDGET), at_reserve, true, 1229, None),
            LiveDecision::Drop {
                regime_total: 1229,
                loud: true
            },
            "at the MESSAGE reserve even a sub-floor message is dropped — exempting it there \
             would re-open the wedge the reserve exists to prevent"
        );

        // A floor-admitted message must not be read as evidence the viewer caught
        // up: the queue it crossed is still over budget.
        assert_eq!(
            decide_live(Some(BUDGET), over_bytes, true, 1229, Some(500)),
            LiveDecision::Admit {
                closes_regime: None
            },
            "a small message crossing an over-budget queue does NOT close the regime"
        );
    }

    /// The usable band for a configured budget. Outside it the option is accepted
    /// and then inert on the byte axis — the failure a caller is least able to
    /// see, since the field is set and the message axis still works.
    #[test]
    fn a_budget_outside_the_usable_band_is_reported() {
        assert_eq!(
            live_budget_band_complaint(None),
            None,
            "no budget, no complaint"
        );
        assert_eq!(
            live_budget_band_complaint(Some(BUDGET)),
            None,
            "a budget inside the band is fine"
        );
        assert!(
            live_budget_band_complaint(Some(LIVE_SMALL_MESSAGE_FLOOR_BYTES)).is_some(),
            "AT the floor nothing can ever be dropped for bytes"
        );
        assert!(
            live_budget_band_complaint(Some(LIVE_SMALL_MESSAGE_FLOOR_BYTES + 1)).is_none(),
            "one byte above the floor is usable — a threshold, not a taper"
        );
        assert!(
            live_budget_band_complaint(Some(CHANNEL_SIZE_BYTES)).is_some(),
            "AT the channel's own quota the occupancy can never reach the budget"
        );
        assert!(
            live_budget_band_complaint(Some(CHANNEL_SIZE_BYTES - 1)).is_none(),
            "one byte below the channel quota is (barely) usable"
        );
    }

    /// One message always crosses an empty queue, however large it is — otherwise
    /// a payload bigger than the budget could never be delivered at all.
    #[test]
    fn a_single_oversized_message_still_crosses_an_empty_queue() {
        assert_eq!(
            decide_live(Some(BUDGET), occ(0, 0), true, BUDGET * 10, None),
            LiveDecision::Admit {
                closes_regime: None
            },
            "the gate compares the budget against the CURRENT occupancy, so an empty queue admits \
             a message of any size"
        );
    }

    // CERULION PATCH (CER-959): `is_temporal` decides which messages the live
    // budget may drop. Getting it wrong in the SKELETON direction is the
    // dangerous one — a dropped `SetStoreInfo`, blueprint or static leaves a
    // viewer unable to render at all — so this pins BOTH directions against a
    // hand-written oracle over the exact production encode path (`proto_of`).
    #[test]
    fn only_recording_data_on_a_timeline_is_temporal() {
        let rec = StoreId::random(StoreKind::Recording, "test_app");
        let bp = StoreId::random(StoreKind::Blueprint, "test_app");

        // SKELETON — never eligible for the live-budget drop.
        let skeleton: Vec<(&str, LogMsg)> = vec![
            ("recording SetStoreInfo", set_store_info_msg(&rec)),
            ("blueprint SetStoreInfo", set_store_info_msg(&bp)),
            ("static chunk", static_msg_for_entity(&rec, "entity_a")),
            (
                "blueprint chunk",
                generate_log_messages(&bp, 1, Temporalness::Temporal)
                    .pop()
                    .unwrap(),
            ),
            (
                "blueprint activation",
                LogMsg::BlueprintActivationCommand(re_log_types::BlueprintActivationCommand {
                    blueprint_id: bp.clone(),
                    make_active: true,
                    make_default: true,
                }),
            ),
        ];
        for (what, msg) in &skeleton {
            assert!(
                !is_temporal(&proto_of(msg)),
                "{what} is scene skeleton — it must NEVER be dropped by the live budget"
            );
        }

        // TEMPORAL — recording data on a timeline, the only eligible class.
        let temporal = generate_log_messages(&rec, 2, Temporalness::Temporal);
        for msg in &temporal {
            assert!(
                is_temporal(&proto_of(msg)),
                "a recording chunk on a timeline is temporal and IS eligible"
            );
        }

        // Anti-tautology: a blueprint chunk and a recording chunk are built by
        // the SAME helper and differ ONLY in store kind, so the skeleton arm
        // above cannot be passing because the helper produces something inert.
        assert_ne!(
            is_temporal(&proto_of(&skeleton[3].1)),
            is_temporal(&proto_of(&temporal[0])),
            "the blueprint/recording distinction is what the classifier reads"
        );
    }

    // CERULION PATCH (CER-858, F1): under `drop_temporal`, three sequential
    // blueprint sends (each a FRESH blueprint store — the Studio `set_blueprint`
    // churn shape) must leave `persistent` holding ONLY the last blueprint store's
    // messages plus the recording data-store `SetStoreInfo`. The superseded
    // blueprints are evicted (bounding growth, since `gc()` is OFF under the flag).
    #[test]
    fn drop_temporal_evicts_superseded_blueprint_stores() {
        let mut buffer = MessageBuffer {
            drop_temporal: true,
            ..Default::default()
        };

        // A recording data store's SetStoreInfo must survive across blueprint churn.
        let recording = StoreId::random(StoreKind::Recording, "test_app");
        let data_info = set_store_info_msg(&recording);
        buffer.add_msg(proto_of(&data_info));

        // Three sequential blueprint sends, each a distinct blueprint store.
        let b1 = StoreId::random(StoreKind::Blueprint, "test_app");
        let b2 = StoreId::random(StoreKind::Blueprint, "test_app");
        let b3 = StoreId::random(StoreKind::Blueprint, "test_app");
        for id in [&b1, &b2] {
            for m in blueprint_stream_for(id, 2) {
                buffer.add_msg(proto_of(&m));
            }
        }
        let bp3 = blueprint_stream_for(&b3, 2);
        for m in &bp3 {
            buffer.add_msg(proto_of(m));
        }

        // Hand oracle: persistent == [data SetStoreInfo] + the LAST blueprint's
        // messages (SetStoreInfo + 2 chunks + activation), in order.
        let expected_msgs: Vec<LogMsg> = std::iter::once(data_info.clone())
            .chain(bp3.iter().cloned())
            .collect();
        let expected_protos: Vec<LogOrTableMsgProto> = expected_msgs.iter().map(proto_of).collect();

        assert_eq!(
            buffer.persistent.queue.len(),
            expected_protos.len(),
            "persistent must hold only the data SetStoreInfo + the LAST blueprint"
        );
        // Blueprints never land in static_/disposable.
        assert_eq!(buffer.static_.queue.len(), 0);
        assert_eq!(buffer.disposable.queue.len(), 0);

        // Store-attribution oracle: bp1/bp2 fully evicted; only `recording` + b3.
        let rec_id = |sid: &StoreId| -> String {
            let proto: StoreIdProto = sid.clone().into();
            proto.recording_id
        };
        let expected_ids: Vec<String> = std::iter::once(rec_id(&recording))
            .chain(std::iter::repeat_n(rec_id(&b3), bp3.len()))
            .collect();
        let actual_ids: Vec<String> = buffer
            .persistent
            .iter()
            .map(|m| {
                MessageBuffer::message_store_id(m)
                    .expect("persistent message has a store id")
                    .recording_id
                    .clone()
            })
            .collect();
        assert_eq!(actual_ids, expected_ids, "wrong stores survived eviction");
        assert!(
            !actual_ids.contains(&rec_id(&b1)) && !actual_ids.contains(&rec_id(&b2)),
            "a superseded blueprint store leaked into persistent"
        );

        // Hand-computed size_bytes: sum of the retained messages' encoded sizes.
        let expected_bytes: u64 = expected_protos.iter().map(|m| m.total_size_bytes()).sum();
        assert_eq!(
            buffer.persistent.size_bytes, expected_bytes,
            "persistent size_bytes must equal the retained messages' summed size"
        );
    }

    // CERULION PATCH (CER-858, F1): eviction preserves the SURVIVING blueprint's
    // activation ordering — `SetStoreInfo(B)` before B's chunks before B's
    // `BlueprintActivationCommand`, in `all()` output — even after a PRIOR
    // blueprint store is evicted.
    #[test]
    fn drop_temporal_blueprint_eviction_preserves_activation_ordering() {
        use re_protos::log_msg::v1alpha1::log_msg::Msg;

        let mut buffer = MessageBuffer {
            drop_temporal: true,
            ..Default::default()
        };
        let b1 = StoreId::random(StoreKind::Blueprint, "test_app");
        let b2 = StoreId::random(StoreKind::Blueprint, "test_app");
        for m in blueprint_stream_for(&b1, 2) {
            buffer.add_msg(proto_of(&m));
        }
        for m in blueprint_stream_for(&b2, 2) {
            buffer.add_msg(proto_of(&m));
        }

        let all = buffer.all(PlaybackBehavior::OldestFirst);
        let b1_rec = {
            let proto: StoreIdProto = b1.clone().into();
            proto.recording_id
        };
        // No b1 message remains.
        for msg in &all {
            if let Some(id) = MessageBuffer::message_store_id(msg) {
                assert_ne!(id.recording_id, b1_rec, "b1 was not evicted");
            }
        }

        // Order: SetStoreInfo(b2), then the 2 chunks, then the activation.
        let kinds: Vec<&'static str> = all
            .iter()
            .map(|m| match inner_proto_msg(m) {
                Msg::SetStoreInfo(..) => "set_store_info",
                Msg::ArrowMsg(..) => "arrow",
                Msg::BlueprintActivationCommand(..) => "activation",
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["set_store_info", "arrow", "arrow", "activation"],
            "surviving blueprint's activation ordering must be preserved"
        );
    }

    // CERULION PATCH (CER-858, F2): under `drop_temporal`, re-logging a static for
    // the SAME entity replaces the prior one (latest-wins), while statics for
    // DIFFERENT entities are both retained. Bounds `static_` under reconnect
    // re-logs (the stock `gc()` that would cap it is OFF under the flag).
    #[test]
    fn drop_temporal_dedups_static_relog_per_entity() {
        let mut buffer = MessageBuffer {
            drop_temporal: true,
            ..Default::default()
        };
        let store = StoreId::random(StoreKind::Recording, "test_app");

        // Same entity logged twice → only the latest is retained.
        let first = static_msg_for_entity(&store, "robot/base");
        let second = static_msg_for_entity(&store, "robot/base");
        buffer.add_msg(proto_of(&first));
        buffer.add_msg(proto_of(&second));
        assert_eq!(
            buffer.static_.queue.len(),
            1,
            "a same-entity static re-log must dedup to one chunk"
        );

        // The retained chunk is the LATEST (second), not the first.
        let retained = buffer.static_.iter().next().unwrap();
        assert_eq!(
            inner_proto_msg(retained),
            inner_proto_msg(&proto_of(&second)),
            "the retained static must be the latest re-log"
        );
        assert_ne!(
            inner_proto_msg(retained),
            inner_proto_msg(&proto_of(&first)),
            "the earlier static must have been evicted"
        );
        // size_bytes reflects only the one retained chunk.
        assert_eq!(
            buffer.static_.size_bytes,
            proto_of(&second).total_size_bytes(),
            "static_ size_bytes must reflect the single retained chunk"
        );

        // A static for a DIFFERENT entity is retained alongside.
        let other = static_msg_for_entity(&store, "robot/arm");
        buffer.add_msg(proto_of(&other));
        assert_eq!(
            buffer.static_.queue.len(),
            2,
            "statics for distinct entities must both be retained"
        );
    }

    #[tokio::test]
    async fn memory_limit_does_not_interrupt_stream() {
        let memory_limits = [
            0, // Will actually disable the message buffer and GC logic. Good to test that!
            1, // An absurdly low memory limit to force all messages to be dropped immediately from history
        ];

        for memory_limit in memory_limits {
            let (completion, addr) =
                setup_with_memory_limit(MemoryLimit::from_bytes(memory_limit)).await;
            let mut client = make_client(addr).await; // We use the same client for both producing and consuming
            let messages = fake_log_stream_blueprint(3);

            // Start reading
            let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();

            write_messages(&mut client, messages.clone()).await;

            // The messages should be echoed to us, even though none of them will be stored in history
            let actual = read_log_stream(&mut log_stream, messages.len()).await;
            assert_eq!(messages, actual);

            completion.finish();
        }
    }

    #[tokio::test]
    async fn static_data_is_returned_first() {
        let (completion, addr) = setup_with_memory_limit(MemoryLimit::UNLIMITED).await;
        let mut client = make_client(addr).await;

        let store_id = StoreId::random(StoreKind::Recording, "test_app");

        let set_store_info = vec![set_store_info_msg(&store_id)];
        let first_static = generate_log_messages(&store_id, 3, Temporalness::Static);
        let first_temporal = generate_log_messages(&store_id, 3, Temporalness::Temporal);
        let second_static = generate_log_messages(&store_id, 3, Temporalness::Static);

        write_messages(&mut client, set_store_info.clone()).await;
        write_messages(&mut client, first_static.clone()).await;
        write_messages(&mut client, first_temporal.clone()).await;
        write_messages(&mut client, second_static.clone()).await;

        // All static data should always come before temporal data:
        let expected =
            itertools::chain!(set_store_info, first_static, second_static, first_temporal)
                .collect_vec();

        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();
        let actual = read_log_stream(&mut log_stream, expected.len()).await;

        assert_eq!(actual, expected);

        completion.finish();
    }

    #[tokio::test]
    async fn playback_newest_first() {
        let (completion, addr) = setup_opt(ServerOptions {
            playback_behavior: PlaybackBehavior::NewestFirst, // this is what we want to test
            memory_limit: MemoryLimit::UNLIMITED,
            cors_allowed_origins: vec![],
            drop_temporal_history: false,     // CERULION PATCH (CER-858)
            live_temporal_budget_bytes: None, // CERULION PATCH (CER-959)
        })
        .await;
        let mut client = make_client(addr).await;

        let store_id = StoreId::random(StoreKind::Recording, "test_app");

        let set_store_info = vec![set_store_info_msg(&store_id)];
        let first_statics = generate_log_messages(&store_id, 3, Temporalness::Static);
        let temporals = generate_log_messages(&store_id, 3, Temporalness::Temporal);
        let second_statics = generate_log_messages(&store_id, 3, Temporalness::Static);

        write_messages(&mut client, set_store_info.clone()).await;
        write_messages(&mut client, first_statics.clone()).await;
        write_messages(&mut client, temporals.clone()).await;
        write_messages(&mut client, second_statics.clone()).await;

        // All static data should always come before temporal data:
        let expected = itertools::chain!(
            set_store_info.into_iter().rev(),
            second_statics.into_iter().rev(),
            first_statics.into_iter().rev(),
            temporals.into_iter().rev()
        )
        .collect_vec();

        let mut log_stream = client.read_messages(ReadMessagesRequest {}).await.unwrap();
        let actual = read_log_stream(&mut log_stream, expected.len()).await;

        assert_eq!(actual, expected);

        completion.finish();
    }

    mod cors_tests {
        use super::super::{DEFAULT_CORS_PATTERNS, is_origin_allowed};

        fn check(origin: &str, extra: &[&str]) -> bool {
            let patterns: Vec<wildmatch::WildMatch> =
                std::iter::chain(DEFAULT_CORS_PATTERNS.iter().copied(), extra.iter().copied())
                    .map(wildmatch::WildMatch::new)
                    .collect();
            is_origin_allowed(origin, &patterns)
        }

        #[test]
        fn default_allowed_origins() {
            assert!(check("http://localhost", &[]));
            assert!(check("http://localhost:8080", &[]));
            assert!(check("https://127.0.0.1", &[]));
            assert!(check("https://127.0.0.1:9090", &[]));
            assert!(check("https://rerun.io", &[]));
            assert!(check("https://rerun.io:443", &[]));
        }

        #[test]
        fn default_rejected_origins() {
            assert!(!check("https://evil.com", &[]));
            assert!(!check("https://notlocalhost.com", &[]));
            assert!(!check("https://localhost.evil.com", &[]));
        }

        #[test]
        fn extra_patterns() {
            assert!(check("https://app.example.com", &["https://*.example.com"]));
            assert!(!check("https://evil.com", &["https://*.example.com"]));

            // `?` is a bit of a footgun, you might think this could work but it doesn't:
            assert!(check("https://example.com", &["http?://example.com"]));
            assert!(!check("http://example.com", &["http?://example.com"]));

            // Port wildcard
            assert!(check(
                "https://example.com:8080",
                &["https://example.com:*"]
            ));
        }

        #[test]
        fn edge_cases() {
            assert!(!check("", &[]));
            assert!(!check("localhost", &[]));
            assert!(!check("evil.com", &[]));
        }
    }
}
