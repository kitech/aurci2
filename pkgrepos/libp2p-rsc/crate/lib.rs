//! Rust-C API over the libp2p crate (0.58). Full contract: detail.md
//!
//! Model: every swarm runs a background task; commands go in through the
//! API (blocking variants await a oneshot reply, `_async` variants return a
//! command_id), and all results are additionally delivered as JSON events
//! via l2p_next_event().

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use base64::Engine;
use futures::StreamExt;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::{DialOpts, NetworkBehaviour, SwarmEvent};
use libp2p::{
    autonat, dcutr, gossipsub, identify, kad, mdns, noise, ping, relay, request_response, tcp,
    upnp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

pub const LP2P_OK: i64 = 0;
pub const LP2P_ERR: i64 = -1;
pub const LP2P_EINVAL: i64 = -2;
pub const LP2P_EPANIC: i64 = -3;
pub const LP2P_ETIMEOUT: i64 = -4;
pub const LP2P_TIMEOUT_FOREVER: i64 = -1;

const EVENT_QUEUE_CAP: usize = 1024;

static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("l2p: failed to create tokio runtime")
});

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Must stay layout-compatible with `struct l2p_result` in libp2p-rsc.h.
#[repr(C)]
pub struct L2pResult {
    pub code: i64,
    pub errmsg: *mut c_char,
    pub data: *mut c_void,
    pub len: usize,
}

// ---------------------------------------------------------------------------
// result construction helpers
// ---------------------------------------------------------------------------

fn cstring_lossy(msg: &str) -> CString {
    CString::new(msg.replace('\0', "?"))
        .unwrap_or_else(|_| CString::new("l2p: error").expect("static string"))
}

fn set_last_error(msg: Option<&str>) {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = msg.map(cstring_lossy);
    });
}

fn ok_null() -> L2pResult {
    set_last_error(None);
    L2pResult { code: LP2P_OK, errmsg: ptr::null_mut(), data: ptr::null_mut(), len: 0 }
}

fn ok_str(s: &str) -> L2pResult {
    set_last_error(None);
    let len = s.len() + 1;
    let data = Box::into_raw((s + "\0").into_boxed_str()) as *mut c_char as *mut c_void;
    L2pResult { code: LP2P_OK, errmsg: ptr::null_mut(), data, len }
}

fn ok_bytes(bytes: Vec<u8>) -> L2pResult {
    set_last_error(None);
    if bytes.is_empty() {
        return ok_null();
    }
    let len = bytes.len();
    let data = Box::into_raw(bytes.into_boxed_slice()) as *mut u8 as *mut c_void;
    L2pResult { code: LP2P_OK, errmsg: ptr::null_mut(), data, len }
}

fn fail(code: i64, msg: String) -> L2pResult {
    set_last_error(Some(&msg));
    L2pResult { code, errmsg: cstring_lossy(&msg).into_raw(), data: ptr::null_mut(), len: 0 }
}

/// Invalid argument: no message, stale thread-local error is cleared.
fn einval() -> L2pResult {
    set_last_error(None);
    L2pResult { code: LP2P_EINVAL, errmsg: ptr::null_mut(), data: ptr::null_mut(), len: 0 }
}

fn guard<F: FnOnce() -> L2pResult>(fname: &'static str, f: F) -> L2pResult {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(_) => fail(LP2P_EPANIC, format!("{}: panic (bug, please report)", fname)),
    }
}

unsafe fn arg_str(p: *const c_char) -> Result<String, L2pResult> {
    if p.is_null() {
        return Err(einval());
    }
    Ok(CStr::from_ptr(p).to_string_lossy().into_owned())
}

unsafe fn swarm_ref<'a>(s: *mut L2pSwarm) -> Result<&'a L2pSwarm, L2pResult> {
    match s.as_mut() {
        Some(swarm) => Ok(swarm),
        None => Err(einval()),
    }
}

// ---------------------------------------------------------------------------
// payload exchange between FFI calls and the swarm task
// ---------------------------------------------------------------------------

enum Outcome {
    Null,
    Str(String),
    Bytes(Vec<u8>),
}

type Reply = Option<oneshot::Sender<Result<Outcome, String>>>;

struct Pending {
    id: u64,
    reply: Reply,
}

enum Command {
    Listen { addr: String, pending: Pending },
    StopListen { addr: String, pending: Pending },
    Dial { addr: String, pending: Pending },
    Disconnect { peer: PeerId, pending: Pending },
    Ping { peer: PeerId, pending: Pending },
    Identify { peer: PeerId, pending: Pending },
    PeerInfo { peer: PeerId, pending: Pending },
    GossipsubSubscribe { topic: String, pending: Pending },
    GossipsubUnsubscribe { topic: String, pending: Pending },
    GossipsubPublish { topic: String, data: Vec<u8>, pending: Pending },
    Request { peer: PeerId, data: Vec<u8>, pending: Pending },
    Respond { channel: u64, data: Option<Vec<u8>>, pending: Pending },
    KadAddAddress { peer: PeerId, addr: String, pending: Pending },
    KadBootstrap { pending: Pending },
    KadGetRecord { key: Vec<u8>, pending: Pending },
    KadPutRecord { key: Vec<u8>, value: Vec<u8>, pending: Pending },
    KadClosest { peer: PeerId, pending: Pending },
    KadProvide { key: Vec<u8>, pending: Pending },
    KadUnprovide { key: Vec<u8>, pending: Pending },
    KadSetMode { mode: String, pending: Pending },
    RelayReserve { relay_peer: PeerId, pending: Pending },
    RelayCancel { relay_peer: PeerId, pending: Pending },
    Shutdown(oneshot::Sender<()>),
}

pub struct L2pSwarm {
    cmd_tx: mpsc::UnboundedSender<Command>,
    ev_rx: Mutex<std::sync::mpsc::Receiver<String>>,
    next_id: AtomicU64,
    local_peer_id: String,
}

fn addr_with_peer(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| {
        if let Protocol::P2p(id) = p {
            Some(id)
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
struct Config {
    listen: Option<Vec<String>>,
    enable_mdns: Option<bool>,
    enable_kad: Option<bool>,
    kad_mode: Option<String>,
    enable_identify: Option<bool>,
    enable_ping: Option<bool>,
    enable_gossipsub: Option<bool>,
    enable_request_response: Option<bool>,
    request_response_protocol: Option<String>,
    enable_relay_client: Option<bool>,
    enable_relay_server: Option<bool>,
    enable_dcutr: Option<bool>,
    enable_autonat: Option<bool>,
    enable_upnp: Option<bool>,
    agent_version: Option<String>,
    idle_connection_timeout_secs: Option<u64>,
    ping_interval_secs: Option<u64>,
    gossipsub_heartbeat_ms: Option<u64>,
}

fn opt_bool(v: Option<bool>, default: bool) -> bool {
    v.unwrap_or(default)
}

// ---------------------------------------------------------------------------
// behaviour & swarm construction
// ---------------------------------------------------------------------------

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p_swarm::derive_prelude")]
struct Behaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    gossipsub: gossipsub::Behaviour,
    kad: kad::Behaviour,
    request_response: request_response::Behaviour<request_response::cbor::codec::Codec<Vec<u8>, Vec<u8>>>,
    mdns: mdns::tokio::Behaviour,
    relay_client: relay::client::Behaviour,
    relay_server: relay::Behaviour,
    dcutr: dcutr::Behaviour,
    autonat: autonat::v1::Behaviour,
    upnp: upnp::tokio::Behaviour,
}

fn parse_addr(s: &str) -> Result<Multiaddr, String> {
    s.parse::<Multiaddr>()
        .map_err(|e| format!("invalid multiaddr '{}': {}", s, e))
}

fn parse_peer(s: &str) -> Result<PeerId, String> {
    s.parse::<PeerId>()
        .map_err(|e| format!("invalid peer id '{}': {}", s, e))
}

fn parse_key(hex_str: &str) -> Result<libp2p::identity::Keypair, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("invalid key hex: {}", e))?;
    libp2p::identity::Keypair::from_protobuf_encoding(&bytes)
        .map_err(|e| format!("invalid key bytes: {}", e))
}

fn build_swarm(
    keypair: libp2p::identity::Keypair,
    cfg: &Config,
) -> Result<Swarm<Behaviour>, String> {
    let local_peer_id = keypair.public().to_peer_id();

    let mut ping_cfg = ping::Config::new();
    if let Some(secs) = cfg.ping_interval_secs {
        ping_cfg = ping_cfg.with_interval(Duration::from_secs(secs));
    }

    let mut identify_cfg = identify::Config::new("l2p/1.0".to_string(), keypair.public());
    if let Some(agent) = &cfg.agent_version {
        identify_cfg = identify_cfg.with_agent_version(agent.clone());
    }

    let mut gossipsub_cfg = gossipsub::Config::default();
    if let Some(ms) = cfg.gossipsub_heartbeat_ms {
        gossipsub_cfg.heartbeat_interval = Duration::from_millis(ms);
    }

    let gossipsub = if opt_bool(cfg.enable_gossipsub, true) {
        Some(
            gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_cfg,
            )
            .map_err(|e| format!("gossipsub init: {}", e))?,
        )
    } else {
        None
    };

    let rr_codec = request_response::cbor::codec::Codec::<Vec<u8>, Vec<u8>>::default();
    let request_response = if opt_bool(cfg.enable_request_response, false) {
        Some(request_response::Behaviour::with_codec(
            rr_codec,
            std::iter::once(StreamProtocol::new(
                cfg.request_response_protocol.as_deref().unwrap_or("/l2p/req/1"),
            )),
            request_response::Config::default(),
        ))
    } else {
        None
    };

    let mdns_b = if opt_bool(cfg.enable_mdns, false) {
        Some(
            mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                .map_err(|e| format!("mdns init: {}", e))?,
        )
    } else {
        None
    };

    let relay_client = if opt_bool(cfg.enable_relay_client, false) {
        Some(relay::client::Behaviour::new(local_peer_id, Default::default()))
    } else {
        None
    };

    let behaviour = Behaviour {
        identify: opt_bool(cfg.enable_identify, true)
            .then(|| identify::Behaviour::new(identify_cfg)),
        ping: opt_bool(cfg.enable_ping, true).then(|| ping::Behaviour::new(ping_cfg)),
        gossipsub,
        kad: opt_bool(cfg.enable_kad, true).then(|| {
            let mut k = kad::Behaviour::new(
                local_peer_id,
                kad::store::MemoryStore::new(local_peer_id),
            );
            k.set_mode(if cfg.kad_mode.as_deref() == Some("server") {
                Some(kad::Mode::Server)
            } else {
                Some(kad::Mode::Client)
            });
            k
        }),
        request_response,
        mdns: mdns_b,
        relay_client,
        relay_server: opt_bool(cfg.enable_relay_server, false)
            .then(|| relay::Behaviour::new(local_peer_id, relay::Config::default())),
        dcutr: opt_bool(cfg.enable_dcutr, true).then(|| dcutr::Behaviour::new(local_peer_id)),
        autonat: opt_bool(cfg.enable_autonat, true).then(|| {
            autonat::v1::Behaviour::new(local_peer_id, autonat::v1::Config::default())
        }),
        upnp: opt_bool(cfg.enable_upnp, true).then(upnp::tokio::Behaviour::default),
    };

    let idle_secs = cfg.idle_connection_timeout_secs.unwrap_or(0);
    let idle = if idle_secs == 0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs(idle_secs)
    };

    let b = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| format!("tcp transport: {}", e))?
        .with_quic()
        .with_dns()
        .map_err(|e| format!("dns: {}", e))?;

    if behaviour.relay_client.is_some() {
        Ok(b.with_relay_client(noise::Config::new, yamux::Config::default)
            .map_err(|e| format!("relay client transport: {}", e))?
            .with_behaviour(|_, relay| Behaviour { relay_client: Some(relay), ..behaviour })
            .map_err(|e| format!("behaviour: {}", e))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(idle))
            .build())
    } else {
        Ok(b.with_behaviour(|_| Behaviour { relay_client: None, ..behaviour })
            .map_err(|e| format!("behaviour: {}", e))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(idle))
            .build())
    }
}

// ---------------------------------------------------------------------------
// swarm task (event loop)
// ---------------------------------------------------------------------------

struct TaskState {
    cmd_id_seed: u64,
    pending: HashMap<u64, Pending>,
    by_conn: HashMap<libp2p::swarm::ConnectionId, u64>,
    by_query: HashMap<kad::QueryId, u64>,
    by_req: HashMap<request_response::OutboundRequestId, u64>,
    ping_waiters: HashMap<PeerId, u64>,
    ident_waiters: HashMap<PeerId, u64>,
    ident_cache: HashMap<PeerId, serde_json::Value>,
    inbound: HashMap<u64, request_response::ResponseChannel<Vec<u8>>>,
    inbound_seed: u64,
    listeners: HashMap<String, libp2p::swarm::ListenerId>,
}

impl TaskState {
    fn next(&mut self) -> u64 {
        self.cmd_id_seed += 1;
        self.cmd_id_seed
    }

    fn push_event(&self, ev_tx: &std::sync::mpsc::SyncSender<String>, value: serde_json::Value) {
        if ev_tx.try_send(value.to_string()).is_err() {
            let _ = ev_tx.try_send(json!({"type":"overflow"}).to_string());
        }
    }

    /// Completes a pending command: oneshot first (if blocking), then the
    /// command_result event (so a timed-out blocking call still gets it).
    fn complete(&mut self, ev_tx: &std::sync::mpsc::SyncSender<String>, id: u64, outcome: Result<Outcome, String>) {
        let payload = match &outcome {
            Ok(Outcome::Str(s)) => json!({"data": s.clone()}),
            Ok(Outcome::Bytes(b)) => json!({"data_base64": base64::engine::general_purpose::STANDARD.encode(b)}),
            Err(_) => json!({}),
        };
        let ok = outcome.is_ok();
        if let Some(p) = self.pending.remove(&id) {
            if let Some(tx) = p.reply {
                let _ = tx.send(outcome);
            }
        }
        self.push_event(ev_tx, json!({"type":"command_result","command_id": id, "ok": ok}.merge(&payload)));
    }
}

use serde_json::Value;

trait MergeInPlace {
    fn merge(self, other: &Value) -> Value;
}
impl MergeInPlace for Value {
    fn merge(self, other: &Value) -> Value {
        match (self, other) {
            (Value::Object(mut a), Value::Object(b)) => {
                for (k, v) in b { a.insert(k.clone(), v.clone()); }
                Value::Object(a)
            }
            (a, _) => a,
        }
    }
}

// ---------------------------------------------------------------------------
// swarm task (event loop)
// ---------------------------------------------------------------------------

struct TaskState {
    pending: HashMap<u64, Pending>,
    by_conn: HashMap<libp2p::swarm::ConnectionId, u64>,
    by_listener: HashMap<libp2p::swarm::ListenerId, u64>,
    by_query: HashMap<kad::QueryId, u64>,
    by_req: HashMap<request_response::OutboundRequestId, u64>,
    ping_waiters: HashMap<PeerId, u64>,
    ident_waiters: HashMap<PeerId, u64>,
    relay_waiters: HashMap<PeerId, u64>,
    ident_cache: HashMap<PeerId, serde_json::Value>,
    inbound: HashMap<u64, request_response::ResponseChannel<Vec<u8>>>,
    inbound_seed: u64,
    listeners: HashMap<String, libp2p::swarm::ListenerId>,
}

impl TaskState {
    fn push_event(&self, ev_tx: &std::sync::mpsc::SyncSender<String>, value: serde_json::Value) {
        if ev_tx.try_send(value.to_string()).is_err() {
            let _ = ev_tx.try_send(json!({"type":"overflow"}).to_string());
        }
    }

    /// Completes a pending command: oneshot first (if blocking), then the
    /// command_result event (so a timed-out blocking call still gets it).
    fn complete(&mut self, ev_tx: &std::sync::mpsc::SyncSender<String>, id: u64, outcome: Result<Outcome, String>) {
        let ok = outcome.is_ok();
        let mut payload = serde_json::Map::new();
        match &outcome {
            Ok(Outcome::Str(s)) => { payload.insert("data".into(), Value::String(s.clone())); }
            Ok(Outcome::Bytes(b)) => { payload.insert("data_base64".into(), Value::String(base64::engine::general_purpose::STANDARD.encode(b))); }
            Err(_) => {}
        }
        if let Some(p) = self.pending.remove(&id) {
            if let Some(tx) = p.reply {
                let _ = tx.send(outcome);
            }
        }
        let mut ev = serde_json::Map::new();
        ev.insert("type".into(), Value::String("command_result".into()));
        ev.insert("command_id".into(), json!(id));
        ev.insert("ok".into(), json!(ok));
        for (k, v) in payload { ev.insert(k, v); }
        self.push_event(ev_tx, Value::Object(ev));
    }
}

async fn run_task(
    mut swarm: Swarm<Behaviour>,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    ev_tx: std::sync::mpsc::SyncSender<String>,
) {
    let mut st = TaskState {
        pending: HashMap::new(),
        by_conn: HashMap::new(),
        by_listener: HashMap::new(),
        by_query: HashMap::new(),
        by_req: HashMap::new(),
        ping_waiters: HashMap::new(),
        ident_waiters: HashMap::new(),
        relay_waiters: HashMap::new(),
        ident_cache: HashMap::new(),
        inbound: HashMap::new(),
        inbound_seed: 0,
        listeners: HashMap::new(),
    };
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break,
                    Some(Command::Shutdown(ack)) => { let _ = ack.send(()); break; }
                    Some(c) => handle_command(&mut swarm, c, &mut st, &ev_tx),
                }
            }
            ev = swarm.select_next_some() => on_event(&mut swarm, ev, &mut st, &ev_tx),
        }
    }
}

fn info_json(info: &identify::Info) -> serde_json::Value {
    json!({
        "protocol_version": info.protocol_version,
        "agent_version": info.agent_version,
        "public_key_hex": hex::encode(info.public_key.to_protobuf_encoding()),
        "listen_addrs": info.listen_addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        "protocols": info.protocols.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        "observed_addr": info.observed_addr.to_string(),
    })
}

fn reply_now(st: &mut TaskState, ev_tx: &std::sync::mpsc::SyncSender<String>, pending: Pending, outcome: Result<Outcome, String>) {
    if let Some(tx) = pending.reply {
        let _ = tx.send(outcome);
    }
    let _ = st;
    let _ = ev_tx;
}

fn handle_command(swarm: &mut Swarm<Behaviour>, cmd: Command, st: &mut TaskState, ev_tx: &std::sync::mpsc::SyncSender<String>) {
    match cmd {
        Command::Listen { addr, pending } => {
            match parse_addr(&addr).and_then(|a| swarm.listen_on(a).map_err(|e| e.to_string())) {
                Ok(listener_id) => {
                    st.listeners.insert(addr, listener_id);
                    st.by_listener.insert(listener_id, pending.id);
                    st.pending.insert(pending.id, pending);
                }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_listen('{}'): {}", addr, e))),
            }
        }
        Command::StopListen { addr, pending } => {
            match st.listeners.remove(&addr) {
                Some(listener_id) => {
                    swarm.remove_listener(listener_id);
                    reply_now(st, ev_tx, pending, Ok(Outcome::Null));
                }
                None => reply_now(st, ev_tx, pending, Err(format!("l2p_stop_listen('{}'): not listening", addr))),
            }
        }
        Command::Dial { addr, pending } => {
            let addr_res = match parse_addr(&addr) {
                Ok(a) => a,
                Err(e) => { reply_now(st, ev_tx, pending, Err(format!("l2p_dial: {}", e))); return; }
            };
            let opts = match addr_with_peer(&addr_res) {
                Some(peer) => DialOpts::peer_id(peer).addresses(vec![addr_res]).build(),
                None => DialOpts::unknown_peer_id().address(addr_res).build(),
            };
            let conn_id = opts.connection_id();
            match swarm.dial(opts) {
                Ok(()) => {
                    st.by_conn.insert(conn_id, pending.id);
                    st.pending.insert(pending.id, pending);
                }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_dial('{}'): {}", addr, e))),
            }
        }
        Command::Disconnect { peer, pending } => {
            match swarm.disconnect_peer_id(peer) {
                Ok(()) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                Err(()) => reply_now(st, ev_tx, pending, Err(format!("l2p_disconnect('{}'): not connected", peer))),
            }
        }
        Command::Ping { peer, pending } => {
            st.ping_waiters.insert(peer, pending.id);
            st.pending.insert(pending.id, pending);
        }
        Command::Identify { peer, pending } => {
            match st.ident_cache.get(&peer) {
                Some(info) => reply_now(st, ev_tx, pending, Ok(Outcome::Str(info.to_string()))),
                None => {
                    st.ident_waiters.insert(peer, pending.id);
                    st.pending.insert(pending.id, pending);
                }
            }
        }
        Command::PeerInfo { peer, pending } => {
            match st.ident_cache.get(&peer) {
                Some(info) => reply_now(st, ev_tx, pending, Ok(Outcome::Str(info.to_string()))),
                None => reply_now(st, ev_tx, pending, Err(format!("l2p_peer_info('{}'): no cached identify info", peer))),
            }
        }
        Command::GossipsubSubscribe { topic, pending } => {
            let t = gossipsub::IdentTopic::new(&topic);
            match swarm.behaviour_mut().gossipsub.as_mut().ok_or("gossipsub disabled").and_then(|g| g.subscribe(&t).map_err(|e| e.to_string())) {
                Ok(_) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_gossipsub_subscribe('{}'): {}", topic, e))),
            }
        }
        Command::GossipsubUnsubscribe { topic, pending } => {
            let t = gossipsub::IdentTopic::new(&topic);
            match swarm.behaviour_mut().gossipsub.as_mut().ok_or("gossipsub disabled").and_then(|g| g.unsubscribe(&t).map_err(|e| e.to_string())) {
                Ok(_) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_gossipsub_unsubscribe('{}'): {}", topic, e))),
            }
        }
        Command::GossipsubPublish { topic, data, pending } => {
            let t = gossipsub::IdentTopic::new(&topic);
            match swarm.behaviour_mut().gossipsub.as_mut().ok_or("gossipsub disabled").and_then(|g| g.publish(t, data).map_err(|e| e.to_string())) {
                Ok(_) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_gossipsub_publish('{}'): {}", topic, e))),
            }
        }
        Command::Request { peer, data, pending } => {
            match swarm.behaviour_mut().request_response.as_mut().ok_or("request-response disabled").map(|rr| rr.send_request(&peer, data)) {
                Ok(req_id) => {
                    st.by_req.insert(req_id, pending.id);
                    st.pending.insert(pending.id, pending);
                }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_request('{}'): {}", peer, e))),
            }
        }
        Command::Respond { channel, data, pending } => {
            match st.inbound.remove(&channel) {
                None => reply_now(st, ev_tx, pending, Err(format!("l2p_response({}): unknown channel", channel))),
                Some(ch) => {
                    match data {
                        None => { drop(ch); reply_now(st, ev_tx, pending, Ok(Outcome::Null)); } // decline
                        Some(d) => match ch.send_response(d) {
                            Ok(()) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                            Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_response({}): {}", channel, e))),
                        },
                    }
                }
            }
        }
        Command::KadAddAddress { peer, addr, pending } => {
            match parse_addr(&addr) {
                Ok(a) => match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.add_address(&peer, a)) {
                    Ok(_) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                    Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_add_address: {}", e))),
                },
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_add_address: {}", e))),
            }
        }
        Command::KadBootstrap { pending } => {
            match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").and_then(|k| k.bootstrap().map_err(|e| e.to_string())) {
                Ok(query_id) => { st.by_query.insert(query_id, pending.id); st.pending.insert(pending.id, pending); }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_bootstrap: {}", e))),
            }
        }
        Command::KadGetRecord { key, pending } => {
            match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.get_record(kad::RecordKey::new(&key))) {
                Ok(query_id) => { st.by_query.insert(query_id, pending.id); st.pending.insert(pending.id, pending); }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_get_record: {}", e))),
            }
        }
        Command::KadPutRecord { key, value, pending } => {
            match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.put_record(kad::Record::new(kad::RecordKey::new(&key), value))) {
                Ok(query_id) => { st.by_query.insert(query_id, pending.id); st.pending.insert(pending.id, pending); }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_put_record: {}", e))),
            }
        }
        Command::KadClosest { peer, pending } => {
            match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.get_closest_peers(peer)) {
                Ok(query_id) => { st.by_query.insert(query_id, pending.id); st.pending.insert(pending.id, pending); }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_get_closest_peers: {}", e))),
            }
        }
        Command::KadProvide { key, pending } => {
            match swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").and_then(|k| k.start_providing(kad::RecordKey::new(&key)).map_err(|e| e.to_string())) {
                Ok(query_id) => { st.by_query.insert(query_id, pending.id); st.pending.insert(pending.id, pending); }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_kad_start_providing: {}", e))),
            }
        }
        Command::KadUnprovide { key, pending } => {
            swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.stop_providing(kad::RecordKey::new(&key)))
                .map_err(|e| e.to_string())
                .map(|_| reply_now(st, ev_tx, pending, Ok(Outcome::Null)))
                .unwrap_or_else(|e| reply_now(st, ev_tx, pending, Err(format!("l2p_kad_stop_providing: {}", e))));
        }
        Command::KadSetMode { mode, pending } => {
            let m = match mode.as_str() { "server" => kad::Mode::Server, "client" => kad::Mode::Client, _ => { reply_now(st, ev_tx, pending, Err(format!("l2p_kad_set_mode('{}'): invalid mode", mode))); return; } };
            swarm.behaviour_mut().kad.as_mut().ok_or("kad disabled").map(|k| k.set_mode(Some(m)))
                .map_err(|e| e.to_string())
                .map(|_| reply_now(st, ev_tx, pending, Ok(Outcome::Null)))
                .unwrap_or_else(|e| reply_now(st, ev_tx, pending, Err(format!("l2p_kad_set_mode: {}", e))));
        }
        Command::RelayReserve { relay_peer, pending } => {
            // dial the relay's circuit address; reservation completes via
            // relay client event ReservationReqAccepted
            let addr_str = format!("/p2p-circuit/p2p/{}", relay_peer);
            match parse_addr(&addr_str) {
                Ok(a) => {
                    let opts = DialOpts::peer_id(relay_peer).addresses(vec![a]).build();
                    let conn_id = opts.connection_id();
                    match swarm.dial(opts) {
                        Ok(()) => {
                            st.by_conn.insert(conn_id, pending.id);
                            st.relay_waiters.insert(relay_peer, pending.id);
                            st.pending.insert(pending.id, pending);
                        }
                        Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_relay_reserve('{}'): {}", relay_peer, e))),
                    }
                }
                Err(e) => reply_now(st, ev_tx, pending, Err(format!("l2p_relay_reserve: {}", e))),
            }
        }
        Command::RelayCancel { relay_peer, pending } => {
            // relay client has no explicit cancel; drop the circuit by
            // disconnecting from the relay peer
            match swarm.disconnect_peer_id(relay_peer) {
                Ok(()) => reply_now(st, ev_tx, pending, Ok(Outcome::Null)),
                Err(()) => reply_now(st, ev_tx, pending, Err(format!("l2p_relay_cancel('{}'): not connected", relay_peer))),
            }
        }
        Command::Shutdown(_) => unreachable!(),
    }
}

fn on_event(
    swarm: &mut Swarm<Behaviour>,
    ev: SwarmEvent<BehaviourEvent<Behaviour>>,
    st: &mut TaskState,
    ev_tx: &std::sync::mpsc::SyncSender<String>,
) {
    match ev {
        SwarmEvent::NewListenAddr { listener_id, address, .. } => {
            if let Some(id) = st.by_listener.remove(&listener_id) {
                st.complete(ev_tx, id, Ok(Outcome::Str(address.to_string())));
            }
            st.push_event(ev_tx, json!({"type":"new_listen_addr","address": address.to_string(),"listener_id": listener_id}));
        }
        SwarmEvent::ExpiredListenAddr { listener_id, address, .. } => {
            if let Some(id) = st.by_listener.remove(&listener_id) {
                st.complete(ev_tx, id, Err(format!("l2p_listen: expired: {}", address)));
            }
            st.push_event(ev_tx, json!({"type":"expired_listen_addr","address": address.to_string(),"listener_id": listener_id}));
        }
        SwarmEvent::ListenerError { listener_id, error, .. } => {
            if let Some(id) = st.by_listener.remove(&listener_id) {
                st.complete(ev_tx, id, Err(format!("l2p_listen: {}", error)));
            }
            st.push_event(ev_tx, json!({"type":"listener_error","error": error.to_string(),"listener_id": listener_id}));
        }
        SwarmEvent::ListenerClosed { listener_id, reason, .. } => {
            st.push_event(ev_tx, json!({"type":"listener_closed","listener_id": listener_id,"reason": reason.map(|e| e.to_string()).unwrap_or_default()}));
        }
        SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, num_established, .. } => {
            if let Some(id) = st.by_conn.remove(&connection_id) {
                st.complete(ev_tx, id, Ok(Outcome::Null));
            }
            st.push_event(ev_tx, json!({
                "type":"connection_established","peer_id": peer_id.to_string(),
                "endpoint": { "dialer": endpoint.get_remote_address().to_string(), "listener": endpoint.get_local_address().map(|a| a.to_string()).unwrap_or_default() },
                "num_established": num_established,
            }));
        }
        SwarmEvent::ConnectionClosed { peer_id, connection_id, num_established, cause, .. } => {
            st.push_event(ev_tx, json!({
                "type":"connection_closed","peer_id": peer_id.to_string(),
                "num_established": num_established,
                "cause": cause.map(|c| c.to_string()).unwrap_or_default(),
            }));
            let _ = connection_id;
        }
        SwarmEvent::IncomingConnection { local_addr, send_back_addr, connection_id, .. } => {
            let _ = connection_id;
            st.push_event(ev_tx, json!({"type":"incoming_connection","local_addr": local_addr.to_string(),"send_back_addr": send_back_addr.to_string()}));
        }
        SwarmEvent::IncomingConnectionError { local_addr, send_back_addr, error, .. } => {
            st.push_event(ev_tx, json!({"type":"incoming_connection_error","local_addr": local_addr.map(|a| a.to_string()).unwrap_or_default(),"send_back_addr": send_back_addr.map(|a| a.to_string()).unwrap_or_default(),"error": error.to_string()}));
        }
        SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error, .. } => {
            if let Some(id) = st.by_conn.remove(&connection_id) {
                st.complete(ev_tx, id, Err(format!("l2p_dial: {}", error)));
            }
            st.push_event(ev_tx, json!({
                "type":"outgoing_connection_error","error": error.to_string(),
                "peer_id": peer_id.map(|p| p.to_string()).unwrap_or_default(),
            }));
        }
        SwarmEvent::Dialing { peer_id, connection_id, .. } => {
            let _ = connection_id;
            st.push_event(ev_tx, json!({"type":"dialing","peer_id": peer_id.map(|p| p.to_string()).unwrap_or_default()}));
        }
        SwarmEvent::NewExternalAddrCandidate { address } => {
            st.push_event(ev_tx, json!({"type":"new_external_addr_candidate","address": address.to_string()}));
        }
        SwarmEvent::ExternalAddrConfirmed { address } => {
            st.push_event(ev_tx, json!({"type":"external_addr_confirmed","address": address.to_string()}));
        }
        SwarmEvent::ExternalAddrExpired { address } => {
            st.push_event(ev_tx, json!({"type":"external_addr_expired","address": address.to_string()}));
        }
        SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
            st.push_event(ev_tx, json!({"type":"new_external_addr_of_peer","peer_id": peer_id.to_string(),"address": address.to_string()}));
        }
        SwarmEvent::Behaviour(be) => on_behaviour_event(be, st, ev_tx),
        other => {
            st.push_event(ev_tx, json!({"type":"other","debug": format!("{:?}", other)}));
        }
    }
}

fn on_behaviour_event(
    be: BehaviourEvent<Behaviour>,
    st: &mut TaskState,
    ev_tx: &std::sync::mpsc::SyncSender<String>,
) {
    match be {
        BehaviourEvent::Ping(ping::Event::Ping { peer_id, result, .. }) => {
            let (ok, rtt_ms) = match result {
                Ok(d) => (true, Some(d.as_secs_f64() * 1000.0)),
                Err(e) => (false, None),
            };
            if let Some(id) = st.ping_waiters.remove(&peer_id) {
                let outcome = match (ok, rtt_ms) {
                    (true, Some(ms)) => Ok(Outcome::Str(format!("{:.3}", ms))),
                    _ => Err(format!("l2p_ping('{}'): ping failed", peer_id)),
                };
                st.complete(ev_tx, id, outcome);
            }
            let mut ev = serde_json::Map::new();
            ev.insert("type".into(), Value::String("ping".into()));
            ev.insert("peer_id".into(), Value::String(peer_id.to_string()));
            if let Some(ms) = rtt_ms { ev.insert("rtt_ms".into(), json!(ms)); }
            else { ev.insert("error".into(), json!("ping failed")); }
            st.push_event(ev_tx, Value::Object(ev));
        }
        BehaviourEvent::Identify(identify::Event::Received { peer_id, info, rtt, .. }) => {
            let mut v = info_json(&info);
            if let Some(d) = rtt { v["rtt_ms"] = json!(d.as_secs_f64() * 1000.0); }
            st.ident_cache.insert(peer_id, v.clone());
            if let Some(id) = st.ident_waiters.remove(&peer_id) {
                st.complete(ev_tx, id, Ok(Outcome::Str(v.to_string())));
            }
            let mut ev = serde_json::Map::new();
            ev.insert("type".into(), Value::String("identify".into()));
            ev.insert("peer_id".into(), Value::String(peer_id.to_string()));
            if let Value::Object(m) = v { for (k, val) in m { ev.insert(k, val); } }
            st.push_event(ev_tx, Value::Object(ev));
        }
        BehaviourEvent::Identify(identify::Event::Error { peer_id, error, .. }) => {
            if let Some(id) = st.ident_waiters.remove(&peer_id) {
                st.complete(ev_tx, id, Err(format!("l2p_identify_peer('{}'): {}", peer_id, error)));
            }
            st.push_event(ev_tx, json!({"type":"identify_error","peer_id": peer_id.to_string(),"error": error.to_string()}));
        }
        BehaviourEvent::Gossipsub(gossipsub::Event::Message { propagation_source, message, .. }) => {
            st.push_event(ev_tx, json!({
                "type":"gossipsub_message",
                "topic": message.topic.to_string(),
                "source": message.source.map(|p| p.to_string()).unwrap_or_default(),
                "propagation_source": propagation_source.to_string(),
                "sequence_number": message.sequence_number,
                "data_base64": base64::engine::general_purpose::STANDARD.encode(&message.data),
            }));
        }
        BehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic }) => {
            st.push_event(ev_tx, json!({"type":"gossipsub_subscribed","peer_id": peer_id.to_string(),"topic": topic.to_string()}));
        }
        BehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic }) => {
            st.push_event(ev_tx, json!({"type":"gossipsub_unsubscribed","peer_id": peer_id.to_string(),"topic": topic.to_string()}));
        }
        BehaviourEvent::Gossipsub(other) => {
            st.push_event(ev_tx, json!({"type":"gossipsub_other","debug": format!("{:?}", other)}));
        }
        BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed { id, result, .. }) => {
            match result {
                kad::QueryResult::Bootstrap(Ok(_)) => st.complete(ev_tx, id, Ok(Outcome::Null)),
                kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(record))) => {
                    st.complete(ev_tx, id, Ok(Outcome::Bytes(record.record.value)));
                    st.push_event(ev_tx, json!({
                        "type":"kad_record","key_hex": hex::encode(record.record.key.as_ref()),
                        "data_base64": base64::engine::general_purpose::STANDARD.encode(&record.record.value),
                    }));
                }
                kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. })) => {}
                kad::QueryResult::GetRecord(Err(e)) => {
                    st.complete(ev_tx, id, Err(format!("l2p_kad_get_record: {}", e)));
                }
                kad::QueryResult::PutRecord(Ok(_)) => st.complete(ev_tx, id, Ok(Outcome::Null)),
                kad::QueryResult::PutRecord(Err(e)) => st.complete(ev_tx, id, Err(format!("l2p_kad_put_record: {}", e))),
                kad::QueryResult::GetClosestPeers(Ok(peers)) => {
                    let list: Vec<String> = peers.iter().map(|p| p.to_string()).collect();
                    st.complete(ev_tx, id, Ok(Outcome::Str(Value::Array(list.into_iter().map(Value::String)).to_string())));
                }
                kad::QueryResult::GetClosestPeers(Err(e)) => {
                    st.complete(ev_tx, id, Err(format!("l2p_kad_get_closest_peers: {}", e)));
                }
                kad::QueryResult::StartProviding(Ok(_)) => st.complete(ev_tx, id, Ok(Outcome::Null)),
                kad::QueryResult::StartProviding(Err(e)) => {
                    st.complete(ev_tx, id, Err(format!("l2p_kad_start_providing: {}", e)));
                }
                other => {
                    st.complete(ev_tx, id, Ok(Outcome::Null));
                    st.push_event(ev_tx, json!({"type":"kad_query_progressed","debug": format!("{:?}", other)}));
                }
            }
        }
        BehaviourEvent::Kad(kad::Event::RoutingUpdated { peer_id, addresses, .. }) => {
            st.push_event(ev_tx, json!({
                "type":"kad_routing_updated","peer_id": peer_id.to_string(),
                "addresses": addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            }));
        }
        BehaviourEvent::Kad(other) => {
            st.push_event(ev_tx, json!({"type":"kad_other","debug": format!("{:?}", other)}));
        }
        BehaviourEvent::RequestResponse(request_response::Event::Message { peer_id, message, .. }) => {
            match message {
                request_response::Message::Request { request_id, request, channel } => {
                    st.inbound_seed += 1;
                    let channel_id = st.inbound_seed;
                    st.inbound.insert(channel_id, channel);
                    st.push_event(ev_tx, json!({
                        "type":"request_response_inbound",
                        "peer_id": peer_id.to_string(),
                        "channel_id": channel_id,
                        "data_base64": base64::engine::general_purpose::STANDARD.encode(&request),
                    }));
                }
                request_response::Message::Response { request_id, response, .. } => {
                    if let Some(id) = st.by_req.remove(&request_id) {
                        st.complete(ev_tx, id, Ok(Outcome::Bytes(response)));
                    } else {
                        st.push_event(ev_tx, json!({"type":"request_response_outbound","debug":"response without pending command"}));
                    }
                }
            }
        }
        BehaviourEvent::Mdns(mdns::Event::Discovered(peers)) => {
            st.push_event(ev_tx, json!({
                "type":"mdns_discovered",
                "peers": peers.into_iter().map(|(p, a)| json!({"peer_id": p.to_string(),"addr": a.to_string()})).collect::<Vec<_>>(),
            }));
        }
        BehaviourEvent::Mdns(mdns::Event::Expired(peer_id)) => {
            st.push_event(ev_tx, json!({"type":"mdns_expired","peer_id": peer_id.to_string()}));
        }
        BehaviourEvent::RelayClient(relay::client::Event::ReservationReqAccepted { relay_peer_id, .. }) => {
            if let Some(id) = st.relay_waiters.remove(&relay_peer_id) {
                st.complete(ev_tx, id, Ok(Outcome::Null));
            }
            st.push_event(ev_tx, json!({"type":"relay_reservation_accepted","relay_peer_id": relay_peer_id.to_string()}));
        }
        BehaviourEvent::RelayClient(relay::client::Event::ReservationReqFailed { relay_peer_id, error, .. }) => {
            if let Some(id) = st.relay_waiters.remove(&relay_peer_id) {
                st.complete(ev_tx, id, Err(format!("l2p_relay_reserve('{}'): {:?}", relay_peer_id, error)));
            }
            st.push_event(ev_tx, json!({"type":"relay_reservation_failed","relay_peer_id": relay_peer_id.map(|p| p.to_string()).unwrap_or_default()}));
        }
        BehaviourEvent::RelayClient(relay::client::Event::ReservationReqDenied { relay_peer_id }) => {
            if let Some(id) = st.relay_waiters.remove(&relay_peer_id) {
                st.complete(ev_tx, id, Err(format!("l2p_relay_reserve('{}'): denied by relay", relay_peer_id)));
            }
            st.push_event(ev_tx, json!({"type":"relay_reservation_denied","relay_peer_id": relay_peer_id.to_string()}));
        }
        BehaviourEvent::RelayClient(relay::client::Event::ReservationTimedOut { relay_peer_id }) => {
            if let Some(id) = st.relay_waiters.remove(&relay_peer_id) {
                st.complete(ev_tx, id, Err(format!("l2p_relay_reserve('{}'): reservation timed out", relay_peer_id)));
            }
            st.push_event(ev_tx, json!({"type":"relay_reservation_timed_out","relay_peer_id": relay_peer_id.to_string()}));
        }
        BehaviourEvent::Upnp(upnp::Event::NewExternalAddr { .. }) => {
            st.push_event(ev_tx, json!({"type":"upnp_new_external_addr"}));
        }
        BehaviourEvent::Upnp(upnp::Event::GatewayNotFound) => {
            st.push_event(ev_tx, json!({"type":"upnp_gateway_not_found"}));
        }
        BehaviourEvent::Upnp(upnp::Event::NonRoutableGateway) => {
            st.push_event(ev_tx, json!({"type":"upnp_non_routable_gateway"}));
        }
        other => {
            st.push_event(ev_tx, json!({"type":"other","debug": format!("{:?}", other)}));
        }
    }
}

// ---------------------------------------------------------------------------
// FFI plumbing
// ---------------------------------------------------------------------------

fn outcome_to_l2p(outcome: Result<Outcome, String>) -> L2pResult {
    match outcome {
        Ok(Outcome::Null) => ok_null(),
        Ok(Outcome::Str(s)) => ok_str(&s),
        Ok(Outcome::Bytes(b)) => ok_bytes(b),
        Err(msg) => fail(LP2P_ERR, msg),
    }
}

fn send_async(
    swarm: &L2pSwarm,
    make: impl FnOnce(u64, Reply) -> Command,
) -> L2pResult {
    let id = swarm.next_id.fetch_add(1, Ordering::Relaxed);
    let (_, _) = (id, ());
    let cmd = make(id, None);
    if swarm.cmd_tx.send(cmd).is_err() {
        return fail(LP2P_ERR, "l2p: event loop terminated".to_string());
    }
    ok_str(&format!("{}", id))
}

fn send_blocking(
    swarm: &L2pSwarm,
    fname: &'static str,
    timeout_ms: i64,
    make: impl FnOnce(u64, Reply) -> Command,
) -> L2pResult {
    let id = swarm.next_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel::<Result<Outcome, String>>();
    let cmd = make(id, Some(tx));
    if swarm.cmd_tx.send(cmd).is_err() {
        return fail(LP2P_ERR, format!("{}: event loop terminated", fname));
    }
    let res = if timeout_ms < 0 {
        match RT.handle().block_on(rx) {
            Ok(outcome) => outcome,
            Err(_) => return fail(LP2P_ERR, format!("{}: event loop terminated", fname)),
        }
    } else {
        match RT.handle().block_on(async {
            tokio::time::timeout(Duration::from_millis(timeout_ms as u64), rx).await
        }) {
            Err(_) => return fail(LP2P_ETIMEOUT, format!("{}: timeout ({} ms); result will still arrive as a command_result event", fname, timeout_ms)),
            Ok(Err(_)) => return fail(LP2P_ERR, format!("{}: event loop terminated", fname)),
            Ok(Ok(outcome)) => outcome,
        }
    };
    outcome_to_l2p(res)
}

// ---------------------------------------------------------------------------
// exported API: identity keys
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_key_new() -> L2pResult {
    guard("l2p_key_new", || {
        let key = libp2p::identity::Keypair::generate_ed25519();
        let bytes = key.to_protobuf_encoding().expect("ed25519 encoding");
        ok_str(&hex::encode(bytes))
    })
}

#[no_mangle]
pub extern "C" fn l2p_key_from_bytes(key_hex: *const c_char) -> L2pResult {
    guard("l2p_key_from_bytes", || {
        let s = match unsafe { arg_str(key_hex) } {
            Ok(v) => v,
            Err(r) => return r,
        };
        match parse_key(&s) {
            Ok(key) => {
                let bytes = key.to_protobuf_encoding().expect("ed25519 encoding");
                ok_str(&hex::encode(bytes))
            }
            Err(e) => fail(LP2P_ERR, format!("l2p_key_from_bytes: {}", e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn l2p_key_peer_id(key_hex: *const c_char) -> L2pResult {
    guard("l2p_key_peer_id", || {
        let s = match unsafe { arg_str(key_hex) } {
            Ok(v) => v,
            Err(r) => return r,
        };
        match parse_key(&s) {
            Ok(key) => ok_str(&key.public().to_peer_id().to_string()),
            Err(e) => fail(LP2P_ERR, format!("l2p_key_peer_id: {}", e)),
        }
    })
}

// ---------------------------------------------------------------------------
// exported API: swarm lifecycle
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_swarm_new(
    key_hex: *const c_char,
    config_json: *const c_char,
    out: *mut *mut L2pSwarm,
) -> L2pResult {
    guard("l2p_swarm_new", || unsafe {
        if out.is_null() {
            return einval();
        }
        let key_s = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let cfg_s = match arg_str(config_json) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let keypair = match parse_key(&key_s) {
            Ok(k) => k,
            Err(e) => return fail(LP2P_ERR, format!("l2p_swarm_new: {}", e)),
        };
        let cfg: Config = if cfg_s.trim().is_empty() {
            Config::default()
        } else {
            match serde_json::from_str(&cfg_s) {
                Ok(c) => c,
                Err(e) => return fail(LP2P_ERR, format!("l2p_swarm_new: invalid config json: {}", e)),
            }
        };
        let listen_addrs = cfg.listen.clone().unwrap_or_default();
        let mut swarm = match build_swarm(keypair.clone(), &cfg) {
            Ok(s) => s,
            Err(e) => return fail(LP2P_ERR, format!("l2p_swarm_new: {}", e)),
        };
        let local_peer_id = swarm.local_peer_id().to_string();
        for addr in listen_addrs {
            match parse_addr(&addr).and_then(|a| swarm.listen_on(a).map_err(|e| e.to_string())) {
                Ok(_) => {}
                Err(e) => return fail(LP2P_ERR, format!("l2p_swarm_new: listen '{}': {}", addr, e)),
            }
        }
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (ev_tx, ev_rx) = std::sync::mpsc::sync_channel::<String>(EVENT_QUEUE_CAP);
        RT.spawn(run_task(swarm, cmd_rx, ev_tx));
        *out = Box::into_raw(Box::new(L2pSwarm {
            cmd_tx,
            ev_rx: Mutex::new(ev_rx),
            next_id: AtomicU64::new(1),
            local_peer_id,
        }));
        ok_null()
    })
}

#[no_mangle]
pub extern "C" fn l2p_swarm_local_peer_id(s: *mut L2pSwarm) -> L2pResult {
    guard("l2p_swarm_local_peer_id", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        ok_str(&swarm.local_peer_id)
    })
}

#[no_mangle]
pub extern "C" fn l2p_swarm_free(s: *mut L2pSwarm) {
    if s.is_null() {
        return;
    }
    unsafe {
        let swarm = Box::from_raw(s);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        if swarm.cmd_tx.send(Command::Shutdown(ack_tx)).is_ok() {
            let _ = RT.handle().block_on(async {
                tokio::time::timeout(Duration::from_secs(5), ack_rx).await
            });
        }
        drop(swarm);
    }
}

#[no_mangle]
pub extern "C" fn l2p_next_event(s: *mut L2pSwarm, timeout_ms: i64) -> L2pResult {
    guard("l2p_next_event", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let rx = swarm.ev_rx.lock().expect("l2p: event lock poisoned");
        let json = if timeout_ms < 0 {
            match rx.recv() {
                Ok(v) => v,
                Err(_) => return fail(LP2P_ERR, "l2p_next_event: event loop terminated".to_string()),
            }
        } else if timeout_ms == 0 {
            match rx.try_recv() {
                Ok(v) => v,
                Err(std::sync::mpsc::TryRecvError::Empty) => return ok_null(),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return fail(LP2P_ERR, "l2p_next_event: event loop terminated".to_string())
                }
            }
        } else {
            match rx.recv_timeout(Duration::from_millis(timeout_ms as u64)) {
                Ok(v) => v,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return ok_null(),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return fail(LP2P_ERR, "l2p_next_event: event loop terminated".to_string())
                }
            }
        };
        ok_str(&json)
    })
}

// ---------------------------------------------------------------------------
// exported API: network commands
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_listen(s: *mut L2pSwarm, addr: *const c_char, timeout_ms: i64) -> L2pResult {
    guard("l2p_listen", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_listen", timeout_ms, |id, reply| Command::Listen {
            addr: a.clone(),
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_listen_async(s: *mut L2pSwarm, addr: *const c_char) -> L2pResult {
    guard("l2p_listen_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_async(swarm, |id, reply| Command::Listen {
            addr: a.clone(),
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_stop_listen(s: *mut L2pSwarm, addr: *const c_char) -> L2pResult {
    guard("l2p_stop_listen", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_stop_listen", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::StopListen { addr: a.clone(), pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_dial(s: *mut L2pSwarm, addr: *const c_char, timeout_ms: i64) -> L2pResult {
    guard("l2p_dial", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_dial", timeout_ms, |id, reply| Command::Dial {
            addr: a.clone(),
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_dial_async(s: *mut L2pSwarm, addr: *const c_char) -> L2pResult {
    guard("l2p_dial_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_async(swarm, |id, reply| Command::Dial {
            addr: a.clone(),
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_disconnect(s: *mut L2pSwarm, peer_id: *const c_char) -> L2pResult {
    guard("l2p_disconnect", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_disconnect: {}", e)),
        };
        send_blocking(swarm, "l2p_disconnect", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::Disconnect { peer, pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_ping(s: *mut L2pSwarm, peer_id: *const c_char, timeout_ms: i64) -> L2pResult {
    guard("l2p_ping", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_ping: {}", e)),
        };
        send_blocking(swarm, "l2p_ping", timeout_ms, |id, reply| Command::Ping {
            peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_ping_async(s: *mut L2pSwarm, peer_id: *const c_char) -> L2pResult {
    guard("l2p_ping_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_ping_async: {}", e)),
        };
        send_async(swarm, |id, reply| Command::Ping {
            peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_identify_peer(s: *mut L2pSwarm, peer_id: *const c_char, timeout_ms: i64) -> L2pResult {
    guard("l2p_identify_peer", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_identify_peer: {}", e)),
        };
        send_blocking(swarm, "l2p_identify_peer", timeout_ms, |id, reply| Command::Identify {
            peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_identify_peer_async(s: *mut L2pSwarm, peer_id: *const c_char) -> L2pResult {
    guard("l2p_identify_peer_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_identify_peer_async: {}", e)),
        };
        send_async(swarm, |id, reply| Command::Identify {
            peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_peer_info(s: *mut L2pSwarm, peer_id: *const c_char) -> L2pResult {
    guard("l2p_peer_info", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_peer_info: {}", e)),
        };
        send_blocking(swarm, "l2p_peer_info", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::PeerInfo { peer, pending: Pending { id, reply } }
        })
    })
}

// ---------------------------------------------------------------------------
// exported API: gossipsub
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_gossipsub_subscribe(s: *mut L2pSwarm, topic: *const c_char) -> L2pResult {
    guard("l2p_gossipsub_subscribe", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let t = match arg_str(topic) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_gossipsub_subscribe", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::GossipsubSubscribe { topic: t.clone(), pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_gossipsub_unsubscribe(s: *mut L2pSwarm, topic: *const c_char) -> L2pResult {
    guard("l2p_gossipsub_unsubscribe", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let t = match arg_str(topic) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_gossipsub_unsubscribe", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::GossipsubUnsubscribe { topic: t.clone(), pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_gossipsub_publish(
    s: *mut L2pSwarm,
    topic: *const c_char,
    data: *const u8,
    len: usize,
) -> L2pResult {
    guard("l2p_gossipsub_publish", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let t = match arg_str(topic) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && data.is_null() {
            return einval();
        }
        let payload = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(data, len).to_vec()
        };
        send_blocking(swarm, "l2p_gossipsub_publish", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::GossipsubPublish {
                topic: t.clone(),
                data: payload,
                pending: Pending { id, reply },
            }
        })
    })
}

// ---------------------------------------------------------------------------
// exported API: request-response
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_request(
    s: *mut L2pSwarm,
    peer_id: *const c_char,
    req: *const u8,
    len: usize,
    timeout_ms: i64,
) -> L2pResult {
    guard("l2p_request", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && req.is_null() {
            return einval();
        }
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_request: {}", e)),
        };
        let payload = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(req, len).to_vec()
        };
        send_blocking(swarm, "l2p_request", timeout_ms, |id, reply| Command::Request {
            peer,
            data: payload,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_request_async(
    s: *mut L2pSwarm,
    peer_id: *const c_char,
    req: *const u8,
    len: usize,
) -> L2pResult {
    guard("l2p_request_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && req.is_null() {
            return einval();
        }
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_request_async: {}", e)),
        };
        let payload = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(req, len).to_vec()
        };
        send_async(swarm, |id, reply| Command::Request {
            peer,
            data: payload,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_response(
    s: *mut L2pSwarm,
    channel_id: i64,
    resp: *const u8,
    len: usize,
) -> L2pResult {
    guard("l2p_response", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if channel_id < 0 {
            return einval();
        }
        if len > 0 && resp.is_null() {
            return einval();
        }
        let payload = if len == 0 {
            None // decline (drop the channel)
        } else {
            Some(std::slice::from_raw_parts(resp, len).to_vec())
        };
        send_blocking(swarm, "l2p_response", LP2P_TIMEOUT_FOREVER, |id, reply| Command::Respond {
            channel: channel_id as u64,
            data: payload,
            pending: Pending { id, reply },
        })
    })
}

// ---------------------------------------------------------------------------
// exported API: kad DHT
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_kad_add_address(
    s: *mut L2pSwarm,
    peer_id: *const c_char,
    addr: *const c_char,
) -> L2pResult {
    guard("l2p_kad_add_address", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let a = match arg_str(addr) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_add_address: {}", e)),
        };
        send_blocking(swarm, "l2p_kad_add_address", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::KadAddAddress { peer, addr: a.clone(), pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_bootstrap(s: *mut L2pSwarm, timeout_ms: i64) -> L2pResult {
    guard("l2p_kad_bootstrap", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_kad_bootstrap", timeout_ms, |id, reply| {
            Command::KadBootstrap { pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_bootstrap_async(s: *mut L2pSwarm) -> L2pResult {
    guard("l2p_kad_bootstrap_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_async(swarm, |id, reply| Command::KadBootstrap { pending: Pending { id, reply } })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_get_record(s: *mut L2pSwarm, key_hex: *const c_char, timeout_ms: i64) -> L2pResult {
    guard("l2p_kad_get_record", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_get_record: invalid key hex: {}", e)),
        };
        send_blocking(swarm, "l2p_kad_get_record", timeout_ms, |id, reply| Command::KadGetRecord {
            key,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_get_record_async(s: *mut L2pSwarm, key_hex: *const c_char) -> L2pResult {
    guard("l2p_kad_get_record_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_get_record_async: invalid key hex: {}", e)),
        };
        send_async(swarm, |id, reply| Command::KadGetRecord {
            key,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_put_record(
    s: *mut L2pSwarm,
    key_hex: *const c_char,
    val: *const u8,
    len: usize,
    timeout_ms: i64,
) -> L2pResult {
    guard("l2p_kad_put_record", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && val.is_null() {
            return einval();
        }
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_put_record: invalid key hex: {}", e)),
        };
        let value = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(val, len).to_vec()
        };
        send_blocking(swarm, "l2p_kad_put_record", timeout_ms, |id, reply| Command::KadPutRecord {
            key,
            value,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_put_record_async(
    s: *mut L2pSwarm,
    key_hex: *const c_char,
    val: *const u8,
    len: usize,
) -> L2pResult {
    guard("l2p_kad_put_record_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && val.is_null() {
            return einval();
        }
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_put_record_async: invalid key hex: {}", e)),
        };
        let value = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(val, len).to_vec()
        };
        send_async(swarm, |id, reply| Command::KadPutRecord {
            key,
            value,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_get_closest_peers(
    s: *mut L2pSwarm,
    peer_id: *const c_char,
    timeout_ms: i64,
) -> L2pResult {
    guard("l2p_kad_get_closest_peers", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_get_closest_peers: {}", e)),
        };
        send_blocking(swarm, "l2p_kad_get_closest_peers", timeout_ms, |id, reply| {
            Command::KadClosest { peer, pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_get_closest_peers_async(s: *mut L2pSwarm, peer_id: *const c_char) -> L2pResult {
    guard("l2p_kad_get_closest_peers_async", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_get_closest_peers_async: {}", e)),
        };
        send_async(swarm, |id, reply| Command::KadClosest {
            peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_start_providing(s: *mut L2pSwarm, key_hex: *const c_char) -> L2pResult {
    guard("l2p_kad_start_providing", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_start_providing: invalid key hex: {}", e)),
        };
        send_blocking(swarm, "l2p_kad_start_providing", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::KadProvide { key, pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_stop_providing(s: *mut L2pSwarm, key_hex: *const c_char) -> L2pResult {
    guard("l2p_kad_stop_providing", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let k = match arg_str(key_hex) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let key = match hex::decode(&k) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_kad_stop_providing: invalid key hex: {}", e)),
        };
        send_blocking(swarm, "l2p_kad_stop_providing", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::KadUnprovide { key, pending: Pending { id, reply } }
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_kad_set_mode(s: *mut L2pSwarm, mode: *const c_char) -> L2pResult {
    guard("l2p_kad_set_mode", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let m = match arg_str(mode) {
            Ok(v) => v,
            Err(r) => return r,
        };
        send_blocking(swarm, "l2p_kad_set_mode", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::KadSetMode { mode: m.clone(), pending: Pending { id, reply } }
        })
    })
}

// ---------------------------------------------------------------------------
// exported API: relay
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_relay_reserve(
    s: *mut L2pSwarm,
    relay_peer_id: *const c_char,
    timeout_ms: i64,
) -> L2pResult {
    guard("l2p_relay_reserve", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(relay_peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let relay_peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_relay_reserve: {}", e)),
        };
        send_blocking(swarm, "l2p_relay_reserve", timeout_ms, |id, reply| Command::RelayReserve {
            relay_peer,
            pending: Pending { id, reply },
        })
    })
}

#[no_mangle]
pub extern "C" fn l2p_relay_cancel(s: *mut L2pSwarm, relay_peer_id: *const c_char) -> L2pResult {
    guard("l2p_relay_cancel", || unsafe {
        let swarm = match swarm_ref(s) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let p = match arg_str(relay_peer_id) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let relay_peer = match parse_peer(&p) {
            Ok(v) => v,
            Err(e) => return fail(LP2P_ERR, format!("l2p_relay_cancel: {}", e)),
        };
        send_blocking(swarm, "l2p_relay_cancel", LP2P_TIMEOUT_FOREVER, |id, reply| {
            Command::RelayCancel { relay_peer, pending: Pending { id, reply } }
        })
    })
}

// ---------------------------------------------------------------------------
// exported API: freeing & errors
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn l2p_free_result(r: *mut L2pResult) {
    if r.is_null() {
        return;
    }
    unsafe {
        let result = &mut *r;
        if !result.errmsg.is_null() {
            drop(CString::from_raw(result.errmsg));
            result.errmsg = ptr::null_mut();
        }
        if !result.data.is_null() {
            let slice = ptr::slice_from_raw_parts_mut(result.data as *mut u8, result.len);
            drop(Box::from_raw(slice));
            result.data = ptr::null_mut();
        }
        result.len = 0;
        result.code = LP2P_OK;
    }
}

#[no_mangle]
pub extern "C" fn l2p_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

#[no_mangle]
pub extern "C" fn l2p_free_bytes(p: *mut c_void, len: usize) {
    if p.is_null() {
        return;
    }
    unsafe {
        let slice = ptr::slice_from_raw_parts_mut(p as *mut u8, len);
        drop(Box::from_raw(slice));
    }
}

#[no_mangle]
pub extern "C" fn l2p_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match &*slot.borrow() {
        Some(msg) => msg.as_ptr(),
        None => ptr::null(),
    })
}

