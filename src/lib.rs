use anyhow::Context;
use futures::{
    future::Either,
    io::{AsyncRead, AsyncReadExt as FuturesReadExt, AsyncWrite, AsyncWriteExt as FuturesWriteExt},
    StreamExt,
};
use godot::{
    classes::{multiplayer_peer, IMultiplayerPeerExtension, MultiplayerPeerExtension},
    global::Error,
    prelude::*,
};
use libp2p::{
    autonat,
    core::muxing::StreamMuxerBox,
    dcutr, identify, identity, mdns, noise, ping, quic, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    yamux, Multiaddr, PeerId, StreamProtocol, Swarm, Transport,
};
use libp2p_stream as stream;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tunnel_core::forward_bidirectional;

const FORWARD_PROTOCOL: StreamProtocol = StreamProtocol::new("/p2p-node/tcp-forward/1");
const MAX_PACKET_SIZE: usize = 16 * 1024 * 1024;

type Packet = Vec<u8>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WireMode {
    Reliable = 0,
    Unreliable = 1,
    UnreliableOrdered = 2,
}

impl WireMode {
    fn from_godot(mode: multiplayer_peer::TransferMode) -> Self {
        if mode == multiplayer_peer::TransferMode::UNRELIABLE {
            Self::Unreliable
        } else if mode == multiplayer_peer::TransferMode::UNRELIABLE_ORDERED {
            Self::UnreliableOrdered
        } else {
            Self::Reliable
        }
    }
}

#[derive(Debug)]
struct OutgoingPacket {
    payload: Packet,
    mode: WireMode,
    target_peer: i32,
}

#[derive(Debug)]
struct IncomingPacket {
    payload: Packet,
    mode: WireMode,
    peer_id: i32,
}

type OutgoingSender = std::sync::mpsc::SyncSender<OutgoingPacket>;
type OutgoingReceiver = std::sync::mpsc::Receiver<OutgoingPacket>;
type IncomingSender = std::sync::mpsc::SyncSender<IncomingPacket>;
type IncomingReceiver = std::sync::mpsc::Receiver<IncomingPacket>;

#[derive(Clone, Debug)]
pub struct NetworkMetrics {
    pub ping_ms: Arc<AtomicU64>,
    pub bytes_sent: Arc<AtomicU64>,
    pub bytes_received: Arc<AtomicU64>,
    pub connection_type: Arc<RwLock<String>>,
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        Self {
            ping_ms: Arc::new(AtomicU64::new(0)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
            connection_type: Arc::new(RwLock::new("Disconnected".to_owned())),
        }
    }
}

impl NetworkMetrics {
    fn set_connection_type(&self, value: &str) {
        *self
            .connection_type
            .write()
            .expect("connection metrics lock poisoned") = value.to_owned();
    }
}

#[derive(Default)]
struct PeerIdMap {
    godot_to_libp2p: HashMap<i32, PeerId>,
    libp2p_to_godot: HashMap<PeerId, i32>,
    next_id: i32,
}

impl PeerIdMap {
    fn id_for(&mut self, peer: PeerId) -> i32 {
        if let Some(id) = self.libp2p_to_godot.get(&peer) {
            return *id;
        }
        let id = self.next_id.max(2);
        self.next_id = id.saturating_add(1);
        self.libp2p_to_godot.insert(peer, id);
        self.godot_to_libp2p.insert(id, peer);
        id
    }
}

#[derive(Clone)]
struct PacketEndpoint {
    incoming_tx: IncomingSender,
    outgoing_rx: Arc<Mutex<OutgoingReceiver>>,
    peers: Arc<RwLock<PeerIdMap>>,
    sessions: Arc<RwLock<HashMap<PeerId, mpsc::Sender<OutgoingPacket>>>>,
    metrics: NetworkMetrics,
}

/// A packet bridge between Godot's main thread and the Tokio/libp2p thread.
///
/// Packets are length-prefixed on the libp2p stream. The queues deliberately
/// contain complete packets, so Godot never observes partial Yamux reads.
struct PacketBridge {
    incoming: Arc<Mutex<IncomingReceiver>>,
    outgoing: OutgoingSender,
    peers: Arc<RwLock<PeerIdMap>>,
    metrics: NetworkMetrics,
}

impl PacketBridge {
    fn new(metrics: NetworkMetrics) -> (Self, PacketEndpoint) {
        let (incoming_tx, incoming_rx) = std::sync::mpsc::sync_channel(256);
        let (outgoing_tx, outgoing_rx) = std::sync::mpsc::sync_channel(256);
        let peers = Arc::new(RwLock::new(PeerIdMap {
            next_id: 2,
            ..Default::default()
        }));
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        (
            Self {
                incoming: Arc::new(Mutex::new(incoming_rx)),
                outgoing: outgoing_tx,
                peers: peers.clone(),
                metrics: metrics.clone(),
            },
            PacketEndpoint {
                incoming_tx,
                outgoing_rx: Arc::new(Mutex::new(outgoing_rx)),
                peers,
                sessions,
                metrics,
            },
        )
    }
}

async fn run_packet_session<S>(
    mut stream: S,
    endpoint: PacketEndpoint,
    remote_peer: PeerId,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (session_tx, mut session_rx) = mpsc::channel(256);
    endpoint
        .sessions
        .write()
        .expect("session map lock poisoned")
        .insert(remote_peer, session_tx);
    let godot_peer_id = endpoint
        .peers
        .write()
        .expect("peer map lock poisoned")
        .id_for(remote_peer);
    loop {
        tokio::select! {
            result = read_packet(&mut stream) => {
                let (payload, mode) = result?;
                endpoint.metrics.bytes_received.fetch_add(
                    payload.len() as u64,
                    Ordering::Relaxed,
                );
                let packet = IncomingPacket {
                    payload,
                    mode,
                    peer_id: godot_peer_id,
                };
                if mode == WireMode::Reliable {
                    endpoint.incoming_tx.send(packet)
                        .map_err(|_| anyhow::anyhow!("Godot packet queue closed"))?;
                } else {
                    let _ = endpoint.incoming_tx.try_send(packet);
                }
            }
            packet = session_rx.recv() => {
                let Some(packet) = packet else { break };
                write_packet(&mut stream, &packet).await?;
                endpoint.metrics.bytes_sent.fetch_add(
                    packet.payload.len() as u64,
                    Ordering::Relaxed,
                );
                stream.flush().await?;
            }
        }
    }
    endpoint
        .sessions
        .write()
        .expect("session map lock poisoned")
        .remove(&remote_peer);
    Ok(())
}

async fn run_packet_dispatcher(endpoint: PacketEndpoint) -> anyhow::Result<()> {
    loop {
        let packet = tokio::task::spawn_blocking({
            let outgoing = endpoint.outgoing_rx.clone();
            move || outgoing.lock().expect("packet queue mutex poisoned").recv()
        })
        .await??;
        let targets = {
            let peers = endpoint.peers.read().expect("peer map lock poisoned");
            if packet.target_peer == 0 {
                peers.libp2p_to_godot.keys().copied().collect::<Vec<_>>()
            } else {
                peers
                    .godot_to_libp2p
                    .get(&packet.target_peer)
                    .copied()
                    .into_iter()
                    .collect::<Vec<_>>()
            }
        };
        for peer in targets {
            let sender = endpoint
                .sessions
                .read()
                .expect("session map lock poisoned")
                .get(&peer)
                .cloned();
            if let Some(sender) = sender {
                if packet.mode == WireMode::Reliable {
                    sender
                        .send(OutgoingPacket {
                            payload: packet.payload.clone(),
                            mode: packet.mode,
                            target_peer: packet.target_peer,
                        })
                        .await
                        .map_err(|_| anyhow::anyhow!("QUIC peer session closed"))?;
                } else {
                    // libp2p's Swarm API exposes QUIC streams, not Quinn
                    // datagram handles. A bounded try_send provides the same
                    // game-facing drop-on-congestion semantics without
                    // blocking reliable traffic.
                    let _ = sender.try_send(OutgoingPacket {
                        payload: packet.payload.clone(),
                        mode: packet.mode,
                        target_peer: packet.target_peer,
                    });
                }
            }
        }
    }
}

async fn write_packet<S>(stream: &mut S, packet: &OutgoingPacket) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let length = u32::try_from(packet.payload.len()).context("packet exceeds framing limit")?;
    stream.write_all(&[packet.mode as u8]).await?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(&packet.payload).await?;
    Ok(())
}

async fn read_packet<S>(stream: &mut S) -> anyhow::Result<(Vec<u8>, WireMode)>
where
    S: AsyncRead + Unpin,
{
    let mut mode = [0_u8; 1];
    stream.read_exact(&mut mode).await?;
    let mode = match mode[0] {
        0 => WireMode::Reliable,
        1 => WireMode::Unreliable,
        2 => WireMode::UnreliableOrdered,
        value => anyhow::bail!("invalid packet transfer mode {value}"),
    };
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_PACKET_SIZE {
        anyhow::bail!("incoming packet exceeds maximum size");
    }
    let mut packet = vec![0_u8; length];
    stream.read_exact(&mut packet).await?;
    Ok((packet, mode))
}

/// Native Godot multiplayer peer backed by the existing libp2p/Yamux runtime.
///
/// `MultiplayerPeer` is final in Godot 4's extension API. Godot provides
/// `MultiplayerPeerExtension` specifically for native implementations; it
/// inherits `MultiplayerPeer` and forwards these virtual methods to Rust.
#[derive(GodotClass)]
#[class(tool, base=MultiplayerPeerExtension)]
pub struct Libp2pMultiplayerPeer {
    base: Base<MultiplayerPeerExtension>,
    incoming: Arc<Mutex<IncomingReceiver>>,
    outgoing: OutgoingSender,
    pending: VecDeque<IncomingPacket>,
    peers: Arc<RwLock<PeerIdMap>>,
    metrics: NetworkMetrics,
    packet_peer: i32,
    packet_channel: i32,
    transfer_channel: i32,
    transfer_mode: multiplayer_peer::TransferMode,
    target_peer: i32,
    status: multiplayer_peer::ConnectionStatus,
    unique_id: i32,
    refusing_connections: bool,
}

#[godot_api]
impl IMultiplayerPeerExtension for Libp2pMultiplayerPeer {
    fn init(base: Base<MultiplayerPeerExtension>) -> Self {
        let (bridge, _endpoint) = PacketBridge::new(NetworkMetrics::default());
        Self {
            base,
            incoming: bridge.incoming,
            outgoing: bridge.outgoing,
            pending: VecDeque::new(),
            peers: bridge.peers,
            metrics: bridge.metrics,
            packet_peer: 1,
            packet_channel: 0,
            transfer_channel: 0,
            transfer_mode: multiplayer_peer::TransferMode::RELIABLE,
            target_peer: 0,
            status: multiplayer_peer::ConnectionStatus::CONNECTING,
            unique_id: 1,
            refusing_connections: false,
        }
    }

    fn get_available_packet_count(&self) -> i32 {
        self.pending.len().try_into().unwrap_or(i32::MAX)
    }

    fn get_max_packet_size(&self) -> i32 {
        MAX_PACKET_SIZE as i32
    }

    fn get_packet_script(&mut self) -> PackedByteArray {
        self.pending
            .pop_front()
            .map(|packet| {
                self.packet_peer = packet.peer_id;
                self.transfer_mode = match packet.mode {
                    WireMode::Reliable => multiplayer_peer::TransferMode::RELIABLE,
                    WireMode::Unreliable => multiplayer_peer::TransferMode::UNRELIABLE,
                    WireMode::UnreliableOrdered => {
                        multiplayer_peer::TransferMode::UNRELIABLE_ORDERED
                    }
                };
                PackedByteArray::from(packet.payload)
            })
            .unwrap_or_default()
    }

    fn put_packet_script(&mut self, packet: PackedByteArray) -> Error {
        let bytes = packet.to_vec();
        if bytes.len() > MAX_PACKET_SIZE {
            return Error::ERR_OUT_OF_MEMORY;
        }
        match self.outgoing.try_send(OutgoingPacket {
            payload: bytes,
            mode: WireMode::from_godot(self.transfer_mode),
            target_peer: self.target_peer,
        }) {
            Ok(()) => Error::OK,
            Err(std::sync::mpsc::TrySendError::Full(_)) => Error::ERR_BUSY,
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => Error::ERR_UNAVAILABLE,
        }
    }

    fn get_packet_channel(&self) -> i32 {
        self.packet_channel
    }

    fn get_packet_mode(&self) -> multiplayer_peer::TransferMode {
        self.transfer_mode
    }

    fn set_transfer_channel(&mut self, channel: i32) {
        self.transfer_channel = channel;
    }

    fn get_transfer_channel(&self) -> i32 {
        self.transfer_channel
    }

    fn set_transfer_mode(&mut self, mode: multiplayer_peer::TransferMode) {
        self.transfer_mode = mode;
    }

    fn get_transfer_mode(&self) -> multiplayer_peer::TransferMode {
        self.transfer_mode
    }

    fn set_target_peer(&mut self, peer: i32) {
        if peer > 1
            && !self
                .peers
                .read()
                .expect("peer map lock poisoned")
                .godot_to_libp2p
                .contains_key(&peer)
        {
            godot_error!("unknown Godot peer id {peer}");
        }
        self.target_peer = peer;
    }

    fn get_packet_peer(&self) -> i32 {
        self.packet_peer
    }

    fn is_server(&self) -> bool {
        false
    }

    fn poll(&mut self) {
        if self
            .metrics
            .connection_type
            .read()
            .expect("connection metrics lock poisoned")
            .as_str()
            != "Disconnected"
        {
            self.status = multiplayer_peer::ConnectionStatus::CONNECTED;
        }
        let receiver = self.incoming.lock().expect("packet queue mutex poisoned");
        while let Ok(packet) = receiver.try_recv() {
            if packet.payload.len() <= MAX_PACKET_SIZE {
                self.pending.push_back(packet);
            }
        }
    }

    fn close(&mut self) {
        self.status = multiplayer_peer::ConnectionStatus::DISCONNECTED;
    }

    fn disconnect_peer(&mut self, _peer: i32, _force: bool) {}

    fn get_unique_id(&self) -> i32 {
        self.unique_id
    }

    fn set_refuse_new_connections(&mut self, enable: bool) {
        self.refusing_connections = enable;
    }

    fn is_refusing_new_connections(&self) -> bool {
        self.refusing_connections
    }

    fn is_server_relay_supported(&self) -> bool {
        true
    }

    fn get_connection_status(&self) -> multiplayer_peer::ConnectionStatus {
        self.status
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    autonat: autonat::Behaviour,
    dcutr: dcutr::Behaviour,
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    relay_client: relay::client::Behaviour,
    streams: stream::Behaviour,
}

enum Command {
    Host { port: u16 },
    Join { peer: PeerId, bind_port: u16 },
    AttachPeer { endpoint: PacketEndpoint },
}

#[derive(GodotClass)]
#[class(base=Node)]
pub struct P2PNetworkManager {
    base: Base<Node>,
    commands: Arc<Mutex<Option<mpsc::UnboundedSender<Command>>>>,
    peer_id: String,
    metrics: NetworkMetrics,
}

#[godot_api]
impl INode for P2PNetworkManager {
    fn init(base: Base<Node>) -> Self {
        let commands = Arc::new(Mutex::new(None));
        let thread_commands = commands.clone();
        let metrics = NetworkMetrics::default();
        let thread_metrics = metrics.clone();
        let key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(key.public()).to_string();
        let key_bytes = key.to_protobuf_encoding().expect("encode P2P identity");
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        thread::Builder::new()
            .name("p2p-libp2p-runtime".into())
            .spawn(move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build P2P Tokio runtime");
                runtime.block_on(async move {
                    let (tx, rx) = mpsc::unbounded_channel();
                    *thread_commands.lock().expect("commands mutex poisoned") = Some(tx);
                    let _ = ready_tx.send(());
                    if let Err(error) = run_swarm(rx, key_bytes, thread_metrics).await {
                        godot_print!("P2P runtime stopped: {error:#}");
                    }
                });
            })
            .expect("spawn P2P runtime thread");
        let _ = ready_rx.recv();

        Self {
            base,
            commands,
            peer_id,
            metrics,
        }
    }
}

#[godot_api]
impl P2PNetworkManager {
    #[func]
    fn get_network_metrics(&self) -> VarDictionary {
        let mut metrics = VarDictionary::new();
        let connection_type = self
            .metrics
            .connection_type
            .read()
            .expect("connection metrics lock poisoned")
            .clone();
        metrics.set(
            "ping_ms",
            self.metrics.ping_ms.load(Ordering::Relaxed) as i64,
        );
        metrics.set(
            "bytes_sent",
            self.metrics.bytes_sent.load(Ordering::Relaxed) as i64,
        );
        metrics.set(
            "bytes_received",
            self.metrics.bytes_received.load(Ordering::Relaxed) as i64,
        );
        metrics.set("connection_type", connection_type);
        metrics
    }

    /// Creates a peer instance whose queues are connected to the background
    /// libp2p runtime. Assign the returned object to `multiplayer_peer`.
    #[func]
    fn create_peer(&self) -> Gd<Libp2pMultiplayerPeer> {
        let (bridge, endpoint) = PacketBridge::new(self.metrics.clone());
        self.send(Command::AttachPeer { endpoint });
        Gd::from_init_fn(|base| Libp2pMultiplayerPeer {
            base,
            incoming: bridge.incoming,
            outgoing: bridge.outgoing,
            pending: VecDeque::new(),
            peers: bridge.peers,
            metrics: bridge.metrics,
            packet_peer: 1,
            packet_channel: 0,
            transfer_channel: 0,
            transfer_mode: multiplayer_peer::TransferMode::RELIABLE,
            target_peer: 0,
            status: multiplayer_peer::ConnectionStatus::CONNECTED,
            unique_id: 1,
            refusing_connections: false,
        })
    }

    #[func]
    fn host(&mut self, local_port: u16) -> GString {
        let peer = self.peer_id.clone();
        self.send(Command::Host { port: local_port });
        GString::from(peer.as_str())
    }

    #[func]
    fn join(&self, peer_id: GString, local_bind_port: u16) -> bool {
        match peer_id.to_string().parse() {
            Ok(peer) => {
                self.send(Command::Join {
                    peer,
                    bind_port: local_bind_port,
                });
                true
            }
            Err(error) => {
                godot_error!("invalid remote PeerId: {error}");
                false
            }
        }
    }

    fn send(&self, command: Command) {
        let sender = self
            .commands
            .lock()
            .expect("commands mutex poisoned")
            .clone();
        if let Some(sender) = sender {
            if sender.send(command).is_err() {
                godot_error!("P2P runtime thread is no longer running");
            }
        } else {
            godot_error!("P2P runtime is not initialized");
        }
    }
}

struct P2PNetworkExtension;

#[gdextension]
unsafe impl ExtensionLibrary for P2PNetworkExtension {}

async fn run_swarm(
    mut commands: mpsc::UnboundedReceiver<Command>,
    key_bytes: Vec<u8>,
    metrics: NetworkMetrics,
) -> anyhow::Result<()> {
    let key = identity::Keypair::from_protobuf_encoding(&key_bytes)
        .context("decode background P2P identity")?;
    let peer_id = PeerId::from(key.public());
    let (relay_transport, relay_behaviour) = relay::client::new(peer_id);
    let quic_transport = quic::tokio::Transport::new(quic::Config::new(&key))
        .map(|(peer, connection), _| (peer, StreamMuxerBox::new(connection)))
        .boxed();
    let relay_transport = relay_transport
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&key)?)
        .multiplex(yamux::Config::default())
        .boxed();
    let transport = quic_transport
        .or_transport(relay_transport)
        .map(|output, _| match output {
            Either::Left(output) | Either::Right(output) => output,
        })
        .boxed();
    let behaviour = Behaviour {
        autonat: autonat::Behaviour::new(peer_id, Default::default()),
        dcutr: dcutr::Behaviour::new(peer_id),
        identify: identify::Behaviour::new(identify::Config::new(
            "/p2p-gdextension/1.0.0".into(),
            key.public(),
        )),
        mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?,
        ping: ping::Behaviour::new(ping::Config::new()),
        relay_client: relay_behaviour,
        streams: stream::Behaviour::new(),
    };
    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    let mut control = swarm.behaviour().streams.new_control();
    let mut incoming = control.accept(FORWARD_PROTOCOL)?;
    let mut local_listener: Option<TcpListener> = None;
    let mut host_target: Option<String> = None;
    let mut remote_peer: Option<PeerId> = None;
    let mut packet_endpoint: Option<PacketEndpoint> = None;

    loop {
        tokio::select! {
            Some(command) = commands.recv() => match command {
                Command::Host { port } => host_target = Some(format!("127.0.0.1:{port}")),
                Command::AttachPeer { endpoint } => {
                    tokio::spawn(run_packet_dispatcher(endpoint.clone()));
                    packet_endpoint = Some(endpoint);
                }
                Command::Join { peer, bind_port } => {
                    remote_peer = Some(peer);
                    local_listener = Some(TcpListener::bind(("127.0.0.1", bind_port)).await?);
                    if let Ok(relay) = std::env::var("P2P_BOOTSTRAP_RELAY") {
                        let relay: Multiaddr = relay.parse()?;
                        let target: Multiaddr = format!(
                            "{relay}/p2p-circuit/p2p/{peer}"
                        )
                        .parse()?;
                        swarm.dial(target)?;
                    } else {
                        godot_print!(
                            "P2P_BOOTSTRAP_RELAY is unset; provide a direct peer address to the runtime"
                        );
                    }

                }

            },
            Some((peer, inbound)) = incoming.next(), if host_target.is_some() => {
                if let Some(endpoint) = packet_endpoint.clone() {
                    tokio::spawn(run_packet_session(inbound, endpoint, peer));
                    continue;
                }
                let target = host_target.clone().expect("host target set");
                tokio::spawn(async move {
                    let mut remote = inbound.compat();
                    if let Ok(mut local) = TcpStream::connect(target).await {
                        let _ = forward_bidirectional(&mut local, &mut remote).await;
                    }
                });
            },
            accepted = async {
                match &local_listener {
                    Some(listener) => Some(listener.accept().await),
                    None => None,
                }
            }, if local_listener.is_some() => {
                if let Some(Ok((mut local, _))) = accepted {
                    if let Some(peer) = remote_peer {
                        if let Ok(remote) = control.open_stream(peer, FORWARD_PROTOCOL).await {
                            let mut remote = remote.compat();
                            tokio::spawn(async move {
                                let _ = forward_bidirectional(&mut local, &mut remote).await;
                            });
                        }
                    }
                }
            },
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        let endpoint_text = format!("{endpoint:?}");
                        if endpoint_text.contains("p2p-circuit") {
                            metrics.set_connection_type("Relayed");
                        } else {
                            metrics.set_connection_type("Direct");
                        }
                        if remote_peer == Some(peer_id) {
                            if let Some(endpoint) = packet_endpoint.clone() {
                                match control.open_stream(peer_id, FORWARD_PROTOCOL).await {
                                    Ok(remote) => {
                                        tokio::spawn(run_packet_session(remote, endpoint, peer_id));
                                    }
                                    Err(error) => {
                                        // Keep the established connection and
                                        // let the relay path continue serving
                                        // existing sessions if DCUtR fails.
                                        godot_print!(
                                            "opening packet stream failed; retaining fallback connection: {error}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => {
                        if let Ok(duration) = event.result {
                            metrics.ping_ms.store(
                                duration.as_millis().min(u64::MAX as u128) as u64,
                                Ordering::Relaxed,
                            );
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Dcutr(event)) => {
                        if let Err(error) = event.result {
                            metrics.set_connection_type("Relayed");
                            godot_print!(
                                "DCUtR unavailable; continuing over Circuit Relay v2: {error}"
                            );
                        }
                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        godot_print!("P2P QUIC listening at {address}/p2p/{peer_id}");
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                        for (peer, address) in peers {
                            if let Some(endpoint) = &packet_endpoint {
                                endpoint.peers.write().expect("peer map lock poisoned").id_for(peer);
                            }
                            if let Err(error) = swarm.dial(address) {
                                godot_print!("mDNS dial failed: {error}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(_))) => {}
                    _ => {}
                }
            }
        }
    }
}
