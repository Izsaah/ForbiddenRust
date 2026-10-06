# Rust reverse TCP tunnel

This workspace contains two Tokio binaries:

- `tunnel-core`: shared, transport-independent forwarding and protocol
  primitives used by the TCP and libp2p front ends.
- `reverse-tunnel-relay`: public VPS component. It listens on a dual-stack TLS
  control address and dynamically binds the requested public port.
- `reverse-tunnel-client`: local component. It maintains one outbound TLS
  connection and forwards each yamux stream to its configured local target.

The control connection uses `tokio-rustls` and a relay-generated self-signed
certificate. The client trusts the exact certificate file supplied through
`RELAY_CERT_PATH`; it does not disable certificate validation. After TLS, the
client sends a binary routing frame containing the public port, local target,
and tunnel token. The relay does not parse or terminate the forwarded
application protocol: bytes are copied unchanged at layer 4.

The shared core exposes `Transport`, `StreamForwarder`, and
`BidirectionalForwarder` seams. Concrete TCP/TLS/libp2p adapters remain in the
thin binaries, while route-frame encoding, reconnect backoff, and the
transparent `copy_bidirectional` pipeline are reusable and unit-tested with
`tokio-test` in `tunnel-core`.

## Build

```text
cargo build --release
```

Run the process-level E2E test, which starts both binaries, a loopback
backend, and a simulated external TCP client with dynamically allocated ports:

```text
cargo test --test integration_test -- --nocapture
```

Run the paused-time fault-injection test:

```text
cargo test --test fault_tolerance_test -- --nocapture
```

The fault test uses a transparent TCP proxy that severs the first handshake,
Tokio's paused clock, and `advance` to verify exponential reconnect timing
without waiting in wall-clock time. The relay, client, and P2P node binary
unit tests also run 2,048 proptest cases for randomized CLI input, malformed
TOML, invalid paths, and CLI/file/default precedence.

## CLI and configuration

All three binaries support typed `clap` options and an optional TOML file.
When both are present, command-line options take precedence over TOML values;
the existing positional forms remain accepted for compatibility with older
launch scripts.

Relay example:

```text
cargo run --release -p reverse-tunnel-relay -- \
  --config relay.toml --listen "[::]:9000" --token "replace-with-a-secret"
```

```toml
listen = "[::]:9000"
cert = "relay-cert.pem"
key = "relay-key.pem"
token = "replace-with-a-secret"
```

Client example:

```text
cargo run --release -p reverse-tunnel-client -- \
  --config client.toml --relay "vps.example.com:9000" \
  --public-port 8443 --target "127.0.0.1:8080"
```

```toml
relay = "vps.example.com:9000"
public_port = 8443
target = "127.0.0.1:8080"
cert = "relay-cert.pem"
token = "replace-with-a-secret"
```

The P2P node persists its Ed25519 libp2p identity in `identity.bin` by
default. Set `--identity` or `identity = "..."` to choose another path. The
file is created on first start and reused thereafter, preserving the node's
PeerId across restarts.

```toml
listen = "/ip4/0.0.0.0/tcp/4001"
local_bind = "127.0.0.1:9001"
target = "127.0.0.1:8080"
peer = "12D3KooW..."
identity = "node_identity.bin"
```

## Run

On the VPS, set the same strong, non-empty token on both processes. The first
relay start generates `relay-cert.pem` and `relay-key.pem`; copy only the
certificate to the client host:

```text
set TUNNEL_TOKEN=replace-with-a-long-random-secret
cargo run --release -p reverse-tunnel-relay -- "[::]:9000"
```

On the local machine, copy `relay-cert.pem`, set `RELAY_CERT_PATH` if it is
not in the current directory, and specify the public port followed by the
local application address:

```text
set TUNNEL_TOKEN=replace-with-a-long-random-secret
cargo run --release -p reverse-tunnel-client -- "vps.example.com:9000" "8443" "127.0.0.1:8080"
```

For a game server, use for example `25565` and `127.0.0.1:25565` instead.
The VPS firewall should allow the control port and the dynamically requested
public port. The relay caps accepted public connections at 1024, enables TCP
keep-alives, and applies a bounded accept wait. The client reconnects with
exponential backoff up to 60 seconds when the network drops.

The relay's `[::]` sockets request dual-stack operation (`IPV6_V6ONLY=0`);
the host OS must permit IPv4-mapped IPv6 listeners for IPv4 clients to work.

## Direct libp2p mode

The separate `p2p-node` binary provides a peer-to-peer mode. It uses TCP,
Noise, Yamux, AutoNAT, Circuit Relay v2 client transport, and DCUtR. A relay
multiaddress may be supplied as the optional final argument for bootstrap;
after address discovery, DCUtR attempts a direct upgrade. The relay is not
used as the application data path when that upgrade succeeds.

The positional arguments are:

```text
p2p-node <listen-multiaddr> <dial-multiaddr> <local-bind> <remote-target> <remote-peer-id> [bootstrap-relay-multiaddr]
```

For a deterministic local smoke test:

```text
cargo test --test p2p_test -- --nocapture
```

The P2P integration test launches two standalone nodes, connects the first
node's local TCP listener to the second node, and verifies byte-preserving
forwarding into a loopback backend. It uses direct loopback addresses rather
than requiring an external public relay.

## Godot 4 GDExtension

The `p2p-gdextension` crate builds a Godot 4 `cdylib` exposing
`P2PNetworkManager`:

```gdscript
var manager := P2PNetworkManager.new()
add_child(manager)
var peer_id: String = manager.host(8080)
manager.join(peer_id, 9000)
```

The extension uses libp2p QUIC over UDP and mDNS for local-network discovery.
The extension owns a dedicated Tokio runtime thread, so Swarm polling and
packet framing never block Godot's main thread. Create the native multiplayer
peer and assign it to Godot's high-level multiplayer API:

```gdscript
var peer := manager.create_peer()
multiplayer.multiplayer_peer = peer
```

`Libp2pMultiplayerPeer` maps Godot peer IDs to libp2p `PeerId` values and
preserves reliable, unreliable, and unreliable-ordered modes at the packet
queue boundary. mDNS-discovered peers are dialed automatically on the LAN.
`host(local_port)` and `join(peer_id, local_bind_port)` remain available for
the existing raw TCP forwarding path.

The public libp2p 0.57 Swarm API exposes QUIC streams, but not Quinn datagram
handles. Consequently, unreliable packets use bounded, non-blocking
application queues and are dropped under congestion; reliable packets use the
reliable QUIC stream. This avoids blocking the Godot thread while retaining
the correct gameplay-facing delivery semantics.

`join(peer_id, local_bind_port)` still accepts the legacy relay configuration,
but the QUIC-only transport is intended for direct or mDNS-discovered peers.

Set `P2P_BOOTSTRAP_RELAY` to a relay multiaddress containing its `/p2p/<relay
peer id>` component before starting Godot. The generated
[p2p_network.gdextension](./p2p-gdextension/p2p_network.gdextension) points
Godot at the platform-specific library under
`res://addons/p2p_network/bin/`; copy the built library there as part of the
Godot export step.

Build the extension with:

```text
cargo build --release -p p2p-gdextension
```

### Network debug overlay

The reusable [network_debug_overlay.gd](./p2p-gdextension/network_debug_overlay.gd)
script displays live ping, traffic rate, and Direct/Relayed/Disconnected state.
Attach it to a `CanvasLayer` and use this node layout:

```text
NetworkDebugOverlay (CanvasLayer, script attached)
└── Panel
    └── MarginContainer
        └── VBoxContainer
            ├── PingLabel (Label)
            ├── SentLabel (Label)
            ├── ReceivedLabel (Label)
            └── ConnectionLabel (Label)
```

Put the `P2PNetworkManager` in the `p2p_network_manager` group, or set the
overlay's `manager_path` export to its node path. The overlay reports
per-second traffic rates by sampling the cumulative byte counters returned by
`get_network_metrics()`. Set `show_totals` to `true` to include cumulative
sent and received totals.
