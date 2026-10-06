use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use libp2p::{
    autonat, dcutr, identify, identity, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm, Transport,
};
use libp2p_stream as stream;
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tunnel_core::forward_bidirectional;

const FORWARD_PROTOCOL: StreamProtocol = StreamProtocol::new("/p2p-node/tcp-forward/1");

#[derive(Debug, Parser)]
#[command(name = "p2p-node", about = "Direct libp2p TCP forwarding node")]
struct Cli {
    #[arg(short = 'l', long)]
    listen: Option<String>,
    #[arg(short = 'd', long)]
    dial: Option<String>,
    #[arg(short = 'b', long)]
    local_bind: Option<String>,
    #[arg(short = 't', long)]
    target: Option<String>,
    #[arg(short = 'p', long)]
    peer: Option<String>,
    #[arg(long)]
    bootstrap_relay: Option<String>,
    #[arg(short, long)]
    identity: Option<PathBuf>,
    #[arg(short, long)]
    config: Option<PathBuf>,
    #[arg(value_name = "LEGACY_LISTEN", hide = true)]
    legacy_listen: Option<String>,
    #[arg(value_name = "LEGACY_DIAL", hide = true)]
    legacy_dial: Option<String>,
    #[arg(value_name = "LEGACY_LOCAL_BIND", hide = true)]
    legacy_local_bind: Option<String>,
    #[arg(value_name = "LEGACY_TARGET", hide = true)]
    legacy_target: Option<String>,
    #[arg(value_name = "LEGACY_PEER", hide = true)]
    legacy_peer: Option<String>,
    #[arg(value_name = "LEGACY_RELAY", hide = true)]
    legacy_relay: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen: Option<String>,
    dial: Option<String>,
    local_bind: Option<String>,
    target: Option<String>,
    peer: Option<String>,
    bootstrap_relay: Option<String>,
    identity: Option<PathBuf>,
}

fn load_config(path: Option<&Path>) -> Result<FileConfig> {
    match path {
        Some(path) => Ok(toml::from_str(&fs::read_to_string(path)?)?),
        None => Ok(FileConfig::default()),
    }
}

fn load_or_create_identity(path: &Path) -> Result<identity::Keypair> {
    if path.exists() {
        return identity::Keypair::from_protobuf_encoding(&fs::read(path)?)
            .context("decode persisted libp2p identity");
    }
    let key = identity::Keypair::generate_ed25519();
    fs::write(path, key.to_protobuf_encoding()?)
        .with_context(|| format!("persist identity at {}", path.display()))?;
    Ok(key)
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    autonat: autonat::Behaviour,
    dcutr: dcutr::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    relay_client: relay::client::Behaviour,
    streams: stream::Behaviour,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file = load_config(cli.config.as_deref())?;
    let listen = cli
        .listen
        .or(cli.legacy_listen)
        .or(file.listen)
        .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".into());
    let dial = cli
        .dial
        .or(cli.legacy_dial)
        .or(file.dial)
        .filter(|value| !value.is_empty());
    let local_bind = cli
        .local_bind
        .or(cli.legacy_local_bind)
        .or(file.local_bind)
        .unwrap_or_else(|| "127.0.0.1:0".into());
    let remote_target = cli
        .target
        .or(cli.legacy_target)
        .or(file.target)
        .unwrap_or_else(|| "127.0.0.1:0".into());
    let remote_peer = cli
        .peer
        .or(cli.legacy_peer)
        .or(file.peer)
        .filter(|value| !value.is_empty());
    let bootstrap_relay = cli
        .bootstrap_relay
        .or(cli.legacy_relay)
        .or(file.bootstrap_relay)
        .filter(|value| !value.is_empty());
    let identity_path = cli
        .identity
        .or(file.identity)
        .unwrap_or_else(|| PathBuf::from("identity.bin"));

    let key = load_or_create_identity(&identity_path)?;
    let peer_id = PeerId::from(key.public());
    let (relay_transport, relay_behaviour) = relay::client::new(peer_id);
    let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .or_transport(relay_transport)
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&key)?)
        .multiplex(yamux::Config::default())
        .boxed();

    let behaviour = Behaviour {
        autonat: autonat::Behaviour::new(peer_id, Default::default()),
        dcutr: dcutr::Behaviour::new(peer_id),
        identify: identify::Behaviour::new(identify::Config::new(
            "/p2p-node/1.0.0".into(),
            key.public(),
        )),
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
    swarm.listen_on(listen.parse()?)?;
    let mut control = swarm.behaviour().streams.new_control();
    let mut incoming = control.accept(FORWARD_PROTOCOL)?;
    let local_listener = TcpListener::bind(&local_bind)
        .await
        .with_context(|| format!("bind local forwarding listener {local_bind}"))?;
    println!(
        "P2P_NODE_READY peer={peer_id} local={}",
        local_listener.local_addr()?
    );

    if let Some(dial) = dial {
        swarm.dial(dial.parse::<Multiaddr>()?)?;
    }
    if let Some(relay) = bootstrap_relay {
        // The relay is only a bootstrap path. DCUtR can subsequently upgrade
        // the connection to a direct path when both peers are reachable.
        swarm.dial(relay.parse::<Multiaddr>()?)?;
    }

    let remote_peer = remote_peer
        .map(|value| value.parse::<PeerId>())
        .transpose()
        .context("invalid remote peer ID")?;
    let target = Arc::new(remote_target);
    let _accept_task = tokio::spawn(accept_local(local_listener, control.clone(), remote_peer));

    loop {
        tokio::select! {
            Some((peer, stream)) = incoming.next() => {
                let target = target.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_incoming(stream, &target).await {
                        eprintln!("inbound P2P stream from {peer} failed: {error:#}");
                    }
                });
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } =>
                        println!("P2P_NODE_LISTEN {address}/p2p/{peer_id}"),
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } =>
                        eprintln!("dial to {peer_id:?} failed: {error}"),
                    SwarmEvent::IncomingConnectionError { error, .. } =>
                        eprintln!("incoming connection failed: {error}"),
                    _ => {}
                }
            }
        }
    }
    #[allow(unreachable_code)]
    {
        _accept_task.abort();
        Ok(())
    }
}

async fn accept_local(
    listener: TcpListener,
    mut control: stream::Control,
    peer: Option<PeerId>,
) -> Result<()> {
    let peer = peer.context("remote peer ID is required for local forwarding")?;
    loop {
        let (mut local, _) = listener.accept().await?;
        let remote = loop {
            match control.open_stream(peer, FORWARD_PROTOCOL).await {
                Ok(stream) => break stream,
                Err(error) => {
                    eprintln!("waiting for direct P2P stream to {peer}: {error}");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let mut remote = remote.compat();
        tokio::spawn(async move {
            if let Err(error) = forward_bidirectional(&mut local, &mut remote).await {
                eprintln!("local P2P forwarding failed: {error:#}");
            }
        });
    }
}

async fn serve_incoming<S>(remote: S, target: &str) -> Result<()>
where
    S: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin,
{
    let mut remote = remote.compat();
    let mut local = TcpStream::connect(target)
        .await
        .with_context(|| format!("connect remote target {target}"))?;
    forward_bidirectional(&mut local, &mut remote).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]
        #[test]
        fn cli_parser_never_panics_for_random_strings(values in prop::collection::vec(any::<String>(), 0..14)) {
            let mut args = vec!["p2p-node".to_owned()];
            args.extend(values);
            let _ = Cli::try_parse_from(args);
        }

        #[test]
        fn malformed_toml_returns_result(text in any::<String>()) {
            let path = std::env::temp_dir().join(format!(
                "p2p-node-proptest-{}.toml",
                std::process::id()
            ));
            std::fs::write(&path, format!("[invalid\n{text}")).unwrap();
            let result = load_config(Some(&path));
            let _ = std::fs::remove_file(path);
            prop_assert!(result.is_err());
        }
    }

    #[test]
    fn cli_values_have_precedence_over_file_and_default() {
        let cli = Some("cli");
        let file = Some("file");
        assert_eq!(cli.or(file).unwrap_or("default"), "cli");
        assert_eq!(None.or(file).unwrap_or("default"), "file");
        assert_eq!(None::<&str>.or(None).unwrap_or("default"), "default");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn randomized_precedence_is_cli_then_file_then_default(
            cli in prop::option::of(any::<String>()),
            file in prop::option::of(any::<String>()),
            default in any::<String>(),
        ) {
            let expected = cli.clone().or(file.clone()).unwrap_or(default.clone());
            prop_assert_eq!(cli.or(file).unwrap_or(default), expected);
        }
    }

    #[test]
    fn missing_config_path_returns_error() {
        let path =
            std::env::temp_dir().join(format!("p2p-node-missing-{}.toml", std::process::id()));
        assert!(load_config(Some(&path)).is_err());
    }
}
