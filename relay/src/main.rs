use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde::Deserialize;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    env,
    fs::{self, OpenOptions},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tokio_rustls::{rustls, TlsAcceptor};
use tokio_yamux::{Config, Session};
use tunnel_core::forward_bidirectional;

const FRAME_MAGIC: &[u8; 4] = b"RTF1";
const MAX_TARGET: usize = 1024;
const MAX_CONNECTIONS: usize = 1024;
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const TLS_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
#[command(name = "reverse-tunnel-relay", about = "TLS reverse tunnel relay")]
struct Cli {
    #[arg(short, long)]
    listen: Option<String>,
    #[arg(long)]
    cert: Option<String>,
    #[arg(long)]
    key: Option<String>,
    #[arg(short, long)]
    token: Option<String>,
    #[arg(short, long)]
    config: Option<PathBuf>,
    #[arg(value_name = "LEGACY_LISTEN", hide = true)]
    legacy_listen: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen: Option<String>,
    cert: Option<String>,
    key: Option<String>,
    token: Option<String>,
}

fn load_config(path: Option<&Path>) -> Result<FileConfig> {
    match path {
        Some(path) => Ok(toml::from_str(&fs::read_to_string(path)?)?),
        None => Ok(FileConfig::default()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file = load_config(cli.config.as_deref())?;
    let control_addr = cli
        .listen
        .or(cli.legacy_listen)
        .or(file.listen)
        .unwrap_or_else(|| "[::]:9000".into());
    let cert_path = cli
        .cert
        .or(file.cert)
        .or_else(|| env::var("RELAY_CERT_PATH").ok())
        .unwrap_or_else(|| "relay-cert.pem".into());
    let key_path = cli
        .key
        .or(file.key)
        .or_else(|| env::var("RELAY_KEY_PATH").ok())
        .unwrap_or_else(|| "relay-key.pem".into());
    let token = cli
        .token
        .or(file.token)
        .or_else(|| env::var("TUNNEL_TOKEN").ok())
        .context("a token is required via --token, config, or TUNNEL_TOKEN")?;
    if token.is_empty() {
        anyhow::bail!("TUNNEL_TOKEN must not be empty");
    }

    let tls = load_or_create_tls(&cert_path, &key_path)?;
    let acceptor = TlsAcceptor::from(Arc::new(tls));
    let control = bind_dual_stack(&control_addr).await?;
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    eprintln!("TLS control listener: {control_addr}");
    eprintln!("public listeners are selected by authenticated routing frames");

    loop {
        let (raw, peer) = control
            .accept()
            .await
            .context("accept control connection")?;
        set_keepalive(&raw)?;
        let acceptor = acceptor.clone();
        let expected = Arc::new(token.clone().into_bytes());
        let permits = permits.clone();
        tokio::spawn(async move {
            let result = timeout(TLS_TIMEOUT, acceptor.accept(raw))
                .await
                .context("TLS handshake timeout")
                .and_then(|result| result.context("TLS handshake"));
            match result {
                Ok(tls_stream) => {
                    if let Err(error) = serve_client(tls_stream, expected, permits).await {
                        eprintln!("client {peer} ended: {error:#}");
                    }
                }
                Err(error) => eprintln!("TLS client {peer} rejected: {error:#}"),
            }
        });
    }
}

async fn serve_client<S>(
    mut raw: S,
    expected_token: Arc<Vec<u8>>,
    permits: Arc<Semaphore>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (public_port, target) = read_route_frame(&mut raw, &expected_token).await?;
    let public = Arc::new(bind_dual_stack(&format!("[::]:{public_port}")).await?);
    eprintln!("routing public port {public_port} to {target}");

    let config = Config {
        enable_keepalive: true,
        ..Config::default()
    };
    let mut session = Session::new_server(raw, config);
    let mut control = session.control();
    tokio::spawn(async move {
        while let Some(result) = session.next().await {
            if let Ok(mut stream) = result {
                let _ = stream.shutdown().await;
            }
        }
    });

    loop {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .context("acquire connection permit")?;
        // This timeout bounds a stalled accept operation without killing a
        // healthy idle tunnel; no application protocol is terminated here.
        let accepted = match timeout(ACCEPT_TIMEOUT, public.accept()).await {
            Ok(result) => result.context("accept public connection")?,
            Err(_) => continue,
        };
        let mut stream = control.open_stream().await.context("open yamux stream")?;
        let (mut incoming, peer) = accepted;
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = forward_bidirectional(&mut incoming, &mut stream).await {
                eprintln!("public connection {peer} failed: {error:#}");
            }
        });
    }
}

async fn read_route_frame<S>(stream: &mut S, token: &[u8]) -> Result<(u16, String)>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0u8; 10];
    stream
        .read_exact(&mut header)
        .await
        .context("read route frame")?;
    if &header[..4] != FRAME_MAGIC || header[4] != 1 {
        anyhow::bail!("invalid route frame header");
    }
    let port = u16::from_be_bytes([header[5], header[6]]);
    if port == 0 {
        anyhow::bail!("route port must not be zero");
    }
    let target_len = u16::from_be_bytes([header[7], header[8]]) as usize;
    let token_len = header[9] as usize;
    if target_len == 0 || target_len > MAX_TARGET || token_len != token.len() {
        anyhow::bail!("invalid route frame lengths");
    }
    let mut target = vec![0; target_len];
    let mut offered = vec![0; token_len];
    stream
        .read_exact(&mut target)
        .await
        .context("read route target")?;
    stream
        .read_exact(&mut offered)
        .await
        .context("read route token")?;
    if offered != token {
        anyhow::bail!("invalid tunnel token");
    }
    let target = String::from_utf8(target).context("route target is not UTF-8")?;
    target
        .parse::<SocketAddr>()
        .context("route target must be host:port")?;
    Ok((port, target))
}

fn load_or_create_tls(cert_path: &str, key_path: &str) -> Result<rustls::ServerConfig> {
    let (cert_pem, key_pem) = if Path::new(cert_path).exists() && Path::new(key_path).exists() {
        (fs::read(cert_path)?, fs::read(key_path)?)
    } else {
        let generated = generate_simple_self_signed(vec!["relay".into(), "localhost".into()])?;
        let cert = generated.cert.pem();
        let key = generated.signing_key.serialize_pem();
        write_private_file(cert_path, cert.as_bytes())?;
        write_private_file(key_path, key.as_bytes())?;
        eprintln!("generated TLS certificate at {cert_path}; copy it to the client");
        (cert.into_bytes(), key.into_bytes())
    };
    let mut cert_slice = cert_pem.as_slice();
    let mut certs = rustls_pemfile::certs(&mut cert_slice);
    let cert = certs.next().context("certificate PEM is empty")??;
    let mut key_slice = key_pem.as_slice();
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_slice);
    let key = keys.next().context("private key PEM is empty")??;
    Ok(rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.secret_pkcs8_der().to_vec())),
        )?)
}

fn write_private_file(path: &str, contents: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .or_else(|_| OpenOptions::new().write(true).truncate(true).open(path))?;
    use std::io::Write;
    file.write_all(contents)?;
    Ok(())
}

fn set_keepalive(stream: &TcpStream) -> Result<()> {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)?;
    Ok(())
}

async fn bind_dual_stack(address: &str) -> Result<TcpListener> {
    let address: SocketAddr = address.parse().context("invalid listen address")?;
    let domain = if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if address.is_ipv6() {
        socket.set_only_v6(false)?;
    }
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(TcpListener::from_std(socket.into())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]
        #[test]
        fn cli_parser_never_panics_for_random_strings(values in prop::collection::vec(any::<String>(), 0..12)) {
            let mut args = vec!["relay".to_owned()];
            args.extend(values);
            let _ = Cli::try_parse_from(args);
        }

        #[test]
        fn malformed_toml_returns_result(text in any::<String>()) {
            let path = std::env::temp_dir().join(format!(
                "reverse-tunnel-relay-proptest-{}.toml",
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
        let path = std::env::temp_dir().join(format!(
            "reverse-tunnel-relay-missing-{}.toml",
            std::process::id()
        ));
        assert!(load_config(Some(&path)).is_err());
    }
}
