use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use rustls::pki_types::{CertificateDer, ServerName};
use serde::Deserialize;
use socket2::SockRef;
use std::{env, fs, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::sleep};
use tokio_rustls::{rustls, TlsConnector};
use tokio_yamux::{Config, Session};
pub(crate) use tunnel_core::reconnect_delay;
use tunnel_core::{forward_bidirectional, write_route_frame};

#[derive(Debug, Parser)]
#[command(name = "reverse-tunnel-client", about = "Local reverse tunnel client")]
struct Cli {
    #[arg(short, long)]
    relay: Option<String>,
    #[arg(short = 'p', long)]
    public_port: Option<u16>,
    #[arg(short, long)]
    target: Option<String>,
    #[arg(short = 'k', long)]
    token: Option<String>,
    #[arg(long)]
    cert: Option<String>,
    #[arg(short, long)]
    config: Option<PathBuf>,
    #[arg(value_name = "LEGACY_RELAY", hide = true)]
    legacy_relay: Option<String>,
    #[arg(value_name = "LEGACY_PORT", hide = true)]
    legacy_port: Option<u16>,
    #[arg(value_name = "LEGACY_TARGET", hide = true)]
    legacy_target: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    relay: Option<String>,
    public_port: Option<u16>,
    target: Option<String>,
    token: Option<String>,
    cert: Option<String>,
}

fn load_config(path: Option<&std::path::Path>) -> Result<FileConfig> {
    match path {
        Some(path) => Ok(toml::from_str(&fs::read_to_string(path)?)?),
        None => Ok(FileConfig::default()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file = load_config(cli.config.as_deref())?;
    let relay = cli
        .relay
        .or(cli.legacy_relay)
        .or(file.relay)
        .unwrap_or_else(|| "relay.example.com:9000".into());
    let public_port = cli
        .public_port
        .or(cli.legacy_port)
        .or(file.public_port)
        .unwrap_or(8443);
    let local_target = cli
        .target
        .or(cli.legacy_target)
        .or(file.target)
        .unwrap_or_else(|| "127.0.0.1:443".into());
    let token = cli
        .token
        .or(file.token)
        .or_else(|| env::var("TUNNEL_TOKEN").ok())
        .context("a token is required via --token, config, or TUNNEL_TOKEN")?;
    let cert_path = cli
        .cert
        .or(file.cert)
        .or_else(|| env::var("RELAY_CERT_PATH").ok())
        .unwrap_or_else(|| "relay-cert.pem".into());
    let tls = load_client_tls(&cert_path)?;
    let connector = TlsConnector::from(Arc::new(tls));
    let mut attempt: u32 = 0;

    loop {
        match run_session(
            &relay,
            public_port,
            &local_target,
            token.as_bytes(),
            &connector,
        )
        .await
        {
            Ok(()) => {
                eprintln!("relay disconnected; reconnecting");
                attempt = 0;
            }
            Err(error) => {
                eprintln!("tunnel session failed: {error:#}; reconnecting");
                attempt = attempt.saturating_add(1);
            }
        }
        sleep(reconnect_delay(attempt)).await;
    }
}

async fn run_session(
    relay: &str,
    public_port: u16,
    local_target: &str,
    token: &[u8],
    connector: &TlsConnector,
) -> Result<()> {
    let raw = TcpStream::connect(relay)
        .await
        .with_context(|| format!("connect to {relay}"))?;
    configure_keepalive(&raw)?;
    let server_name = ServerName::try_from("relay").context("invalid TLS server name")?;
    let mut tls = connector
        .connect(server_name, raw)
        .await
        .context("TLS handshake")?;
    write_route_frame(&mut tls, public_port, local_target, token).await?;

    let config = Config {
        enable_keepalive: true,
        ..Config::default()
    };
    let mut session = Session::new_client(tls, config);
    while let Some(result) = session.next().await {
        let mut stream = result.context("accept yamux stream")?;
        let target = local_target.to_owned();
        tokio::spawn(async move {
            match TcpStream::connect(&target).await {
                Ok(mut local) => {
                    if let Err(error) = forward_bidirectional(&mut local, &mut stream).await {
                        eprintln!("local connection {target} failed: {error:#}");
                    }
                }
                Err(error) => eprintln!("connect to local target {target} failed: {error}"),
            }
        });
    }
    Ok(())
}

fn load_client_tls(path: &str) -> Result<rustls::ClientConfig> {
    let pem = fs::read(path).with_context(|| format!("read relay certificate {path}"))?;
    let mut pem_slice = pem.as_slice();
    let mut certs = rustls_pemfile::certs(&mut pem_slice);
    let cert: CertificateDer<'static> =
        certs.next().context("relay certificate PEM is empty")??;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn configure_keepalive(stream: &TcpStream) -> Result<()> {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    SockRef::from(stream).set_tcp_keepalive(&keepalive)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]
        #[test]
        fn cli_parser_never_panics_for_random_strings(values in prop::collection::vec(any::<String>(), 0..12)) {
            let mut args = vec!["client".to_owned()];
            for value in values {
                args.push(value);
            }
            let _ = Cli::try_parse_from(args);
        }

        #[test]
        fn malformed_toml_returns_result(text in any::<String>()) {
            let path = std::env::temp_dir().join(format!(
                "reverse-tunnel-client-proptest-{}.toml",
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
            "reverse-tunnel-client-missing-{}.toml",
            std::process::id()
        ));
        assert!(load_config(Some(&path)).is_err());
    }
}
