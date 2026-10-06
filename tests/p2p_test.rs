use anyhow::{Context, Result};
use std::{env, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    time::{sleep, timeout},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_p2p_nodes_forward_tcp_payload() -> Result<()> {
    timeout(Duration::from_secs(30), run_test())
        .await
        .context("P2P E2E timeout")??;
    Ok(())
}

async fn run_test() -> Result<()> {
    let backend = TcpListener::bind(("127.0.0.1", 0)).await?;
    let backend_addr = backend.local_addr()?;
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await?;
        let mut data = [0; 16];
        stream.read_exact(&mut data).await?;
        stream.write_all(&data).await?;
        Result::<[u8; 16], std::io::Error>::Ok(data)
    });

    let mut node_b = spawn_node(
        "/ip4/127.0.0.1/tcp/0".into(),
        None,
        backend_addr.to_string(),
        None,
        "p2p-test-node-b.identity",
    )
    .await?;
    let b_line = wait_for_line(&mut node_b, "P2P_NODE_LISTEN")
        .await
        .context("read node B listen output")?;
    let b_addr = b_line
        .split_whitespace()
        .nth(1)
        .context("missing node B listen address")?
        .to_string();
    let b_peer = b_addr
        .rsplit_once("/p2p/")
        .context("missing node B peer ID")?
        .1
        .to_string();
    let b_addr = b_addr
        .rsplit_once("/p2p/")
        .context("missing node B peer suffix")?
        .0
        .to_string();
    let mut node_a = spawn_node(
        "/ip4/127.0.0.1/tcp/0".into(),
        Some(b_addr),
        "127.0.0.1:0".into(),
        Some(b_peer),
        "p2p-test-node-a.identity",
    )
    .await?;
    let a_addr = wait_for_line(&mut node_a, "P2P_NODE_READY")
        .await
        .context("read node A ready output")?;
    let local = parse_local_addr(&a_addr)?;
    let mut external = connect_retry(local).await?;
    external.write_all(b"p2p-e2e-payload!").await?;
    let mut response = [0; 16];
    external.read_exact(&mut response).await?;
    assert_eq!(&response, b"p2p-e2e-payload!");

    kill(node_a).await;
    kill(node_b).await;
    let _ = tokio::fs::remove_file("p2p-test-node-a.identity").await;
    let _ = tokio::fs::remove_file("p2p-test-node-b.identity").await;
    assert_eq!(backend_task.await??, *b"p2p-e2e-payload!");
    Ok(())
}

async fn spawn_node(
    listen: String,
    dial: Option<String>,
    target: String,
    remote_peer: Option<String>,
    identity: &str,
) -> Result<Child> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_p2p-node"));
    command
        .args([
            listen,
            dial.unwrap_or_default(),
            "127.0.0.1:0".into(),
            target,
            remote_peer.unwrap_or_default(),
            "--identity".into(),
            identity.into(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    Ok(command.spawn()?)
}

async fn wait_for_line(child: &mut Child, marker: &str) -> Result<String> {
    let stdout = child.stdout.as_mut().context("node stdout unavailable")?;
    let mut reader = BufReader::new(stdout);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            anyhow::bail!("node exited before emitting {marker}");
        }
        if line.contains(marker) {
            return Ok(line);
        }
    }
}

fn parse_local_addr(line: &str) -> Result<std::net::SocketAddr> {
    Ok(line
        .split("local=")
        .nth(1)
        .context("missing local address")?
        .trim()
        .parse()?)
}

async fn connect_retry(addr: std::net::SocketAddr) -> Result<TcpStream> {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            return Ok(stream);
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("could not connect to local P2P forwarding listener")
}

async fn kill(mut child: Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}
