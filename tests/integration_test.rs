use anyhow::{Context, Result};
use std::{
    env,
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::oneshot,
    time::{sleep, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const TOKEN: &str = "e2e-test-token-with-sufficient-entropy";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_tcp_client_reaches_local_backend_through_yamux_tunnel() -> Result<()> {
    let result = timeout(TEST_TIMEOUT, run_e2e()).await;
    match result {
        Ok(result) => result,
        Err(_) => anyhow::bail!("E2E test exceeded {TEST_TIMEOUT:?}"),
    }
}

async fn run_e2e() -> Result<()> {
    let control_port = unused_port().await?;
    let public_port = unused_port().await?;
    let backend = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("bind dummy backend")?;
    let backend_addr = backend.local_addr().context("read backend address")?;
    let payload = b"transparent payload from the simulated mobile device";

    let backend_task = tokio::spawn(echo_once(backend, payload.to_vec()));
    let temp_dir = test_directory();
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("create test certificate directory")?;
    let cert_path = temp_dir.join("relay-cert.pem");
    let key_path = temp_dir.join("relay-key.pem");
    let relay = spawn_binary(
        env!("CARGO_BIN_EXE_reverse-tunnel-relay"),
        [format!("[::]:{control_port}")],
        &temp_dir,
        &cert_path,
        &key_path,
        None,
    )
    .await?;

    wait_for_tcp(control_port).await?;
    wait_for_file(&cert_path).await?;

    let client = spawn_binary(
        env!("CARGO_BIN_EXE_reverse-tunnel-client"),
        [
            format!("127.0.0.1:{control_port}"),
            public_port.to_string(),
            backend_addr.to_string(),
        ],
        &temp_dir,
        &cert_path,
        &key_path,
        Some(TOKEN),
    )
    .await?;

    let result = verify_public_route(public_port, payload).await;
    let backend_result = backend_task.await.context("join dummy backend")??;
    shutdown(client).await;
    shutdown(relay).await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    result?;
    assert_eq!(backend_result, payload);
    Ok(())
}

async fn verify_public_route(public_port: u16, payload: &[u8]) -> Result<()> {
    let mut external = wait_for_public_route(public_port).await?;
    external
        .write_all(payload)
        .await
        .context("write simulated external payload")?;
    external.flush().await.context("flush external payload")?;
    let mut response = vec![0; payload.len()];
    external
        .read_exact(&mut response)
        .await
        .context("read response from local backend")?;
    assert_eq!(
        response, payload,
        "relay/client pipeline changed payload bytes"
    );
    Ok(())
}

async fn echo_once(listener: TcpListener, expected: Vec<u8>) -> Result<Vec<u8>> {
    let (mut stream, _) = listener
        .accept()
        .await
        .context("accept backend connection")?;
    let mut received = vec![0; expected.len()];
    stream
        .read_exact(&mut received)
        .await
        .context("read backend payload")?;
    stream
        .write_all(&received)
        .await
        .context("write backend response")?;
    stream.flush().await.context("flush backend response")?;
    Ok(received)
}

async fn spawn_binary<I>(
    binary: &str,
    args: I,
    directory: &PathBuf,
    cert_path: &PathBuf,
    key_path: &PathBuf,
    token: Option<&str>,
) -> Result<ChildHandle>
where
    I: IntoIterator<Item = String>,
{
    let mut command = Command::new(binary);
    command
        .args(args)
        .current_dir(directory)
        .env("RELAY_CERT_PATH", cert_path)
        .env("RELAY_KEY_PATH", key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(token) = token {
        command.env("TUNNEL_TOKEN", token);
    } else {
        command.env("TUNNEL_TOKEN", TOKEN);
    }
    let mut child = command.spawn().with_context(|| format!("spawn {binary}"))?;
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tokio::select! {
            status = child.wait() => status.context("wait for tunnel process"),
            _ = &mut stop_rx => {
                child.kill().await.context("terminate tunnel process")?;
                child.wait().await.context("wait after termination")
            }
        }
    });
    Ok(ChildHandle {
        stop: Some(stop_tx),
        task,
    })
}

struct ChildHandle {
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<std::process::ExitStatus>>,
}

async fn shutdown(mut child: ChildHandle) {
    if let Some(stop) = child.stop.take() {
        let _ = stop.send(());
    }
    let _ = timeout(Duration::from_secs(5), child.task).await;
}

async fn wait_for_public_route(port: u16) -> Result<TcpStream> {
    for _ in 0..100 {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(stream) => return Ok(stream),
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
    anyhow::bail!("public relay port {port} did not become reachable")
}

async fn wait_for_tcp(port: u16) -> Result<()> {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("control port {port} did not become reachable")
}

async fn wait_for_file(path: &PathBuf) -> Result<()> {
    for _ in 0..100 {
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("relay certificate {} was not generated", path.display())
}

async fn unused_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("reserve dynamic test port")?;
    Ok(listener.local_addr()?.port())
}

fn test_directory() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    env::temp_dir().join(format!("reverse-tunnel-e2e-{}-{nanos}", std::process::id()))
}
