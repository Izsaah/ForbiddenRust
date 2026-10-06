#[allow(dead_code)]
#[path = "../client/src/main.rs"]
mod client_binary;

use anyhow::Result;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{copy_bidirectional, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{advance, pause},
};

#[tokio::test]
async fn reconnects_after_proxy_severs_initial_handshake() -> Result<()> {
    pause();
    let backend = TcpListener::bind(("127.0.0.1", 0)).await?;
    let proxy = TcpListener::bind(("127.0.0.1", 0)).await?;
    let proxy_addr = proxy.local_addr()?;
    let backend_addr = backend.local_addr()?;
    let enabled = Arc::new(AtomicBool::new(true));
    let sever_after_handshake = Arc::new(AtomicBool::new(true));
    let connections = Arc::new(AtomicUsize::new(0));

    let backend_task = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = backend.accept().await?;
            let mut handshake = [0; 5];
            stream.read_exact(&mut handshake).await?;
            if &handshake != b"HELLO" {
                anyhow::bail!("unexpected handshake");
            }
            stream.write_all(b"OK").await?;
        }
        Ok::<(), anyhow::Error>(())
    });
    let proxy_task = tokio::spawn(run_proxy(
        proxy,
        backend_addr,
        enabled.clone(),
        sever_after_handshake.clone(),
        connections.clone(),
    ));

    let mut attempt = 0;
    let response = loop {
        attempt += 1;
        match connect_once(proxy_addr).await {
            Ok(response) => break response,
            Err(_) => {
                // Restore the proxy after the first deliberately severed
                // handshake; the next retry must use the exponential delay.
                sever_after_handshake.store(false, Ordering::Release);
                let delay = client_binary::reconnect_delay(attempt - 1);
                let sleeper = tokio::time::sleep(delay);
                tokio::pin!(sleeper);
                tokio::task::yield_now().await;
                advance(delay).await;
                sleeper.await;
            }
        }
    };

    assert_eq!(&response, b"OK");
    assert_eq!(connections.load(Ordering::Acquire), 2);
    assert_eq!(attempt, 2);
    enabled.store(false, Ordering::Release);
    proxy_task.abort();
    backend_task.await??;
    Ok(())
}

async fn connect_once(address: std::net::SocketAddr) -> Result<Vec<u8>> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(b"HELLO").await?;
    let mut response = [0; 2];
    stream.read_exact(&mut response).await?;
    if response != *b"OK" {
        anyhow::bail!("proxy did not restore the handshake");
    }
    Ok(response.to_vec())
}

async fn run_proxy(
    listener: TcpListener,
    backend: std::net::SocketAddr,
    enabled: Arc<AtomicBool>,
    sever_after_handshake: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
) -> Result<()> {
    loop {
        let (mut incoming, _) = listener.accept().await?;
        connections.fetch_add(1, Ordering::AcqRel);
        if !enabled.load(Ordering::Acquire) {
            continue;
        }
        let mut upstream = TcpStream::connect(backend).await?;
        let sever = sever_after_handshake.load(Ordering::Acquire);
        tokio::spawn(async move {
            let mut handshake = [0; 5];
            if incoming.read_exact(&mut handshake).await.is_ok() {
                let _ = upstream.write_all(&handshake).await;
                if sever {
                    return;
                }
                let _ = copy_bidirectional(&mut incoming, &mut upstream).await;
            }
        });
    }
}
