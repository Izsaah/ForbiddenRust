//! Transport-independent primitives shared by tunnel front ends.
//!
//! The core deliberately knows nothing about TCP, TLS, or libp2p connection
//! establishment. Adapters provide an `AsyncRead + AsyncWrite` stream and the
//! same transparent forwarding pipeline is reused everywhere.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub const FRAME_MAGIC: &[u8; 4] = b"RTF1";
pub const MAX_TARGET: usize = 1024;

/// Opens a transport-specific bidirectional stream.
#[async_trait]
pub trait Transport {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    async fn connect(&self) -> Result<Self::Stream>;
}

/// Owns the application-independent L4 forwarding policy.
#[async_trait]
pub trait StreamForwarder {
    async fn forward<A, B>(&self, left: A, right: B) -> Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin + Send,
        B: AsyncRead + AsyncWrite + Unpin + Send;
}

/// Default transparent forwarder. It never parses or terminates application
/// protocols such as TLS, HTTP, or game-protocol frames.
#[derive(Clone, Copy, Debug, Default)]
pub struct BidirectionalForwarder;

#[async_trait]
impl StreamForwarder for BidirectionalForwarder {
    async fn forward<A, B>(&self, mut left: A, mut right: B) -> Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin + Send,
        B: AsyncRead + AsyncWrite + Unpin + Send,
    {
        tokio::io::copy_bidirectional(&mut left, &mut right)
            .await
            .context("copy bidirectional tunnel bytes")?;
        Ok(())
    }
}

/// Convenience function for callers that do not need to retain a forwarder.
pub async fn forward_bidirectional<A, B>(left: A, right: B) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut left = left;
    let mut right = right;
    tokio::io::copy_bidirectional(&mut left, &mut right)
        .await
        .context("copy bidirectional tunnel bytes")?;
    Ok(())
}

/// Exponential reconnect delay, capped at 64 seconds.
pub fn reconnect_delay(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(6))
}

/// Encodes the client-to-relay route control frame.
pub async fn write_route_frame<W>(
    stream: &mut W,
    public_port: u16,
    target: &str,
    token: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if public_port == 0
        || target.is_empty()
        || target.len() > MAX_TARGET
        || token.len() > u8::MAX as usize
    {
        anyhow::bail!("invalid route frame values");
    }
    target
        .parse::<std::net::SocketAddr>()
        .context("route target must be host:port")?;
    let target_len = u16::try_from(target.len()).context("route target is too long")?;
    stream.write_all(FRAME_MAGIC).await?;
    stream.write_all(&[1]).await?;
    stream.write_all(&public_port.to_be_bytes()).await?;
    stream.write_all(&target_len.to_be_bytes()).await?;
    stream.write_all(&[token.len() as u8]).await?;
    stream.write_all(target.as_bytes()).await?;
    stream.write_all(token).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn forwarder_copies_bytes_through_mocked_streams() {
        let left = Builder::new().read(b"request").write(b"response").build();
        let right = Builder::new().write(b"request").read(b"response").build();
        BidirectionalForwarder
            .forward(left, right)
            .await
            .expect("mock streams should forward");
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(3), Duration::from_secs(8));
        assert_eq!(reconnect_delay(99), Duration::from_secs(64));
    }

    #[tokio::test]
    async fn route_frame_is_written_to_mock_stream() {
        let expected = b"RTF1\x01\x1f\x90\x00\x0c\x05127.0.0.1:80token";
        let mut writer = Builder::new().write(expected).build();
        write_route_frame(&mut writer, 8080, "127.0.0.1:80", b"token")
            .await
            .expect("valid route frame");
    }
}
