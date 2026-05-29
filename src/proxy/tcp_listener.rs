use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport::server::TcpIncoming;

/// Build a `TcpIncoming` for tonic with configurable TCP_KEEPALIVE and TCP_USER_TIMEOUT.
///
/// Both options are inherited by accepted sockets on Linux, so setting them on the
/// listener applies fleet-wide. Pass 0 to disable either.
pub fn build_incoming(
    addr: SocketAddr,
    tcp_keepalive_secs: u64,
    tcp_user_timeout_secs: u64,
) -> anyhow::Result<TcpIncoming> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.set_nodelay(true)?;

    if tcp_keepalive_secs > 0 {
        let ka = TcpKeepalive::new().with_time(Duration::from_secs(tcp_keepalive_secs));
        if let Err(e) = socket.set_tcp_keepalive(&ka) {
            tracing::warn!(
                "TCP keepalive setsockopt failed on {}: {} (continuing)",
                addr,
                e
            );
        }
    }

    if tcp_user_timeout_secs > 0 {
        if let Err(e) = socket
            .set_tcp_user_timeout(Some(Duration::from_secs(tcp_user_timeout_secs)))
        {
            tracing::warn!(
                "TCP_USER_TIMEOUT setsockopt failed on {}: {} (continuing)",
                addr,
                e
            );
        }
    }

    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    let std_listener: std::net::TcpListener = socket.into();
    let tokio_listener = tokio::net::TcpListener::from_std(std_listener)?;

    tracing::info!(
        "TCP listener on {} (keepalive={}s, user_timeout={}s)",
        addr,
        tcp_keepalive_secs,
        tcp_user_timeout_secs
    );

    TcpIncoming::from_listener(tokio_listener, true, None)
        .map_err(|e| anyhow::anyhow!("TcpIncoming::from_listener failed: {}", e))
}
