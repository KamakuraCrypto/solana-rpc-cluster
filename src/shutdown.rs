use tokio::signal::unix::{signal, SignalKind};

pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM");

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT, shutting down...");
        }
        _ = sigterm.recv() => {
            tracing::info!("Received SIGTERM, shutting down...");
        }
    }
}

pub fn setup_sighup_handler() -> tokio::sync::mpsc::Receiver<()> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let mut sighup = signal(SignalKind::hangup()).expect("Failed to register SIGHUP");
        loop {
            sighup.recv().await;
            tracing::info!("Received SIGHUP, reloading config...");
            let _ = tx.send(()).await;
        }
    });
    rx
}
