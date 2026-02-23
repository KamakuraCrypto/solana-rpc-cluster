use crate::config::FirewallConfig;
use std::collections::HashSet;
use std::net::IpAddr;

pub async fn sync_firewall(
    ips: &HashSet<IpAddr>,
    config: &FirewallConfig,
    ports: &[u16],
) -> anyhow::Result<()> {
    if !config.enabled {
        return Ok(());
    }

    match config.backend.as_str() {
        "nftables" => sync_nftables(ips, &config.chain_name, ports).await,
        "iptables" => sync_iptables(ips, &config.chain_name, ports).await,
        other => anyhow::bail!("Unknown firewall backend: {}", other),
    }
}

async fn sync_nftables(
    ips: &HashSet<IpAddr>,
    chain_name: &str,
    ports: &[u16],
) -> anyhow::Result<()> {
    let ports_str = ports
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let mut rules = format!(
        "flush chain inet filter {chain}\n",
        chain = chain_name
    );

    for ip in ips {
        rules.push_str(&format!(
            "add rule inet filter {} ip saddr {} tcp dport {{ {} }} accept\n",
            chain_name, ip, ports_str
        ));
    }

    rules.push_str(&format!(
        "add rule inet filter {} tcp dport {{ {} }} drop\n",
        chain_name, ports_str
    ));

    let output = tokio::process::Command::new("nft")
        .arg("-f")
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?
        .wait_with_output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!("nftables sync failed: {}", stderr);
    } else {
        tracing::info!(
            "Firewall synced: {} IPs whitelisted on ports [{}]",
            ips.len(),
            ports_str
        );
    }

    Ok(())
}

async fn sync_iptables(ips: &HashSet<IpAddr>, chain_name: &str, ports: &[u16]) -> anyhow::Result<()> {
    let ports_str = ports
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // Flush existing chain
    let _ = tokio::process::Command::new("iptables")
        .args(["-F", chain_name])
        .output()
        .await;

    // Create chain if it doesn't exist
    let _ = tokio::process::Command::new("iptables")
        .args(["-N", chain_name])
        .output()
        .await;

    for ip in ips {
        let _ = tokio::process::Command::new("iptables")
            .args([
                "-A",
                chain_name,
                "-s",
                &ip.to_string(),
                "-p",
                "tcp",
                "-m",
                "multiport",
                "--dports",
                &ports_str,
                "-j",
                "ACCEPT",
            ])
            .output()
            .await;
    }

    let _ = tokio::process::Command::new("iptables")
        .args([
            "-A",
            chain_name,
            "-p",
            "tcp",
            "-m",
            "multiport",
            "--dports",
            &ports_str,
            "-j",
            "DROP",
        ])
        .output()
        .await;

    tracing::info!(
        "iptables synced: {} IPs whitelisted on ports [{}]",
        ips.len(),
        ports_str
    );

    Ok(())
}
