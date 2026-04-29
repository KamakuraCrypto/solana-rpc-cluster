# solana-rpc-cluster

A self-hosted Solana RPC proxy that fans traffic across multiple geographic nodes simultaneously — improving transaction landing rates and providing automatic failover.

## Why

Running a single RPC upstream creates a single point of failure and limits transaction throughput. Routing through multiple regional nodes in parallel means:

- **Better tx landing** — transactions hit different leaders and validator neighborhoods at once, increasing inclusion probability
- **Redundancy** — if one region falls behind or goes offline, traffic routes automatically to healthy nodes
- **One endpoint for clients** — the proxy handles region selection, deduplication, and failover internally

## Features

- Multi-region JSON-RPC proxy with configurable tx fanout
- Yellowstone gRPC subscription multiplexer with deduplication
- aRPC (v1 + v2) proxy
- Per-IP and per-API-key rate limiting with burst support
- IP whitelist with per-entry RPS/TPS overrides
- Health-checked upstream pool with automatic eject/restore
- Live dashboard (stats, IP and key management)
- Discord webhook alerts for upstream up/down/instability events
- Optional nftables/iptables firewall integration

## Quick start

```sh
cp config.example.toml config.toml
# Edit config.toml: set upstream node URLs and dashboard credentials
./solana-rpc-cluster --config config.toml
```

Systemd:

```sh
cp solana-rpc-cluster.service /etc/systemd/system/
systemctl enable --now solana-rpc-cluster.service
```

## Monitoring

```sh
cp node-monitor.env.example /etc/node-monitor.env
# Set DISCORD_WEBHOOK in /etc/node-monitor.env
cp node-monitor /usr/local/bin/node-monitor && chmod +x /usr/local/bin/node-monitor
cp node-monitor.service /etc/systemd/system/
systemctl enable --now node-monitor.service
```

Logs to `/var/log/node-monitor.log`. Sends Discord embeds on state changes only — no spam.

## Config

See `config.example.toml` for all options.

| Section | Purpose |
|---|---|
| `[general]` | Bind addresses, data dir, log level |
| `[[nodes]]` | Upstream node definitions — id, region, priority, URLs |
| `[routing]` | TX fanout targets, gRPC region filter |
| `[rate_limits]` | Global caps and per-client defaults |
| `[grpc]` | Keepalive and multi-region mux tuning |
| `[health]` | Health check interval and thresholds |
| `[[whitelist]]` | Per-IP rate limit overrides |
| `[[api_keys]]` | Static API key entries (also manageable via dashboard) |

## License

MIT
