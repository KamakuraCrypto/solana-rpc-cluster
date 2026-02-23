#!/bin/bash
set -e

echo "=== Solana RPC Cluster Installer ==="

# Build release binary
echo "[1/5] Building release binary..."
cargo build --release

# Create data directory
echo "[2/5] Creating data directory..."
mkdir -p /var/lib/solana-rpc-cluster
echo "  Created /var/lib/solana-rpc-cluster"

# Install binary
echo "[3/5] Installing binary..."
cp target/release/solana-rpc-cluster /usr/local/bin/solana-rpc-cluster
chmod 755 /usr/local/bin/solana-rpc-cluster
echo "  Installed to /usr/local/bin/solana-rpc-cluster"

# Install config
echo "[4/5] Installing config..."
mkdir -p /etc/solana-rpc-cluster
if [ ! -f /etc/solana-rpc-cluster/config.toml ]; then
    cp config.example.toml /etc/solana-rpc-cluster/config.toml
    chmod 600 /etc/solana-rpc-cluster/config.toml
    echo "  Installed config to /etc/solana-rpc-cluster/config.toml"
    echo "  >>> EDIT THIS FILE before starting the service <<<"
else
    echo "  Config already exists, skipping (update manually)"
fi

# Install systemd service
echo "[5/5] Installing systemd service..."
cp solana-rpc-cluster.service /etc/systemd/system/solana-rpc-cluster.service
systemctl daemon-reload
echo "  Installed service file"

echo ""
echo "=== Installation complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config:     nano /etc/solana-rpc-cluster/config.toml"
echo "  2. Add your [[nodes]] entries for each region"
echo "  3. Add your VPS IPs to the [[whitelist]] section"
echo "  4. Start service:   systemctl start solana-rpc-cluster"
echo "  5. Enable on boot:  systemctl enable solana-rpc-cluster"
echo "  6. View logs:       journalctl -u solana-rpc-cluster -f"
echo "  7. Reload config:   systemctl reload solana-rpc-cluster"
echo "  8. Dashboard:       http://127.0.0.1:9000"
echo ""
echo "Quick test:"
echo "  curl http://localhost:8899 -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getHealth\"}'"
