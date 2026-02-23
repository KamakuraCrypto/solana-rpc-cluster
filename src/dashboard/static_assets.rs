use axum::http::header;
use axum::response::IntoResponse;

pub async fn index_page() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        DASHBOARD_HTML,
    )
}

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Solana RPC Cluster</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'SF Mono', 'Fira Code', monospace; background: #0a0a0f; color: #e0e0e0; padding: 20px; }
  h1 { color: #00ff88; margin-bottom: 20px; font-size: 1.4em; }
  h2 { color: #00ccff; margin: 20px 0 10px; font-size: 1.1em; display: flex; align-items: center; gap: 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card { background: #12121a; border: 1px solid #1e1e2e; border-radius: 8px; padding: 14px; }
  .card .label { font-size: 0.75em; color: #888; text-transform: uppercase; margin-bottom: 4px; }
  .card .value { font-size: 1.6em; color: #00ff88; font-weight: bold; }
  .card .value.warn { color: #ffaa00; }
  .card .value.err { color: #ff4444; }
  .node-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .node-card { background: #12121a; border: 1px solid #1e1e2e; border-radius: 8px; padding: 16px; position: relative; }
  .node-card .node-header { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
  .node-card .node-label { font-weight: bold; font-size: 1em; }
  .node-card .node-region { font-size: 0.75em; color: #888; }
  .node-card .node-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; font-size: 0.8em; }
  .node-card .node-stats .ns-label { color: #888; }
  .node-card .node-stats .ns-value { color: #e0e0e0; text-align: right; }
  .node-iris { margin-top: 8px; font-size: 0.75em; display: flex; align-items: center; gap: 4px; }
  table { width: 100%; border-collapse: collapse; background: #12121a; border-radius: 8px; overflow: hidden; }
  th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid #1e1e2e; font-size: 0.85em; }
  th { background: #1a1a2e; color: #00ccff; font-weight: 600; }
  tr:hover { background: #1a1a2e; }
  .actions { display: flex; gap: 6px; }
  .btn { padding: 6px 12px; border: none; border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 0.8em; }
  .btn-add { background: #00ff88; color: #000; font-weight: bold; }
  .btn-del { background: #ff4444; color: #fff; }
  .btn-edit { background: #00ccff; color: #000; }
  .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 100; align-items: center; justify-content: center; }
  .modal.active { display: flex; }
  .modal-content { background: #12121a; border: 1px solid #1e1e2e; border-radius: 8px; padding: 24px; min-width: 340px; }
  .modal-content h3 { color: #00ff88; margin-bottom: 16px; }
  .form-group { margin-bottom: 12px; }
  .form-group label { display: block; font-size: 0.8em; color: #888; margin-bottom: 4px; }
  .form-group input { width: 100%; padding: 8px; background: #0a0a0f; border: 1px solid #1e1e2e; border-radius: 4px; color: #e0e0e0; font-family: inherit; }
  .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .status-dot.active, .status-dot.healthy { background: #00ff88; }
  .status-dot.idle { background: #555; }
  .status-dot.degraded { background: #ffaa00; }
  .status-dot.down { background: #ff4444; }
  .key-display { background: #0a0a0f; padding: 10px; border: 1px solid #00ff88; border-radius: 4px; font-family: monospace; word-break: break-all; color: #00ff88; margin: 10px 0; cursor: pointer; }
  #toast { position: fixed; bottom: 20px; right: 20px; padding: 12px 20px; background: #00ff88; color: #000; border-radius: 6px; font-weight: bold; display: none; z-index: 200; }
  .section { margin-bottom: 24px; }
</style>
</head>
<body>
<h1>SOLANA RPC CLUSTER</h1>

<div class="grid" id="global-stats"></div>

<div class="section">
  <h2>CLUSTER NODES</h2>
  <div class="node-grid" id="node-grid"></div>
</div>

<div class="section">
  <h2>WHITELISTED IPs <button class="btn btn-add" onclick="showAddModal()">+ ADD IP</button></h2>
  <table>
    <thead>
      <tr>
        <th>Status</th>
        <th>IP</th>
        <th>Label</th>
        <th>RPS Limit</th>
        <th>TPS Limit</th>
        <th>Current RPS</th>
        <th>Current TPS</th>
        <th>Total Reqs</th>
        <th>Rate Limited</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="ip-table"></tbody>
  </table>
</div>

<div class="section">
  <h2>API KEYS <button class="btn btn-add" onclick="showKeyModal()">+ GENERATE KEY</button></h2>
  <table>
    <thead>
      <tr>
        <th>Key</th>
        <th>Label</th>
        <th>RPS</th>
        <th>TPS</th>
        <th>Created</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="key-table"></tbody>
  </table>
</div>

<!-- Add/Edit IP Modal -->
<div class="modal" id="add-modal">
  <div class="modal-content">
    <h3 id="modal-title">Add IP</h3>
    <div class="form-group"><label>IP Address</label><input id="m-ip" placeholder="1.2.3.4"></div>
    <div class="form-group"><label>Label</label><input id="m-label" placeholder="my-vps"></div>
    <div class="form-group"><label>RPS Limit</label><input id="m-rps" type="number" placeholder="200"></div>
    <div class="form-group"><label>TPS Limit</label><input id="m-tps" type="number" placeholder="50"></div>
    <div style="display:flex;gap:8px;margin-top:16px;">
      <button class="btn btn-add" onclick="submitIp()">Save</button>
      <button class="btn" style="background:#333;color:#fff;" onclick="closeModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Generate Key Modal -->
<div class="modal" id="key-modal">
  <div class="modal-content">
    <h3>Generate API Key</h3>
    <div class="form-group"><label>Label</label><input id="k-label" placeholder="my-app"></div>
    <div class="form-group"><label>RPS Limit (optional)</label><input id="k-rps" type="number" placeholder="200"></div>
    <div class="form-group"><label>TPS Limit (optional)</label><input id="k-tps" type="number" placeholder="50"></div>
    <div id="generated-key" style="display:none;">
      <div style="font-size:0.8em;color:#888;margin-bottom:4px;">YOUR API KEY (copy now, shown once):</div>
      <div class="key-display" id="key-value" onclick="copyKey()"></div>
    </div>
    <div style="display:flex;gap:8px;margin-top:16px;">
      <button class="btn btn-add" id="gen-key-btn" onclick="generateKey()">Generate</button>
      <button class="btn" style="background:#333;color:#fff;" onclick="closeKeyModal()">Close</button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
const headers = {'Content-Type':'application/json'};
let ipConfigs = {};
let ipStats = {};
let nodesList = [];
let apiKeysList = [];
let ws = null;

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 2500);
}

function connectWs() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host}/api/ws?user=${encodeURIComponent(getDashUser())}&pass=${encodeURIComponent(getDashPass())}`;
  ws = new WebSocket(url);

  ws.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      ipStats = d.per_ip || {};
      ipConfigs = {};
      (d.ips || []).forEach(ip => ipConfigs[ip.ip] = ip);
      nodesList = d.nodes || [];
      renderGlobal(d.global);
      renderNodes();
      renderTable();
    } catch(err) { console.error('WS parse error:', err); }
  };

  ws.onclose = () => { setTimeout(connectWs, 2000); };
  ws.onerror = () => { ws.close(); };
}

function renderGlobal(g) {
  if(!g) return;
  document.getElementById('global-stats').innerHTML = `
    <div class="card"><div class="label">Uptime</div><div class="value">${formatUptime(g.uptime_secs)}</div></div>
    <div class="card"><div class="label">Total Requests</div><div class="value">${fmtNum(g.total_requests)}</div></div>
    <div class="card"><div class="label">Current RPS</div><div class="value${g.global_rps > 1000 ? ' warn' : ''}">${g.global_rps}</div></div>
    <div class="card"><div class="label">Current TPS</div><div class="value${g.global_tps > 200 ? ' warn' : ''}">${g.global_tps}</div></div>
    <div class="card"><div class="label">Connected IPs</div><div class="value">${g.connected_ips}</div></div>
  `;
}

function renderNodes() {
  const grid = document.getElementById('node-grid');
  grid.innerHTML = nodesList.map(n => {
    const statusClass = n.status === 'healthy' ? 'healthy' : n.status === 'degraded' ? 'degraded' : 'down';
    const irisHtml = n.iris_configured
      ? `<div class="node-iris"><span class="status-dot ${n.iris_healthy ? 'healthy' : 'down'}"></span>${n.iris_healthy ? 'TPU OK' : 'TPU Down'}</div>`
      : '';
    const lastMsg = (n.last_msg_age_secs >= 1000000) ? '—' : `${n.last_msg_age_secs}s ago`;
    const winRate = (n.wins + n.dupes) > 0 ? ((n.wins / (n.wins + n.dupes)) * 100).toFixed(0) : '—';
    return `<div class="node-card">
      <div class="node-header">
        <span class="status-dot ${statusClass}"></span>
        <span class="node-label">${n.label || n.id}</span>
        <span class="node-region">${n.region}</span>
      </div>
      <div class="node-stats">
        <span class="ns-label">Status</span><span class="ns-value" style="color:${statusClass==='healthy'?'#00ff88':statusClass==='degraded'?'#ffaa00':'#ff4444'}">${n.status.toUpperCase()}</span>
        <span class="ns-label">Latency</span><span class="ns-value">${n.latency_ms}ms</span>
        <span class="ns-label">Slot</span><span class="ns-value">${fmtNum(n.last_slot)}</span>
        <span class="ns-label">Uptime</span><span class="ns-value">${n.uptime_pct.toFixed(1)}%</span>
        <span class="ns-label">Wins</span><span class="ns-value" style="color:#00ff88">${fmtNum(n.wins)}</span>
        <span class="ns-label">Dupes</span><span class="ns-value">${fmtNum(n.dupes)}</span>
        <span class="ns-label">Win Rate</span><span class="ns-value">${winRate}${winRate!=='—'?'%':''}</span>
        <span class="ns-label">Last Msg</span><span class="ns-value">${lastMsg}</span>
      </div>
      ${irisHtml}
    </div>`;
  }).join('');
}

function renderTable() {
  const tbody = document.getElementById('ip-table');
  const ips = Object.values(ipConfigs);
  const now = Date.now();
  tbody.innerHTML = ips.map(ip => {
    const s = ipStats[ip.ip] || {};
    const lastSeen = s.last_seen_epoch_ms || 0;
    const recentActivity = lastSeen > 0 && (now - lastSeen) < 30000;
    const activeConns = (s.active_rpc_conns||0)+(s.active_ws_conns||0)+(s.active_grpc_streams||0)+(s.active_arpc_streams||0);
    const active = recentActivity || activeConns > 0;
    return `<tr>
      <td><span class="status-dot ${active?'active':'idle'}"></span>${active?'Active':'Idle'}</td>
      <td><strong>${ip.ip}</strong></td>
      <td>${ip.label||'-'}</td>
      <td>${ip.rps}</td>
      <td>${ip.tps}</td>
      <td>${s.rps_current||0}</td>
      <td>${s.tps_current||0}</td>
      <td>${fmtNum(s.total_rpc_requests||0)}</td>
      <td class="${(s.rate_limited_count||0)>0?'err':''}">${s.rate_limited_count||0}</td>
      <td class="actions">
        <button class="btn btn-edit" onclick="editIp('${ip.ip}')">Edit</button>
        <button class="btn btn-del" onclick="removeIp('${ip.ip}')">Del</button>
      </td>
    </tr>`;
  }).join('');
}

function showAddModal() {
  document.getElementById('modal-title').textContent = 'Add IP';
  document.getElementById('m-ip').value = '';
  document.getElementById('m-ip').disabled = false;
  document.getElementById('m-label').value = '';
  document.getElementById('m-rps').value = '200';
  document.getElementById('m-tps').value = '50';
  document.getElementById('add-modal').classList.add('active');
}

function editIp(ip) {
  const c = ipConfigs[ip];
  document.getElementById('modal-title').textContent = 'Edit ' + ip;
  document.getElementById('m-ip').value = ip;
  document.getElementById('m-ip').disabled = true;
  document.getElementById('m-label').value = c.label || '';
  document.getElementById('m-rps').value = c.rps;
  document.getElementById('m-tps').value = c.tps;
  document.getElementById('add-modal').classList.add('active');
}

function closeModal() {
  document.getElementById('add-modal').classList.remove('active');
}

async function submitIp() {
  const ip = document.getElementById('m-ip').value.trim();
  const isEdit = document.getElementById('m-ip').disabled;
  const body = {
    ip, label: document.getElementById('m-label').value,
    rps: parseInt(document.getElementById('m-rps').value) || null,
    tps: parseInt(document.getElementById('m-tps').value) || null,
  };
  try {
    const url = isEdit ? `/api/ips/${ip}` : '/api/ips';
    const method = isEdit ? 'PUT' : 'POST';
    const r = await fetch(url, {method, headers, body: JSON.stringify(body)});
    if(r.ok) { toast(isEdit?'Updated':'Added'); closeModal(); }
    else toast('Error: ' + r.status);
  } catch(e) { toast('Error: '+e); }
}

async function removeIp(ip) {
  if(!confirm('Remove ' + ip + '?')) return;
  try {
    const r = await fetch(`/api/ips/${ip}`, {method:'DELETE', headers});
    if(r.ok) { toast('Removed'); }
    else toast('Error: ' + r.status);
  } catch(e) { toast('Error: '+e); }
}

// API Key functions
function showKeyModal() {
  document.getElementById('k-label').value = '';
  document.getElementById('k-rps').value = '';
  document.getElementById('k-tps').value = '';
  document.getElementById('generated-key').style.display = 'none';
  document.getElementById('gen-key-btn').disabled = false;
  document.getElementById('key-modal').classList.add('active');
}

function closeKeyModal() {
  document.getElementById('key-modal').classList.remove('active');
  loadApiKeys();
}

async function generateKey() {
  const body = {
    label: document.getElementById('k-label').value,
    rps: parseInt(document.getElementById('k-rps').value) || null,
    tps: parseInt(document.getElementById('k-tps').value) || null,
  };
  try {
    const r = await fetch('/api/keys', {method:'POST', headers, body: JSON.stringify(body)});
    if(r.ok) {
      const data = await r.json();
      document.getElementById('key-value').textContent = data.key;
      document.getElementById('generated-key').style.display = 'block';
      document.getElementById('gen-key-btn').disabled = true;
      toast('Key generated!');
    } else toast('Error: ' + r.status);
  } catch(e) { toast('Error: '+e); }
}

function copyKey() {
  const key = document.getElementById('key-value').textContent;
  navigator.clipboard.writeText(key).then(() => toast('Copied!'));
}

async function loadApiKeys() {
  try {
    const r = await fetch('/api/keys');
    if(r.ok) {
      apiKeysList = await r.json();
      renderApiKeys();
    }
  } catch(e) { console.error('Failed to load keys:', e); }
}

function renderApiKeys() {
  const tbody = document.getElementById('key-table');
  tbody.innerHTML = apiKeysList.map(k => `<tr>
    <td><code>${k.key_prefix}</code></td>
    <td>${k.label || '-'}</td>
    <td>${k.rps || 'default'}</td>
    <td>${k.tps || 'default'}</td>
    <td>${k.created_at ? new Date(k.created_at).toLocaleDateString() : '-'}</td>
    <td class="actions">
      <button class="btn btn-del" onclick="revokeKey('${k.key_prefix}')">Revoke</button>
    </td>
  </tr>`).join('');
}

async function revokeKey(prefix) {
  if(!confirm('Revoke key ' + prefix + '?')) return;
  try {
    const r = await fetch(`/api/keys/${encodeURIComponent(prefix)}`, {method:'DELETE', headers});
    if(r.ok) { toast('Revoked'); loadApiKeys(); }
    else toast('Error: ' + r.status);
  } catch(e) { toast('Error: '+e); }
}

function fmtNum(n) {
  if(n >= 1000000) return (n/1000000).toFixed(1)+'M';
  if(n >= 1000) return (n/1000).toFixed(1)+'K';
  return n.toString();
}

function formatUptime(s) {
  const d = Math.floor(s/86400);
  const h = Math.floor((s%86400)/3600);
  const m = Math.floor((s%3600)/60);
  if(d > 0) return `${d}d ${h}h`;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// Init: connect WS using browser's Basic Auth (sent automatically on WS upgrade)
(function() {
  connectWs();
  loadApiKeys();
})();

function connectWs() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  // Browser sends Basic Auth header automatically on WS upgrade (same origin)
  const url = `${proto}//${location.host}/api/ws`;
  ws = new WebSocket(url);

  ws.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      ipStats = d.per_ip || {};
      ipConfigs = {};
      (d.ips || []).forEach(ip => ipConfigs[ip.ip] = ip);
      nodesList = d.nodes || [];
      renderGlobal(d.global);
      renderNodes();
      renderTable();
    } catch(err) { console.error('WS parse error:', err); }
  };

  ws.onclose = () => { setTimeout(connectWs, 3000); };
  ws.onerror = () => { ws.close(); };
}
</script>
</body>
</html>
"##;
