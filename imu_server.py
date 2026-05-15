#!/usr/bin/env python3
"""
imu_server_v5.py — Raw TCP server for ESP32 IMU data

Requirements:
    pip install flask

Run:
    python imu_server_v5.py

Dashboard:
    http://localhost:6050

TCP ingest:
    handled by server.py on port 6071
"""

import json
import queue
import struct
import threading
import socket
import time
import sqlite3

from collections import deque
from datetime import datetime
from flask import Flask, Response, request, jsonify

app = Flask(__name__)

WEB_PORT = 6050
DATA_HTTP_PORT = 6070

# ─────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────
data_store = deque(maxlen=2000)
data_lock = threading.Lock()

subscribers = []
subscribers_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────
# SQLite setup
# ─────────────────────────────────────────────────────────────
db_lock = threading.Lock()

conn_db = sqlite3.connect(
    "imu_data.db",
    check_same_thread=False
)

cur_db = conn_db.cursor()

cur_db.execute("""
CREATE TABLE IF NOT EXISTS imu_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    server_time REAL,
    timestamp_ms INTEGER,

    ax REAL,
    ay REAL,
    az REAL,

    gx REAL,
    gy REAL,
    gz REAL

)
""")

conn_db.commit()

# ─────────────────────────────────────────────────────────────
# Frame format
# 0xAA | ts(4) | ax(2) | ay(2) | az(2)
#      | gx(2) | gy(2) | gz(2)
# ─────────────────────────────────────────────────────────────
HEADER = 0xAA

FRAME_SIZE = 17

SCALE_ACC  = 1.0 / 1000.0
SCALE_GYRO = 1.0 / 1000.0


def parse_frame(buf):
    """
    Parse one 17-byte frame.
    Returns:
        [ts, ax, ay, az, gx, gy, gz]
    """

    if len(buf) < FRAME_SIZE:
        return None

    if buf[0] != HEADER:
        return None

    ts, ax, ay, az, gx, gy, gz = struct.unpack_from(
        '<Ihhhhhh',
        buf,
        1
    )

    return [
        ts,

        ax * SCALE_ACC,
        ay * SCALE_ACC,
        az * SCALE_ACC,

        gx * SCALE_GYRO,
        gy * SCALE_GYRO,
        gz * SCALE_GYRO
    ]


# ─────────────────────────────────────────────────────────────
# Broadcast SSE
# ─────────────────────────────────────────────────────────────
def broadcast(samples):

    event_data = json.dumps({"d": samples})

    with subscribers_lock:

        dead = []

        for q in subscribers:
            try:
                q.put_nowait(event_data)

            except queue.Full:
                dead.append(q)

        for q in dead:
            subscribers.remove(q)


# ─────────────────────────────────────────────────────────────
# Store to SQLite
# ─────────────────────────────────────────────────────────────
def store_samples_db(samples):

    rows = []

    server_time = time.time()

    for s in samples:

        rows.append((
            server_time,

            s[0],

            s[1],
            s[2],
            s[3],

            s[4],
            s[5],
            s[6]
        ))

    with db_lock:

        cur_db.executemany("""
        INSERT INTO imu_samples (
            server_time,
            timestamp_ms,

            ax,
            ay,
            az,

            gx,
            gy,
            gz
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn_db.commit()


# ─────────────────────────────────────────────────────────────
# TCP client handler
# ─────────────────────────────────────────────────────────────
def handle_client(conn, addr):

    print(f"[TCP] Client connected: {addr}")

    buf = b""

    try:
        while True:

            chunk = conn.recv(4096)
            # chunk = conn.recv(65536)

            if not chunk:
                break

            buf += chunk

            samples = []

            while len(buf) >= FRAME_SIZE:

                # sync to frame header
                if buf[0] != HEADER:

                    skip = buf.find(bytes([HEADER]))

                    buf = buf[skip:] if skip != -1 else b""

                    continue

                frame = parse_frame(buf[:FRAME_SIZE])

                buf = buf[FRAME_SIZE:]

                if frame:
                    samples.append(frame)

            if samples:

                with data_lock:
                    for s in samples:
                        data_store.append(s)

                store_samples_db(samples)

                broadcast(samples)

                print(
                    f"[TCP] {len(samples)} samples "
                    f"(store={len(data_store)})"
                )

    except Exception as e:

        print(f"[TCP] Client error: {e}")

    finally:

        conn.close()

        print(f"[TCP] Client disconnected: {addr}")


# ─────────────────────────────────────────────────────────────
# TCP server
# ─────────────────────────────────────────────────────────────
def tcp_server():

    srv = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    srv.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    srv.bind(("0.0.0.0", 5001))

    srv.listen(5)

    print("[TCP] Listening on port 5001")

    while True:

        conn, addr = srv.accept()

        threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        ).start()


# ─────────────────────────────────────────────────────────────
# SSE stream
# ─────────────────────────────────────────────────────────────
@app.route("/stream")
def stream():

    q = queue.Queue(maxsize=100)

    with subscribers_lock:
        subscribers.append(q)

    def event_generator():

        with data_lock:
            history = list(data_store)[-200:]

        if history:
            yield f"data: {json.dumps({'d': history})}\n\n"

        try:
            while True:

                try:
                    yield f"data: {q.get(timeout=20)}\n\n"

                except queue.Empty:
                    yield ": keepalive\n\n"

        except GeneratorExit:

            with subscribers_lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(
        event_generator(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


# ─────────────────────────────────────────────────────────────
# History endpoint
# ─────────────────────────────────────────────────────────────
@app.route("/history")
def history():

    n = int(request.args.get("n", 500))

    with data_lock:
        rows = list(data_store)[-n:]

    return jsonify({
        "d": rows,
        "count": len(rows)
    })


# ─────────────────────────────────────────────────────────────
# CSV download
# ─────────────────────────────────────────────────────────────
@app.route("/download")
def download_csv():

    def fmt_wall_time(unix_float):
        dt = datetime.fromtimestamp(unix_float)
        ms = int(round((unix_float - int(unix_float)) * 1000)) % 1000
        return dt.strftime("%Y-%m-%d %H:%M:%S:") + f"{ms:03d}"

    def generate():

        yield "timestamp(YYYY-MM-DD HH:MM:SS:ms),ax,ay,az,gx,gy,gz\n"

        with data_lock:
            current_data = list(data_store)

        if current_data:
            start_ts_ms = current_data[0][0]
            start_wall = time.time() - ((current_data[-1][0] - start_ts_ms) / 1000.0)

        for row in current_data:
            abs_time = start_wall + ((row[0] - start_ts_ms) / 1000.0)

            yield (
                f"{fmt_wall_time(abs_time)},"
                f"{row[1]:.4f},"
                f"{row[2]:.4f},"
                f"{row[3]:.4f},"
                f"{row[4]:.4f},"
                f"{row[5]:.4f},"
                f"{row[6]:.4f}\n"
            )

    return Response(
        generate(),
        mimetype="text/csv",
        headers={
            "Content-Disposition":
            "attachment; filename=imu_data.csv"
        }
    )


# ─────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IMU Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: #0d1117;
  color: #e6edf3;
  font-family: 'Segoe UI', sans-serif;
  padding: clamp(12px, 4vw, 20px);
}

h1 {
  font-size: clamp(1rem, 5vw, 1.4rem);
  color: #58a6ff;
  margin-bottom: 4px;
}

.subtitle {
  font-size: clamp(0.65rem, 2.5vw, 0.85rem);
  color: #8b949e;
  margin-bottom: clamp(12px, 3vw, 20px);
}

/* STATS */
.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
  gap: clamp(12px, 3vw, 20px);
  margin-bottom: clamp(12px, 3vw, 18px);
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: clamp(6px, 2vw, 10px);
  padding: clamp(12px, 3vw, 16px);
}

.stat { flex: 1; text-align: center; }

.stat-val {
  font-size: clamp(1rem, 4vw, 1.5rem);
  font-weight: bold;
  color: #58a6ff;
}

.stat-lbl {
  font-size: clamp(0.6rem, 1.8vw, 0.8rem);
  color: #8b949e;
  margin-top: 2px;
}

/* GRID — stacked mobile, two larger charts on desktop */
.grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: clamp(12px, 3vw, 16px);
}

@media (min-width: 640px) {
  .grid { grid-template-columns: 1fr 1fr; }
}

@media (min-width: 1024px) {
  .grid { grid-template-columns: 1fr 1fr; }
}

/* CARDS */
.card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: clamp(6px, 2vw, 10px);
  padding: clamp(12px, 3vw, 16px);
}

.card h2 {
  font-size: clamp(0.75rem, 2vw, 0.9rem);
  color: #8b949e;
  margin-bottom: clamp(8px, 2vw, 12px);
}

canvas { width: 100% !important; height: clamp(150px, 40vw, 200px) !important; }

/* STATUS DOT */
.dot {
  display: inline-block;
  width: 8px; height: 8px;
  border-radius: 50%;
  background: #3fb950;
  margin-right: 6px;
  animation: pulse 1.5s infinite;
  flex-shrink: 0;
}

@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

.disconnected { background: #f85149 !important; animation: none !important; }

/* LIVE CONSOLE */
.console-card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: clamp(6px, 2vw, 10px);
  padding: clamp(12px, 3vw, 16px);
  margin-top: clamp(12px, 3vw, 18px);
}

.console-header {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: center;
  gap: clamp(8px, 2vw, 12px);
  margin-bottom: clamp(8px, 2vw, 12px);
}

.console-header h2 {
  font-size: clamp(0.75rem, 2vw, 0.9rem);
  color: #8b949e;
  margin-bottom: 0;
}

.console-actions { display: flex; gap: clamp(6px, 1.5vw, 8px); flex-wrap: wrap; }

.btn-console {
  padding: clamp(4px, 1vw, 6px) clamp(8px, 2vw, 12px);
  background: #21262d;
  color: #c9d1d9;
  border: 1px solid #30363d;
  border-radius: 4px;
  font-size: clamp(0.65rem, 1.5vw, 0.8rem);
  cursor: pointer;
  white-space: nowrap;
  transition: background 0.15s, border-color 0.15s;
}

.btn-console:hover { background: #30363d; border-color: #444c56; }
.btn-console.active { background: #1f6feb; border-color: #1f6feb; color: white; }
.btn-export { background: #238636; color: white; border-color: #2ea043; }
.btn-export:hover { background: #2ea043; }

#dataConsole {
  background: #0d1117;
  color: #3fb950;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: clamp(11px, 1.5vw, 13px);
  height: clamp(200px, 50vh, 350px);
  overflow-y: auto;
  padding: clamp(8px, 2vw, 12px);
  border: 1px solid #30363d;
  border-radius: 6px;
  white-space: pre;
  line-height: 1.4;
}

@media (max-width: 480px) {
  .console-header { flex-direction: column; align-items: flex-start; }
  .console-actions { width: 100%; }
  .btn-console { flex: 1; text-align: center; }
}
</style>
</head>
<body>

<h1><span class="dot" id="dot"></span>IMU Dashboard</h1>
<p class="subtitle">nRF52840 LSM6DS3 → ESP32 → Raw TCP → Python</p>

<div class="stats">
  <div class="stat"><div class="stat-val" id="sTotal">0</div><div class="stat-lbl">TOTAL SAMPLES</div></div>
  <div class="stat"><div class="stat-val" id="sRate">—</div><div class="stat-lbl">SAMPLE RATE (Hz)</div></div>
  <div class="stat"><div class="stat-val" id="sBatch">0</div><div class="stat-lbl">BATCHES RECV'D</div></div>
  <div class="stat"><div class="stat-val" id="sLast">—</div><div class="stat-lbl">ELAPSED (ms)</div></div>
</div>

<div class="grid">
  <div class="card"><h2>ACCELEROMETER (g)</h2><canvas id="accChart"></canvas></div>
  <div class="card"><h2>GYROSCOPE (°/s)</h2><canvas id="gyrChart"></canvas></div>
</div>

<div class="console-card">
  <div class="console-header">
    <h2>LIVE MONITOR</h2>
    <div class="console-actions">
      <button class="btn-console active" id="btnAutoscroll">Autoscroll: ON</button>
      <button class="btn-console" id="btnClear">Clear</button>
      <button class="btn-console btn-export" id="btnExport">Export CSV</button>
    </div>
  </div>
  <div id="dataConsole"></div>
</div>

<script>
const MAX_POINTS = 300;

function makeChart(id, labels, colors) {
  return new Chart(document.getElementById(id).getContext('2d'), {
    type: 'line',
    data: {
      labels: [],
      datasets: labels.map((lbl, i) => ({
        label: lbl, data: [],
        borderColor: colors[i],
        borderWidth: 1.5, pointRadius: 0, tension: 0.2
      }))
    },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#8b949e', font: { size: 11 } } } },
      scales: {
        x: { display: false },
        y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } }
      }
    }
  });
}

const accChart  = makeChart('accChart',  ['AX','AY','AZ'],  ['#58a6ff','#3fb950','#ff7b72']);
const gyrChart  = makeChart('gyrChart',  ['GX','GY','GZ'],  ['#d2a8ff','#ffa657','#79c0ff']);

// ── Stats
let totalSamples = 0, batchCount = 0, lastTs = null, rateWindow = [];

function updateStats(samples) {
  totalSamples += samples.length;
  batchCount++;
  if (samples.length) lastTs = samples[samples.length - 1][0];
  const now = Date.now();
  rateWindow.push({ t: now, n: samples.length });
  rateWindow = rateWindow.filter(x => now - x.t < 3000);
  const rate = rateWindow.reduce((a, x) => a + x.n, 0) / 3;
  document.getElementById('sTotal').textContent = totalSamples.toLocaleString();
  document.getElementById('sRate').textContent  = rate.toFixed(1);
  document.getElementById('sBatch').textContent = batchCount;
  document.getElementById('sLast').textContent  = lastTs ?? '—';
}

// ── Console buttons
let isAutoScroll = true;

document.getElementById('btnAutoscroll').addEventListener('click', function () {
  isAutoScroll = !isAutoScroll;
  this.textContent = isAutoScroll ? 'Autoscroll: ON' : 'Autoscroll: OFF';
  this.classList.toggle('active', isAutoScroll);
});

document.getElementById('btnClear').addEventListener('click', () => {
  document.getElementById('dataConsole').textContent = '';
});

document.getElementById('btnExport').addEventListener('click', () => {
  window.location.href = `${DATA_BASE}/download`;
});

// ── Console log
function logToConsole(samples) {
  const el  = document.getElementById('dataConsole');
  const now = new Date();
  const ts  =
    `${now.getHours().toString().padStart(2,'0')}:` +
    `${now.getMinutes().toString().padStart(2,'0')}:` +
    `${now.getSeconds().toString().padStart(2,'0')}.` +
    `${now.getMilliseconds().toString().padStart(3,'0')}`;
  const f = n => Number(n).toFixed(3).padStart(7, ' ');

  const lines = samples.map(row => {
    const [, ax, ay, az, gx, gy, gz] = row;
    return (
      `${ts} -> ` +
      `AX=${f(ax)} AY=${f(ay)} AZ=${f(az)} | ` +
      `GX=${f(gx)} GY=${f(gy)} GZ=${f(gz)}`
    );
  }).join('\n');

  el.textContent += (el.textContent ? '\n' : '') + lines;

  // trim to 300 lines
  const all = el.textContent.split('\n');
  if (all.length > 300) el.textContent = all.slice(all.length - 300).join('\n');

  if (isAutoScroll) el.scrollTop = el.scrollHeight;
}

// ── SSE connection
const dot = document.getElementById('dot');
const DATA_BASE = `${window.location.protocol}//${window.location.hostname}:6070`;

function connect() {
  const src = new EventSource(`${DATA_BASE}/stream`);

  src.onmessage = e => {
    const { d } = JSON.parse(e.data);

    d.forEach(row => {
      const [ts, ax, ay, az, gx, gy, gz] = row;

      accChart.data.labels.push(ts.toString());
      gyrChart.data.labels.push(ts.toString());

      accChart.data.datasets[0].data.push(ax);
      accChart.data.datasets[1].data.push(ay);
      accChart.data.datasets[2].data.push(az);

      gyrChart.data.datasets[0].data.push(gx);
      gyrChart.data.datasets[1].data.push(gy);
      gyrChart.data.datasets[2].data.push(gz);
    });

    // trim charts together
    while (accChart.data.labels.length > MAX_POINTS) {
      accChart.data.labels.shift();
      gyrChart.data.labels.shift();
      accChart.data.datasets.forEach(ds => ds.data.shift());
      gyrChart.data.datasets.forEach(ds => ds.data.shift());
    }

    accChart.update('none');
    gyrChart.update('none');

    updateStats(d);
    logToConsole(d);
    dot.classList.remove('disconnected');
  };

  src.onerror = () => {
    dot.classList.add('disconnected');
    src.close();
    setTimeout(connect, 3000);
  };
}

connect();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":

    print(f"Dashboard: http://localhost:{WEB_PORT}")

    app.run(
        host="0.0.0.0",
        port=WEB_PORT,
        threaded=True,
        debug=False
    )
