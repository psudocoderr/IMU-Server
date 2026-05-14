#!/usr/bin/env python3
"""
server.py — Raw TCP server for ESP32 IMU data

TCP ingest  : port 5001
CSV download: http://<host>:5000/download

Session model
─────────────
Every server boot registers a new row in `server_sessions`.
The resulting session_id is stamped on every sample written this run.
On CSV export, rows are grouped by session_id so:
  • wall-clock timestamps are reconstructed from internal timestamp_ms
  • each session has a clear banner + blank-line separator
  • session numbers are permanent — they never renumber on re-export
"""

import csv
import io
import json
import queue
import struct
import threading
import socket
import time
import sqlite3

from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DATA_HTTP_PORT = 6070
TCP_PORT = 6071

# ─────────────────────────────────────────────────────────────
# SQLite setup
# ─────────────────────────────────────────────────────────────
db_lock = threading.Lock()

conn_db = sqlite3.connect("imu_data.db", check_same_thread=False)
cur_db  = conn_db.cursor()

# ── Sessions registry (one row per server boot) ───────────────
cur_db.execute("""
CREATE TABLE IF NOT EXISTS server_sessions (
    session_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    boot_time   REAL
)
""")

# ── IMU samples ───────────────────────────────────────────────
cur_db.execute("""
CREATE TABLE IF NOT EXISTS imu_samples (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id   INTEGER NOT NULL DEFAULT 1,
    server_time  REAL,
    timestamp_ms INTEGER,
    ax           REAL,
    ay           REAL,
    az           REAL,
    gx           REAL,
    gy           REAL,
    gz           REAL
)
""")
conn_db.commit()

# ── Migration: add session_id to databases created before v6 ──
try:
    cur_db.execute(
        "ALTER TABLE imu_samples ADD COLUMN session_id INTEGER NOT NULL DEFAULT 1"
    )
    conn_db.commit()
    print("[DB] Migrated: added session_id column to imu_samples")
except sqlite3.OperationalError:
    pass  # column already exists

# ── Register this boot as a new session ───────────────────────
cur_db.execute("INSERT INTO server_sessions (boot_time) VALUES (?)", (time.time(),))
conn_db.commit()
CURRENT_SESSION_ID = cur_db.lastrowid
print(
    f"[DB] Session #{CURRENT_SESSION_ID} started — "
    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)

# ─────────────────────────────────────────────────────────────
# Frame format
# 0xAA | ts(4) | ax(2) | ay(2) | az(2)
#      | gx(2) | gy(2) | gz(2)
# Total: 17 bytes
# ─────────────────────────────────────────────────────────────
HEADER     = 0xAA
FRAME_SIZE = 17

SCALE_ACC  = 1.0 / 1000.0
SCALE_GYRO = 1.0 / 1000.0

data_store = deque(maxlen=2000)
data_lock = threading.Lock()

subscribers = []
subscribers_lock = threading.Lock()


def parse_frame(buf):
    if len(buf) < FRAME_SIZE or buf[0] != HEADER:
        return None
    ts, ax, ay, az, gx, gy, gz = struct.unpack_from('<Ihhhhhh', buf, 1)
    return [
        ts,
        ax * SCALE_ACC,  ay * SCALE_ACC,  az * SCALE_ACC,
        gx * SCALE_GYRO, gy * SCALE_GYRO, gz * SCALE_GYRO,
    ]


# ─────────────────────────────────────────────────────────────
# Store to SQLite (stamps CURRENT_SESSION_ID on every row)
# ─────────────────────────────────────────────────────────────
def store_samples_db(samples):
    rows = [
        (CURRENT_SESSION_ID, time.time(),
         s[0], s[1], s[2], s[3], s[4], s[5], s[6])
        for s in samples
    ]
    with db_lock:
        cur_db.executemany("""
            INSERT INTO imu_samples
                (session_id, server_time, timestamp_ms,
                 ax, ay, az, gx, gy, gz)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn_db.commit()


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
# TCP client handler
# ─────────────────────────────────────────────────────────────
def handle_client(conn, addr):
    print(f"[TCP] Client connected: {addr}")
    buf = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break

            buf += chunk
            samples = []

            while len(buf) >= FRAME_SIZE:
                if buf[0] != HEADER:
                    skip = buf.find(bytes([HEADER]))
                    buf  = buf[skip:] if skip != -1 else b""
                    continue

                frame = parse_frame(buf[:FRAME_SIZE])
                buf   = buf[FRAME_SIZE:]
                if frame:
                    samples.append(frame)

            if samples:
                with data_lock:
                    for s in samples:
                        data_store.append(s)

                store_samples_db(samples)
                broadcast(samples)
                print(f"[TCP] {len(samples)} samples stored (session #{CURRENT_SESSION_ID})")

    except Exception as e:
        print(f"[TCP] Client error: {e}")
    finally:
        conn.close()
        print(f"[TCP] Client disconnected: {addr}")


# ─────────────────────────────────────────────────────────────
# TCP server
# ─────────────────────────────────────────────────────────────
def tcp_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", TCP_PORT))
    srv.listen(5)
    print(f"[TCP] Listening on port {TCP_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ─────────────────────────────────────────────────────────────
# CSV helpers
# ─────────────────────────────────────────────────────────────
HEADER_ROW = [
    "timestamp(YYYY-MM-DD HH:MM:SS:ms)",
    "ax", "ay", "az",
    "gx", "gy", "gz",
]


def _wall_ts_str(unix_float, include_date=False):
    """Format a Unix float with millisecond precision."""
    dt  = datetime.fromtimestamp(unix_float)
    ms  = int(round((unix_float - int(unix_float)) * 1000)) % 1000
    fmt = "%Y-%m-%d %H:%M:%S:" if include_date else "%H:%M:%S:"
    return dt.strftime(fmt) + f"{ms:03d}"


def _session_banner(session_id, date_str, start_hms, end_hms, duration_s, n_samples):
    """
    One-line comment that Excel and text editors both show clearly:
      # ════ Session 3 ════ 2026-05-13 ════ 15:33:03:000 → 15:33:19:412 ════ 16s | 1 600 samples
    """
    return (
        f"# {'═' * 4} Session {session_id} {'═' * 4} "
        f"{date_str} {'═' * 4} "
        f"{start_hms} \u2192 {end_hms} {'═' * 4} "
        f"{duration_s}s | {n_samples:,} samples"
    )


def build_csv(rows):
    """
    rows: list of tuples from the DB query —
        (session_id, server_time, timestamp_ms,
         ax, ay, az, gx, gy, gz)

    CSV layout
    ──────────
    For each session (ordered by session_id):

      <blank line>   ← separates sessions (skipped before the very first)
      # ════ Session N ════ YYYY-MM-DD ════ HH:MM:SS:mmm → HH:MM:SS:mmm ════ Xs | N samples
      timestamp(YYYY-MM-DD HH:MM:SS:ms),ax,ay,az,gx,gy,gz
      YYYY-MM-DD HH:MM:SS:mmm,...
      …

    Internal timestamp_ms
    ─────────────────────
    Used to align each sample within its session, but omitted from CSV output.

    Wall-clock timestamp
    ─────────────────────
    Anchored to the server_time of the session's first row:
        abs_time = session_start_server_time
                   + (ts_ms − session_start_ts_ms) / 1000.0
    """
    buf = io.StringIO()
    w   = csv.writer(buf)

    if not rows:
        w.writerow(HEADER_ROW)
        return buf.getvalue()

    # ── Group rows by session_id (stable insertion order) ────────
    sessions      = {}
    session_order = []
    for row in rows:
        sid = row[0]
        if sid not in sessions:
            sessions[sid] = []
            session_order.append(sid)
        sessions[sid].append(row)

    # ── Emit each session block ───────────────────────────────────
    first = True
    for sid in session_order:
        s_rows = sessions[sid]

        start_srv_t  = s_rows[0][1]
        start_ts_ms  = s_rows[0][2]
        end_srv_t    = s_rows[-1][1]
        duration_s   = int(round(end_srv_t - start_srv_t))
        date_str     = datetime.fromtimestamp(start_srv_t).strftime("%Y-%m-%d")
        start_hms    = _wall_ts_str(start_srv_t)
        end_hms      = _wall_ts_str(end_srv_t)

        if not first:
            buf.write("\n")          # blank line between sessions (visible in Excel too)
        first = False

        buf.write(
            _session_banner(sid, date_str, start_hms, end_hms,
                            duration_s, len(s_rows)) + "\n"
        )
        w.writerow(HEADER_ROW)

        for r in s_rows:
            ts_ms    = r[2]
            rel_ms   = ts_ms - start_ts_ms          # reset to 0 at session start
            abs_time = start_srv_t + rel_ms / 1000.0
            ts_wall  = _wall_ts_str(abs_time, include_date=True)

            w.writerow([
                ts_wall,
                f"{r[3]:.4f}", f"{r[4]:.4f}", f"{r[5]:.4f}",
                f"{r[6]:.4f}", f"{r[7]:.4f}", f"{r[8]:.4f}",
            ])

    return buf.getvalue()


def build_incremental_csv(rows):
    """
    Build CSV rows for DB rows newer than a known SQLite id.

    Row layout:
      0:id  1:session_id  2:server_time  3:timestamp_ms
      4:ax  5:ay  6:az  7:gx  8:gy  9:gz
    """
    if not rows:
        return ""

    buf = io.StringIO()
    w = csv.writer(buf)

    session_starts = {}
    with db_lock:
        for sid in sorted({r[1] for r in rows}):
            session_starts[sid] = conn_db.execute(
                """
                SELECT server_time, timestamp_ms
                FROM   imu_samples
                WHERE  session_id = ?
                ORDER  BY id
                LIMIT  1
                """,
                (sid,)
            ).fetchone()

    for r in rows:
        _, sid, _, ts_ms, ax, ay, az, gx, gy, gz = r
        start_srv_t, start_ts_ms = session_starts[sid]
        rel_ms = ts_ms - start_ts_ms
        ts_wall = _wall_ts_str(start_srv_t + rel_ms / 1000.0, include_date=True)

        w.writerow([
            ts_wall,
            f"{ax:.4f}", f"{ay:.4f}", f"{az:.4f}",
            f"{gx:.4f}", f"{gy:.4f}", f"{gz:.4f}",
        ])

    return buf.getvalue()


# ─────────────────────────────────────────────────────────────
# CSV download — http://<host>:5000/download
# ─────────────────────────────────────────────────────────────
class CSVHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if int(args[1]) >= 400:
            print(f"[HTTP] {args[0]} {args[1]}")

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/stream":
            self.stream()
            return

        if parsed.path == "/history":
            self.history(parsed)
            return

        if parsed.path == "/download_since":
            self.download_since(parsed)
            return

        if parsed.path == "/latest_id":
            self.latest_id()
            return

        if parsed.path != "/download":
            self.send_response(404)
            self.end_headers()
            return

        with db_lock:
            rows = conn_db.execute(
                """
                SELECT session_id, server_time, timestamp_ms,
                       ax, ay, az, gx, gy, gz
                FROM   imu_samples
                ORDER  BY session_id, id
                """
            ).fetchall()

        csv_text = build_csv(rows)
        data     = csv_text.encode()

        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Disposition", "attachment; filename=imu_data.csv")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
            print(f"[HTTP] CSV sent — {len(rows)} rows across session(s)")
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            print(f"[HTTP] CSV download aborted by client — {len(rows)} rows prepared")

    def latest_id(self):
        with db_lock:
            latest = conn_db.execute(
                "SELECT COALESCE(MAX(id), 0) FROM imu_samples"
            ).fetchone()[0]

        data = str(latest).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def download_since(self, parsed):
        qs = parse_qs(parsed.query)
        try:
            after_id = int(qs.get("after_id", ["0"])[0])
        except ValueError:
            after_id = 0

        with db_lock:
            rows = conn_db.execute(
                """
                SELECT id, session_id, server_time, timestamp_ms,
                       ax, ay, az, gx, gy, gz
                FROM   imu_samples
                WHERE  id > ?
                ORDER  BY id
                """,
                (after_id,)
            ).fetchall()

            latest = conn_db.execute(
                "SELECT COALESCE(MAX(id), ?) FROM imu_samples",
                (after_id,)
            ).fetchone()[0]

        if not rows:
            self.send_response(204)
            self.send_header("X-Last-Id", str(latest))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return

        csv_text = build_incremental_csv(rows)
        data = csv_text.encode()

        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Last-Id", str(rows[-1][0]))
        self.end_headers()

        try:
            self.wfile.write(data)
            print(
                f"[HTTP] incremental CSV sent — "
                f"{len(rows)} rows ({after_id} → {rows[-1][0]})"
            )
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            print(f"[HTTP] incremental CSV aborted — {len(rows)} rows prepared")

    def stream(self):
        q = queue.Queue(maxsize=100)

        with subscribers_lock:
            subscribers.append(q)

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            with data_lock:
                history = list(data_store)[-200:]

            if history:
                self.wfile.write(
                    f"data: {json.dumps({'d': history})}\n\n".encode()
                )
                self.wfile.flush()

            while True:
                try:
                    event_data = q.get(timeout=20)
                    self.wfile.write(f"data: {event_data}\n\n".encode())
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")

                self.wfile.flush()
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass
        finally:
            with subscribers_lock:
                if q in subscribers:
                    subscribers.remove(q)

    def history(self, parsed):
        qs = parse_qs(parsed.query)
        try:
            n = int(qs.get("n", ["500"])[0])
        except ValueError:
            n = 500

        with data_lock:
            rows = list(data_store)[-n:]

        data = json.dumps({"d": rows, "count": len(rows)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def http_server():
    srv = ThreadingHTTPServer(("0.0.0.0", DATA_HTTP_PORT), CSVHandler)
    print(f"[HTTP] Data API at http://0.0.0.0:{DATA_HTTP_PORT}")
    srv.serve_forever()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=tcp_server, daemon=True).start()
    threading.Thread(target=http_server, daemon=True).start()

    print("[READY] Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[STOP]")
