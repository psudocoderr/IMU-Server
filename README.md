# IMU-Server

TCP data ingestion server and live dashboard for the InertiaLink pipeline.

Receives binary IMU frames from ESP32 over TCP, persists to SQLite,
and serves a real-time Chart.js dashboard.

## Components

| File | Description |
|------|-------------|
| `imu_server_v5.py` | TCP listener on :6071; parses 17-byte frames; writes SQLite |
| `server.py` | Flask web server on :6050; SSE stream; CSV export |
| `auto_csv.py` | Scheduled CSV dump utility |

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `http://<host>:6050` | Live Chart.js dashboard |
| `http://<host>:6070/export/csv` | Full data CSV download |
| `<host>:6071` | ESP32 TCP binary ingest |

## Deployment

See [DEPLOY_RPI.md](DEPLOY_RPI.md) for full Raspberry Pi / VPS setup.

## Firmware

Firmware component lives in a separate repo:
**[InertiaLink](https://github.com/psudocoderr/InertiaLink)**
— nRF52840 peripheral + ESP32 central firmware.
