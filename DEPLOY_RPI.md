# Raspberry Pi Deployment

Target:
- Website: `http://10.185.151.179:6050`
- Data API / CSV: `http://10.185.151.179:6070`
- ESP32 TCP ingest: `10.185.151.179:6071`

Run manually:

```bash
cd ~/imu_server_v5_deploy
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python server.py
```

In another terminal:

```bash
cd ~/imu_server_v5_deploy
. .venv/bin/activate
python imu_server_v5.py
```

Install as services:

```bash
sudo cp imu-data-server.service /etc/systemd/system/
sudo cp imu-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now imu-data-server.service
sudo systemctl enable --now imu-web.service
```

Check status/logs:

```bash
systemctl status imu-data-server.service imu-web.service
journalctl -u imu-data-server.service -f
journalctl -u imu-web.service -f
```

ESP32 server config should point to:

```cpp
#define SERVER_IP   "10.185.151.179"
#define SERVER_PORT 6071
```
