# VideoPlayer Download Backend

FastAPI + yt-dlp download server for the VideoPlayerApp Android app.

## Quick Start

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Or with Docker:
```bash
docker-compose up -d
```

## All API Endpoints

### Core

| Method | Path | Description |
|--------|------|-------------|
| GET  | `/health` | Server status, yt-dlp version, job counts |
| POST | `/info` | Fetch video metadata + all available formats |
| GET  | `/dashboard` | Web UI with live job management |
| GET  | `/docs` | Swagger / OpenAPI documentation |

### Download Management

| Method | Path | Description |
|--------|------|-------------|
| POST   | `/download/start` | Queue a single download |
| POST   | `/download/batch` | Queue multiple URLs at once (max 20) |
| GET    | `/download/status/{job_id}` | Poll progress, speed, ETA |
| GET    | `/download/file/{job_id}` | Stream finished file to device |
| GET    | `/download/list` | All jobs (newest first) |
| DELETE | `/download/{job_id}` | Delete job + file |
| POST   | `/download/retry/{job_id}` | Retry a failed job |
| POST   | `/download/cancel_all` | Cancel all queued/active jobs |

### Social Media

| Method | Path | Description |
|--------|------|-------------|
| POST | `/social/info` | Enhanced info with platform name, cookies support, view counts |
| POST | `/social/download` | Alias for `/download/start` |
| GET  | `/social/platforms` | List all supported platform categories |
| GET  | `/social/extract-url?url=…` | Get direct stream URL (for in-app streaming) |

### Subtitles

| Method | Path | Description |
|--------|------|-------------|
| GET | `/subtitle/search?title=…&language=en` | Search OpenSubtitles.com |
| GET | `/subtitle/download?file_id=…` | Download SRT file (returns content) |

### System

| Method | Path | Description |
|--------|------|-------------|
| GET    | `/system/disk` | Download directory and system disk usage |
| DELETE | `/system/cleanup?older_than_hours=1` | Delete old files and jobs |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_DIR` | `/tmp/videodl` | Where files are stored |
| `MAX_FILE_AGE_SECONDS` | `3600` | Auto-delete files after N seconds |
| `MAX_CONCURRENT` | `3` | Max simultaneous downloads |
| `API_KEY` | *(empty)* | Optional auth key (sent as `X-API-Key` header) |
| `PROXY_URL` | *(empty)* | Proxy for geo-restricted content (e.g. `socks5://127.0.0.1:1080`) |
| `MAX_PLAYLIST` | `10` | Max playlist items to return |
| `RATE_LIMIT_PER_MIN` | `30` | Requests per IP per minute |
| `OPENSUBTITLES_API_KEY` | *(empty)* | OpenSubtitles API key for subtitle search |

## Connecting the Android App

1. Start the server on your computer
2. Find your computer's local IP: `ipconfig` (Windows) or `ifconfig` (Mac/Linux)
3. In the app: **Menu → Download** → set server URL to `http://YOUR_IP:8000`
4. Both devices must be on the same WiFi network

## Running Tests

```bash
# Server must be running first
python test_api.py

# Or against a remote server:
python test_api.py http://192.168.1.100:8000
```
