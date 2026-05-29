"""
Video Downloader Backend  v2.0
================================
FastAPI + yt-dlp server with:
  • Video info extraction + format listing
  • Async download jobs with live progress
  • Optional API-key authentication
  • Rate limiting per IP
  • Proxy support
  • Playlist support (returns first N videos)
  • Web dashboard at /dashboard
  • OpenAPI docs at /docs

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
Docker: docker-compose up -d
"""

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yt_dlp
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
DOWNLOAD_DIR        = Path(os.environ.get("DOWNLOAD_DIR", "/tmp/videodl"))
MAX_FILE_AGE_SEC    = int(os.environ.get("MAX_FILE_AGE_SECONDS", "3600"))
MAX_CONCURRENT      = int(os.environ.get("MAX_CONCURRENT", "3"))
API_KEY             = os.environ.get("API_KEY", "")           # empty = no auth
PROXY_URL           = os.environ.get("PROXY_URL", "")         # e.g. socks5://127.0.0.1:1080
MAX_PLAYLIST        = int(os.environ.get("MAX_PLAYLIST", "10"))
RATE_LIMIT_PER_MIN  = int(os.environ.get("RATE_LIMIT_PER_MIN", "30"))

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s")
log = logging.getLogger("videodl")

# ---------------------------------------------------------------------------
# Rate limiter (in-memory, per-IP)
# ---------------------------------------------------------------------------
_rate_store: Dict[str, list] = defaultdict(list)   # ip -> [timestamps]

def check_rate_limit(request: Request):
    if RATE_LIMIT_PER_MIN <= 0:
        return
    ip  = request.client.host if request.client else "unknown"
    now = time.time()
    window = _rate_store[ip]
    # Purge entries older than 60 s
    _rate_store[ip] = [t for t in window if now - t < 60]
    if len(_rate_store[ip]) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=429,
            detail=f"Rate limit: max {RATE_LIMIT_PER_MIN} requests/minute")
    _rate_store[ip].append(now)

# ---------------------------------------------------------------------------
# API-key auth
# ---------------------------------------------------------------------------
def check_api_key(request: Request):
    if not API_KEY:
        return   # auth disabled
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

def auth_deps(request: Request):
    check_rate_limit(request)
    check_api_key(request)

# ---------------------------------------------------------------------------
# Job store
# ---------------------------------------------------------------------------
jobs: Dict[str, "DownloadJob"] = {}

class DownloadJob:
    def __init__(self, job_id: str, url: str, format_id: Optional[str] = None,
                 playlist: bool = False):
        self.job_id      = job_id
        self.url         = url
        self.format_id   = format_id
        self.playlist    = playlist
        self.status      = "queued"
        self.progress    = 0.0
        self.speed       = ""
        self.eta         = ""
        self.title       = ""
        self.filename    = ""
        self.file_path   = ""
        self.file_size   = 0
        self.error       = ""
        self.created_at  = time.time()
        self.thumbnail   = ""

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items()
                if not k.startswith("_")}

# ---------------------------------------------------------------------------
# yt-dlp helpers
# ---------------------------------------------------------------------------
def _base_opts() -> dict:
    opts = {"quiet": True, "no_warnings": True}
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
    return opts

def _info_opts() -> dict:
    return {**_base_opts(), "skip_download": True, "noplaylist": True}

def _playlist_info_opts() -> dict:
    return {**_base_opts(), "skip_download": True,
            "extract_flat": True, "playlistend": MAX_PLAYLIST}

def _dl_opts(job: DownloadJob, out_tmpl: str) -> dict:
    def hook(d):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done  = d.get("downloaded_bytes", 0)
            if total:
                job.progress = done / total * 100
            job.speed = d.get("_speed_str", "").strip()
            job.eta   = d.get("_eta_str",   "").strip()
        elif d["status"] == "finished":
            job.progress = 100.0
            job.filename = Path(d["filename"]).name

    fmt = job.format_id if job.format_id \
        else "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"

    return {**_base_opts(),
            "format": fmt,
            "outtmpl": out_tmpl,
            "noplaylist": True,
            "progress_hooks": [hook],
            "merge_output_format": "mp4"}

def _sanitise(s: str, n: int = 80) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", s or "video")[:n].strip()

def _resolve_file(ydl, info, base_path: str) -> Tuple[str, str]:
    """Return (file_path, filename) for a completed download."""
    prepared = ydl.prepare_filename(info)
    for ext in (".mp4", ".mkv", ".webm", ".m4v", ".avi", ".mov"):
        c = Path(prepared).with_suffix(ext)
        if c.exists():
            return str(c), c.name
    # Fallback: newest file in download dir
    files = sorted(DOWNLOAD_DIR.glob("*"), key=lambda f: f.stat().st_mtime, reverse=True)
    if files:
        return str(files[0]), files[0].name
    return "", ""

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    t = asyncio.create_task(_cleanup_loop())
    yield
    t.cancel()

async def _cleanup_loop():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        expired = [jid for jid, j in list(jobs.items())
                   if now - j.created_at > MAX_FILE_AGE_SEC]
        for jid in expired:
            j = jobs.pop(jid, None)
            if j and j.file_path:
                Path(j.file_path).unlink(missing_ok=True)
        if expired:
            log.info(f"Cleaned {len(expired)} expired jobs")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="VideoPlayer Download API",
    description="yt-dlp powered backend for VideoPlayerApp Android",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class InfoRequest(BaseModel):
    url: str
    extract_playlist: bool = False

class DownloadRequest(BaseModel):
    url: str
    format_id: Optional[str] = None
    is_playlist: bool = False

class VideoFormat(BaseModel):
    format_id:  str
    ext:        str
    resolution: str
    fps:        Optional[int] = None
    filesize:   Optional[int] = None
    vcodec:     str = ""
    acodec:     str = ""
    note:       str = ""

class PlaylistEntry(BaseModel):
    title: str
    url:   str
    duration: Optional[float] = None
    thumbnail: Optional[str] = None

class VideoInfo(BaseModel):
    title:       str
    uploader:    str = ""
    duration:    Optional[float] = None
    thumbnail:   Optional[str] = None
    description: Optional[str] = None
    webpage_url: str
    formats:     List[VideoFormat] = []
    playlist:    Optional[List[PlaylistEntry]] = None

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    active = sum(1 for j in jobs.values() if j.status == "downloading")
    return {
        "status": "ok",
        "yt_dlp_version": yt_dlp.version.__version__,
        "active_jobs": active,
        "total_jobs": len(jobs),
        "auth_enabled": bool(API_KEY),
        "proxy_enabled": bool(PROXY_URL),
    }


@app.post("/info", response_model=VideoInfo)
def get_info(req: InfoRequest, _=Depends(auth_deps)):
    try:
        if req.extract_playlist:
            return _extract_playlist_info(req.url)
        return _extract_single_info(req.url)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(500, str(e))
    except Exception as e:
        log.exception("Info error")
        raise HTTPException(500, str(e))


def _extract_single_info(url: str) -> VideoInfo:
    with yt_dlp.YoutubeDL(_info_opts()) as ydl:
        info = ydl.extract_info(url, download=False)

    raw = info.get("formats", [])
    formats, seen = [], set()
    for f in raw:
        vco = f.get("vcodec", "none")
        h   = f.get("height")
        w   = f.get("width")
        if not h or vco in (None, "none"):
            continue
        res = f"{w}x{h}" if w else f"{h}p"
        key = (res, f.get("ext", ""))
        if key in seen: continue
        seen.add(key)
        fps  = f.get("fps")
        size = f.get("filesize") or f.get("filesize_approx")
        formats.append(VideoFormat(
            format_id  = f.get("format_id", ""),
            ext        = f.get("ext", ""),
            resolution = res,
            fps        = int(fps) if fps else None,
            filesize   = int(size) if size else None,
            vcodec     = vco,
            acodec     = f.get("acodec", "none"),
            note       = f.get("format_note", ""),
        ))
    formats.sort(key=lambda x: (
        int(x.resolution.split("x")[-1]) if "x" in x.resolution
        else int(x.resolution.replace("p", "").replace("Best Available", "9999")),
        x.fps or 0
    ), reverse=True)

    # Prepend "Best" option
    formats.insert(0, VideoFormat(
        format_id="bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        ext="mp4", resolution="Best Available",
        vcodec="auto", acodec="auto", note="Highest quality"))

    return VideoInfo(
        title       = info.get("title", "Untitled"),
        uploader    = info.get("uploader", ""),
        duration    = info.get("duration"),
        thumbnail   = info.get("thumbnail"),
        description = (info.get("description") or "")[:500],
        webpage_url = info.get("webpage_url", url),
        formats     = formats,
    )


def _extract_playlist_info(url: str) -> VideoInfo:
    with yt_dlp.YoutubeDL(_playlist_info_opts()) as ydl:
        info = ydl.extract_info(url, download=False)

    entries_raw = info.get("entries", []) or []
    playlist = [
        PlaylistEntry(
            title     = e.get("title", "Untitled"),
            url       = e.get("url") or e.get("webpage_url", ""),
            duration  = e.get("duration"),
            thumbnail = e.get("thumbnail"),
        )
        for e in entries_raw[:MAX_PLAYLIST]
        if e
    ]
    return VideoInfo(
        title       = info.get("title", "Playlist"),
        uploader    = info.get("uploader", ""),
        webpage_url = info.get("webpage_url", url),
        playlist    = playlist,
    )


@app.post("/download/start")
async def start_download(req: DownloadRequest, bg: BackgroundTasks,
                         _=Depends(auth_deps)):
    active = sum(1 for j in jobs.values() if j.status == "downloading")
    if active >= MAX_CONCURRENT:
        raise HTTPException(429, f"Max {MAX_CONCURRENT} concurrent downloads. Retry shortly.")

    job_id = str(uuid.uuid4())
    job    = DownloadJob(job_id, req.url, req.format_id, req.is_playlist)
    jobs[job_id] = job
    bg.add_task(_run_download, job)
    return {"job_id": job_id, "status": "queued"}


@app.get("/download/status/{job_id}")
def status(job_id: str, _=Depends(auth_deps)):
    j = jobs.get(job_id)
    if not j: raise HTTPException(404, "Job not found")
    return j.to_dict()


@app.get("/download/file/{job_id}")
def get_file(job_id: str):
    # No auth on file download so Android DownloadManager can fetch directly
    j = jobs.get(job_id)
    if not j:           raise HTTPException(404, "Job not found")
    if j.status != "done": raise HTTPException(409, f"Status: {j.status}")
    if not j.file_path or not Path(j.file_path).exists():
        raise HTTPException(410, "File expired or missing")
    return FileResponse(j.file_path, filename=j.filename, media_type="video/mp4",
                        headers={"Content-Disposition": f'attachment; filename="{j.filename}"'})


@app.get("/download/list")
def list_jobs(_=Depends(auth_deps)):
    return [j.to_dict() for j in sorted(jobs.values(),
            key=lambda x: x.created_at, reverse=True)]


@app.delete("/download/{job_id}")
def delete_job(job_id: str, _=Depends(auth_deps)):
    j = jobs.pop(job_id, None)
    if not j: raise HTTPException(404, "Not found")
    if j.file_path: Path(j.file_path).unlink(missing_ok=True)
    return {"deleted": True}


@app.post("/download/cancel_all")
def cancel_all(_=Depends(auth_deps)):
    count = 0
    for j in list(jobs.values()):
        if j.status in ("queued", "downloading"):
            j.status = "error"; j.error = "Cancelled by user"; count += 1
    return {"cancelled": count}


# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    rows = ""
    for j in sorted(jobs.values(), key=lambda x: x.created_at, reverse=True):
        color = {"done": "#4CAF50", "error": "#E94560",
                 "downloading": "#E94560", "queued": "#AAAAAA"}.get(j.status, "#fff")
        rows += f"""
        <tr>
          <td title="{html.escape(j.url)}">{html.escape(j.title or j.url[:60])}</td>
          <td style="color:{color};font-weight:bold">{j.status}</td>
          <td>{j.progress:.1f}%</td>
          <td>{j.speed}</td>
          <td>{j.eta}</td>
          <td>{_fmt_size(j.file_size)}</td>
          <td>
            {'<a href="/download/file/' + j.job_id + '" style="color:#E94560">⬇ Save</a>' if j.status == "done" else ""}
            <a href="#" onclick="del('{j.job_id}')" style="color:#888;margin-left:8px">✕</a>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>VideoPlayer Downloader</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#0F0F1A;color:#eee;font-family:'Segoe UI',system-ui,sans-serif;padding:24px}}
    h1{{color:#E94560;margin-bottom:24px;font-size:1.6rem}}
    .card{{background:#1A1A2E;border-radius:12px;padding:20px;margin-bottom:20px}}
    label{{display:block;color:#aaa;font-size:.8rem;margin-bottom:4px;margin-top:12px}}
    input,select{{width:100%;padding:10px;background:#0F0F1A;border:1px solid #333;
      border-radius:6px;color:#fff;font-size:.95rem}}
    button{{background:#E94560;color:#fff;border:none;padding:10px 24px;
      border-radius:6px;cursor:pointer;font-size:.95rem;margin-top:12px}}
    button:hover{{background:#c73350}}
    button.sec{{background:#333344}}
    table{{width:100%;border-collapse:collapse;font-size:.85rem}}
    th{{text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;font-weight:600}}
    td{{padding:8px;border-bottom:1px solid #1a1a2e;vertical-align:middle}}
    tr:hover{{background:#1a1a2e88}}
    #msg{{padding:10px;border-radius:6px;margin-top:12px;display:none}}
    .ok{{background:#1b3a1b;color:#4CAF50}} .err{{background:#3a1b1b;color:#E94560}}
    a{{color:#E94560;text-decoration:none}}
    .stat{{display:inline-block;background:#0F0F1A;border-radius:8px;padding:8px 16px;
      margin-right:12px;margin-bottom:8px;font-size:.85rem}}
    .stat span{{display:block;font-size:1.4rem;font-weight:bold;color:#E94560}}
  </style>
</head>
<body>
  <h1>🎬 VideoPlayer Downloader</h1>

  <div class="card">
    <div>
      <div class="stat">Jobs total<span>{len(jobs)}</span></div>
      <div class="stat">Active<span>{sum(1 for j in jobs.values() if j.status=='downloading')}</span></div>
      <div class="stat">Done<span>{sum(1 for j in jobs.values() if j.status=='done')}</span></div>
      <div class="stat">yt-dlp<span style="font-size:.9rem">{yt_dlp.version.__version__}</span></div>
    </div>
  </div>

  <div class="card">
    <h2 style="color:#E94560;margin-bottom:16px;font-size:1.1rem">New Download</h2>
    <label>Video URL</label>
    <input id="url" type="url" placeholder="https://youtube.com/watch?v=…">
    <label>Quality (leave blank for best)</label>
    <input id="fmt" type="text" placeholder="e.g. bestvideo[height<=720]+bestaudio/best">
    <button onclick="startDl()">⬇ Download</button>
    <button class="sec" style="margin-left:8px" onclick="fetchInfo()">🔍 Fetch Formats</button>
    <div id="msg"></div>
    <pre id="formats" style="background:#0F0F1A;padding:12px;border-radius:6px;margin-top:12px;
      font-size:.8rem;display:none;overflow-x:auto;color:#ccc;max-height:200px;overflow-y:auto"></pre>
  </div>

  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 style="color:#E94560;font-size:1.1rem">Jobs</h2>
      <div>
        <button class="sec" style="font-size:.8rem;padding:6px 12px" onclick="location.reload()">↻ Refresh</button>
        <button class="sec" style="font-size:.8rem;padding:6px 12px;margin-left:6px" onclick="cancelAll()">✕ Cancel Active</button>
      </div>
    </div>
    <table>
      <tr><th>Title/URL</th><th>Status</th><th>Progress</th><th>Speed</th><th>ETA</th><th>Size</th><th>Actions</th></tr>
      {rows or '<tr><td colspan="7" style="text-align:center;color:#666;padding:24px">No jobs yet</td></tr>'}
    </table>
  </div>

<script>
function msg(text, ok) {{
  const el = document.getElementById('msg');
  el.textContent = text; el.className = ok ? 'ok' : 'err'; el.style.display = 'block';
  setTimeout(() => el.style.display='none', 4000);
}}
async function startDl() {{
  const url = document.getElementById('url').value.trim();
  const fmt = document.getElementById('fmt').value.trim();
  if (!url) {{ msg('Enter a URL', false); return; }}
  try {{
    const r = await fetch('/download/start', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{url, format_id: fmt||null}})
    }});
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail);
    msg('Started: ' + d.job_id, true);
    setTimeout(() => location.reload(), 1500);
  }} catch(e) {{ msg(e.message, false); }}
}}
async function fetchInfo() {{
  const url = document.getElementById('url').value.trim();
  if (!url) {{ msg('Enter a URL', false); return; }}
  const pre = document.getElementById('formats');
  pre.textContent = 'Fetching…'; pre.style.display = 'block';
  try {{
    const r = await fetch('/info', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{url}})
    }});
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail);
    const lines = (d.formats||[]).map(f => `${{f.format_id.padEnd(40)}} ${{f.resolution.padEnd(12)}} ${{f.ext.padEnd(6)}} ${{f.note}}`);
    pre.textContent = ['format_id'.padEnd(40)+' resolution   ext    note', '-'.repeat(80), ...lines].join('\\n');
  }} catch(e) {{ pre.textContent = 'Error: '+e.message; }}
}}
async function del(id) {{
  await fetch('/download/'+id, {{method:'DELETE'}});
  location.reload();
}}
async function cancelAll() {{
  await fetch('/download/cancel_all', {{method:'POST'}});
  location.reload();
}}
// Auto-refresh every 5s if jobs are active
if ({str(any(j.status in ('queued','downloading') for j in jobs.values())).lower()}) {{
  setTimeout(() => location.reload(), 5000);
}}
</script>
</body></html>"""


def _fmt_size(b: int) -> str:
    if b >= 1024**3: return f"{b/1024**3:.2f} GB"
    if b >= 1024**2: return f"{b/1024**2:.1f} MB"
    if b >= 1024:    return f"{b/1024:.0f} KB"
    return f"{b} B" if b else "—"


# ---------------------------------------------------------------------------
# Background download
# ---------------------------------------------------------------------------
async def _run_download(job: DownloadJob):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _download_sync, job)


def _download_sync(job: DownloadJob):
    job.status = "downloading"
    try:
        # Get title first
        with yt_dlp.YoutubeDL({**_base_opts(), "skip_download": True}) as ydl:
            info     = ydl.extract_info(job.url, download=False)
            job.title     = info.get("title", "video")
            job.thumbnail = info.get("thumbnail", "")

        safe   = _sanitise(job.title)
        tmpl   = str(DOWNLOAD_DIR / f"{safe}_%(id)s.%(ext)s")
        opts   = _dl_opts(job, tmpl)

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(job.url, download=True)
            job.file_path, job.filename = _resolve_file(ydl, info, tmpl)
            if job.file_path:
                job.file_size = Path(job.file_path).stat().st_size

        job.status   = "done"
        job.progress = 100.0
        log.info(f"Done {job.job_id}: {job.filename} ({job.file_size} bytes)")

    except Exception as e:
        job.status = "error"
        job.error  = str(e)
        log.error(f"Failed {job.job_id}: {e}")


# ---------------------------------------------------------------------------
# Additional social media specific endpoints (v3 additions)
# ---------------------------------------------------------------------------

class SocialInfoRequest(BaseModel):
    url:              str
    extract_playlist: bool  = False
    cookies_content:  str   = ""   # optional cookies string (Netscape format)


@app.post("/social/info")
def get_social_info(req: SocialInfoRequest, _=Depends(auth_deps)):
    """
    Enhanced info endpoint for social media URLs.
    Handles: YouTube, Instagram, TikTok, Twitter/X, Facebook, TikTok, etc.
    Returns richer metadata including platform name and available streams.
    """
    try:
        opts = _info_opts()
        # Write cookies to temp file if provided
        cookie_file = None
        if req.cookies_content:
            import tempfile
            tf = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
            tf.write(req.cookies_content)
            tf.flush()
            cookie_file = tf.name
            opts["cookiefile"] = cookie_file

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(req.url, download=False)

        if cookie_file:
            import os as _os
            try: _os.unlink(cookie_file)
            except: pass

        # Build response
        platform = _detect_platform(req.url)

        raw_formats = info.get("formats", [])
        formats, seen = [], set()

        for f in raw_formats:
            vco = f.get("vcodec", "none")
            h   = f.get("height")
            w   = f.get("width")
            if not h or vco in (None, "none"):
                continue
            res = f"{w}x{h}" if w else f"{h}p"
            key = (res, f.get("ext",""), f.get("fps",0))
            if key in seen: continue
            seen.add(key)
            fps  = f.get("fps")
            size = f.get("filesize") or f.get("filesize_approx")
            formats.append({
                "format_id":  f.get("format_id",""),
                "ext":        f.get("ext",""),
                "resolution": res,
                "fps":        int(fps) if fps else None,
                "filesize":   int(size) if size else None,
                "vcodec":     vco,
                "acodec":     f.get("acodec","none"),
                "note":       f.get("format_note",""),
                "tbr":        f.get("tbr"),
            })

        # Sort best first
        formats.sort(key=lambda x: (
            int(x["resolution"].split("x")[-1]) if "x" in x["resolution"] else 0,
        ), reverse=True)

        # Add best option
        formats.insert(0, {
            "format_id":  "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "ext":        "mp4", "resolution": "Best Available",
            "fps": None, "filesize": None, "vcodec":"auto","acodec":"auto",
            "note": "Highest quality",
        })

        # Also add audio-only
        formats.append({
            "format_id": "bestaudio[ext=m4a]/bestaudio/best",
            "ext": "m4a", "resolution": "Audio Only",
            "fps": None, "filesize": None, "vcodec": "none", "acodec": "auto",
            "note": "Best audio",
        })

        return {
            "platform":    platform,
            "title":       info.get("title","Untitled"),
            "uploader":    info.get("uploader",""),
            "uploader_id": info.get("uploader_id",""),
            "duration":    info.get("duration"),
            "thumbnail":   info.get("thumbnail"),
            "description": (info.get("description") or "")[:1000],
            "view_count":  info.get("view_count"),
            "like_count":  info.get("like_count"),
            "upload_date": info.get("upload_date"),
            "webpage_url": info.get("webpage_url", req.url),
            "formats":     formats,
            "tags":        (info.get("tags") or [])[:10],
        }

    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        log.exception("Social info error")
        return {"count": 0, "results": []}


@app.post("/social/download")
async def social_download(req: DownloadRequest, bg: BackgroundTasks, _=Depends(auth_deps)):
    """Alias for /download/start — added for semantic clarity."""
    return await start_download(req, bg)


@app.get("/social/platforms")
def list_platforms():
    """Returns list of supported platform categories."""
    return {
        "video": ["YouTube", "Vimeo", "Dailymotion", "Bilibili", "Rumble", "Odysee"],
        "social": ["Instagram", "TikTok", "Twitter/X", "Facebook", "Pinterest",
                   "Snapchat", "LinkedIn", "Tumblr"],
        "streaming": ["Twitch", "Reddit"],
        "total_sites": "1000+",
        "reference": "https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md"
    }


@app.get("/social/extract-url")
def extract_video_url(url: str = "", _=Depends(auth_deps)):
    """
    Quickly extract the direct stream URL without downloading.
    Useful for streaming directly in the app instead of downloading.
    """
    if not url:
        raise HTTPException(400, "url parameter required")
    try:
        opts = {**_base_opts(), "skip_download": True, "quiet": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            # Get the best format URL
            formats = info.get("formats", [])
            best = None
            for f in reversed(formats):
                if f.get("url") and f.get("vcodec","none") != "none":
                    best = f
                    break
            if not best and formats:
                best = formats[-1]
            return {
                "title":    info.get("title",""),
                "url":      best.get("url","") if best else "",
                "ext":      best.get("ext","mp4") if best else "mp4",
                "manifest": info.get("manifest_url",""),
            }
    except Exception as e:
        raise HTTPException(422, str(e))


def _detect_platform(url: str) -> str:
    lower = url.lower()
    if "youtube.com" in lower or "youtu.be" in lower: return "YouTube"
    if "instagram.com" in lower: return "Instagram"
    if "tiktok.com" in lower: return "TikTok"
    if "twitter.com" in lower or "x.com" in lower: return "Twitter/X"
    if "facebook.com" in lower or "fb.watch" in lower: return "Facebook"
    if "vimeo.com" in lower: return "Vimeo"
    if "dailymotion.com" in lower: return "Dailymotion"
    if "reddit.com" in lower or "v.redd.it" in lower: return "Reddit"
    if "twitch.tv" in lower: return "Twitch"
    if "bilibili.com" in lower: return "Bilibili"
    if "rumble.com" in lower: return "Rumble"
    if "odysee.com" in lower: return "Odysee"
    if "pinterest.com" in lower: return "Pinterest"
    if "linkedin.com" in lower: return "LinkedIn"
    return "Unknown"


# ===========================================================================
# BATCH DOWNLOAD  — start multiple URLs in one request
# ===========================================================================

class BatchRequest(BaseModel):
    urls:      list[str]
    format_id: str | None = None

@app.post("/download/batch")
async def batch_download(req: BatchRequest, bg: BackgroundTasks, _=Depends(auth_deps)):
    """
    Queue multiple URLs for download at once.
    Returns list of job_ids in the same order as the input URLs.
    """
    if not req.urls:
        raise HTTPException(400, "urls list is empty")
    if len(req.urls) > 20:
        raise HTTPException(400, "Max 20 URLs per batch request")

    results = []
    for url in req.urls:
        active = sum(1 for j in jobs.values() if j.status == "downloading")
        if active >= MAX_CONCURRENT:
            # Queue it — it will wait
            pass
        job_id = str(uuid.uuid4())
        job    = DownloadJob(job_id, url.strip(), req.format_id)
        jobs[job_id] = job
        bg.add_task(_run_download, job)
        results.append({"url": url, "job_id": job_id, "status": "queued"})

    return {"batch_size": len(results), "jobs": results}


# ===========================================================================
# RETRY A FAILED JOB
# ===========================================================================

@app.post("/download/retry/{job_id}")
async def retry_job(job_id: str, bg: BackgroundTasks, _=Depends(auth_deps)):
    """
    Retry a failed or errored job using the same URL and format.
    Creates a new job with a new ID.
    """
    old = jobs.get(job_id)
    if not old:
        raise HTTPException(404, "Job not found")
    if old.status not in ("error", "done"):
        raise HTTPException(409, f"Job is still {old.status} — cannot retry")

    new_id  = str(uuid.uuid4())
    new_job = DownloadJob(new_id, old.url, old.format_id)
    jobs[new_id] = new_job
    bg.add_task(_run_download, new_job)
    return {"old_job_id": job_id, "new_job_id": new_id, "status": "queued"}


# ===========================================================================
# SUBTITLE SEARCH  — proxy to OpenSubtitles so API key stays server-side
# ===========================================================================

@app.get("/subtitle/search")
def subtitle_search(
    title:    str,
    language: str = "en",
    imdb_id:  str = "",
    limit:    int = 20,
    _=Depends(auth_deps)
):
    """
    Search OpenSubtitles.com for subtitles.
    Proxied through backend so the API key never lives in the APK.

    Set OPENSUBTITLES_API_KEY env var for higher rate limits.
    Anonymous access = 5 searches/day; registered = 200/day.
    """
    import urllib.request, urllib.parse, json as _json

    api_key = os.environ.get("OPENSUBTITLES_API_KEY", "")
    headers = {
        "User-Agent": "VideoPlayerApp v10.0",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Api-Key"] = api_key

    params = {"query": title, "languages": language, "order_by": "download_count",
              "order_direction": "desc"}
    if imdb_id:
        params["imdb_id"] = imdb_id.replace("tt", "")

    qs  = urllib.parse.urlencode(params)
    url = f"https://api.opensubtitles.com/api/v1/subtitles?{qs}"

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read())

        results = []
        for item in (data.get("data") or [])[:limit]:
            attrs = item.get("attributes", {})
            files = attrs.get("files", [])
            if not files:
                continue
            results.append({
                "id":               item.get("id", ""),
                "file_id":          files[0].get("file_id", 0),
                "movie_name":       attrs.get("release", attrs.get("movie_name", "")),
                "language":         attrs.get("language", language),
                "download_count":   attrs.get("download_count", 0),
                "rating":           attrs.get("ratings", 0),
                "fps":              attrs.get("fps", 0),
                "hearing_impaired": attrs.get("hearing_impaired", False),
                "hd":               attrs.get("hd", False),
            })

        return {"count": len(results), "results": results}

    except Exception as e:
        return {"count": 0, "results": []}


@app.get("/subtitle/download")
def subtitle_download(file_id: int, _=Depends(auth_deps)):
    """
    Download a subtitle file from OpenSubtitles and return its content.
    The Android app can save it directly as a .srt file.

    file_id comes from /subtitle/search results.
    """
    import urllib.request, json as _json

    api_key = os.environ.get("OPENSUBTITLES_API_KEY", "")
    headers = {
        "User-Agent": "VideoPlayerApp v10.0",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Api-Key"] = api_key

    # Step 1: Get download link
    try:
        body    = _json.dumps({"file_id": file_id}).encode()
        req     = urllib.request.Request(
            "https://api.opensubtitles.com/api/v1/download",
            data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            link_data = _json.loads(resp.read())

        dl_url = link_data.get("link", "")
        fname  = link_data.get("file_name", f"subtitle_{file_id}.srt")
        if not dl_url:
            return {"count": 0, "results": []}

        # Step 2: Download the actual SRT
        req2 = urllib.request.Request(dl_url, headers={"User-Agent": "VideoPlayerApp v10.0"})
        with urllib.request.urlopen(req2, timeout=20) as resp2:
            content = resp2.read()

        # Return as plain text (the Android app saves it as .srt)
        from fastapi.responses import Response
        return Response(
            content=content,
            media_type="text/plain; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fname}"',
                "X-Subtitle-Filename": fname,
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        return {"count": 0, "results": []}


# ===========================================================================
# DISK USAGE  — how much space is used by downloads
# ===========================================================================

@app.get("/system/disk")
def disk_usage(_=Depends(auth_deps)):
    """Returns download directory disk usage and system disk stats."""
    import shutil

    dl_size  = sum(f.stat().st_size for f in DOWNLOAD_DIR.rglob("*") if f.is_file())
    dl_files = sum(1 for f in DOWNLOAD_DIR.rglob("*") if f.is_file())

    try:
        total, used, free = shutil.disk_usage(str(DOWNLOAD_DIR))
    except Exception:
        total = used = free = 0

    return {
        "download_dir":        str(DOWNLOAD_DIR),
        "download_dir_size":   dl_size,
        "download_dir_files":  dl_files,
        "disk_total":          total,
        "disk_used":           used,
        "disk_free":           free,
        "disk_free_pct":       round(free / total * 100, 1) if total else 0,
    }


@app.delete("/system/cleanup")
def cleanup_files(older_than_hours: int = 1, _=Depends(auth_deps)):
    """Delete download files older than N hours. Returns count deleted."""
    cutoff  = time.time() - older_than_hours * 3600
    deleted = 0
    freed   = 0
    for f in list(DOWNLOAD_DIR.rglob("*")):
        if f.is_file() and f.suffix == ".part":
            continue  # never delete in-progress yt-dlp temp files
        if f.is_file() and f.stat().st_mtime < cutoff:
            freed += f.stat().st_size
            f.unlink(missing_ok=True)
            deleted += 1
    # Also clean matching jobs
    old_jobs = [jid for jid, j in list(jobs.items())
                if j.created_at < cutoff and j.status in ("done", "error")]
    for jid in old_jobs:
        jobs.pop(jid, None)

    return {"deleted_files": deleted, "freed_bytes": freed, "cleaned_jobs": len(old_jobs)}
