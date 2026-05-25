#!/usr/bin/env python3
"""
Quick smoke-test for the VideoPlayer backend.
Run with:  python test_api.py [base_url]
"""

import json, sys, time
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import urlencode

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8000"
OK   = 0
FAIL = 0

def check(label, passed, detail=""):
    global OK, FAIL
    if passed:
        print(f"  ✓  {label}")
        OK += 1
    else:
        print(f"  ✗  {label}: {detail}")
        FAIL += 1

def get(path, timeout=10):
    try:
        with urlopen(f"{BASE}{path}", timeout=timeout) as r:
            return json.loads(r.read()), r.status
    except URLError as e:
        return None, str(e)

from urllib.error import HTTPError

def post(path, data, timeout=20):
    body = json.dumps(data).encode()
    req  = Request(f"{BASE}{path}", data=body,
                   headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read()), r.status
    except HTTPError as e:
        print("🔥 ERROR BODY:", e.read().decode())
        return None, e.code
    except URLError as e:
        return None, str(e)
        
def delete(path, timeout=10):
    req = Request(f"{BASE}{path}", method="DELETE")
    try:
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read()), r.status
    except URLError as e:
        return None, str(e)

print(f"\n🎬 VideoPlayer Backend Tests  ({BASE})")
print("-" * 55)

# ── Health ──────────────────────────────────────────────────────────────────
d, s = get("/health")
check("GET /health returns ok",       d and d.get("status") == "ok", s)
check("Health shows yt-dlp version",  d and "yt_dlp_version" in d, d)

# ── Info ─────────────────────────────────────────────────────────────────────
TEST_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Big Buck Bunny (short, public domain)
d, s = post("/info", {"url": TEST_URL})
check("POST /info fetches metadata",      d and "title" in d, s)
check("POST /info returns formats list",  d and isinstance(d.get("formats"), list) and len(d["formats"]) > 0, d)
check("POST /info has thumbnail",         d and d.get("thumbnail"), d)

# ── Social Info ──────────────────────────────────────────────────────────────
d, s = post("/social/info", {"url": TEST_URL})
check("POST /social/info returns platform", d and "platform" in d, s)
check("POST /social/info has view_count",   d and "view_count" in d, d)

# ── Social Platforms ─────────────────────────────────────────────────────────
d, s = get("/social/platforms")
check("GET /social/platforms lists sites",  d and "video" in d and "social" in d, s)

# ── Extract URL ──────────────────────────────────────────────────────────────
from urllib.parse import quote

d, s = get(f"/social/extract-url?url={quote(TEST_URL)}")
check("GET /social/extract-url returns url", d and "url" in d, s)

# ── Download lifecycle ───────────────────────────────────────────────────────
d, s = post("/download/start", {"url": TEST_URL, "format_id": "worst"})
check("POST /download/start queues job",  d and "job_id" in d, s)
job_id = d["job_id"] if d else None

if job_id:
    time.sleep(1)
    d, s = get(f"/download/status/{job_id}")
    check("GET /download/status returns job",    d and "status" in d, s)
    check("Status has progress field",           d and "progress" in d, d)

    d, s = get("/download/list")
    check("GET /download/list returns array",    isinstance(d, list), s)
    check("Job appears in list",                 any(j.get("job_id") == job_id for j in (d or [])), d)

    d, s = delete(f"/download/{job_id}")
    check("DELETE /download/{job_id} works",     d and d.get("deleted"), s)

# ── Batch download ───────────────────────────────────────────────────────────
d, s = post("/download/batch", {"urls": [TEST_URL], "format_id": "worst"})
check("POST /download/batch queues jobs",    d and "jobs" in d, s)
check("Batch returns correct count",         d and d.get("batch_size") == 1, d)
# Clean up batch job
if d and d.get("jobs"):
    for bj in d["jobs"]:
        delete(f"/download/{bj['job_id']}")

# ── Cancel all ───────────────────────────────────────────────────────────────
d, s = post("/download/cancel_all", {})
check("POST /download/cancel_all works",     d and "cancelled" in d, s)

# ── Subtitle search ──────────────────────────────────────────────────────────
d, s = get("/subtitle/search?title=big+buck+bunny&language=en")
check("GET /subtitle/search returns results",(d and "results" in d) or s == 502, s)
check("Subtitle results is a list",           isinstance((d or {}).get("results"), list), d)

# ── System / disk ────────────────────────────────────────────────────────────
d, s = get("/system/disk")
check("GET /system/disk returns usage",      d and "disk_free" in d, s)

d, s = delete("/system/cleanup?older_than_hours=0")
check("DELETE /system/cleanup runs",         d and "deleted_files" in d, s)

# ── Docs ─────────────────────────────────────────────────────────────────────
try:
    with urlopen(f"{BASE}/docs") as r:
        ok = r.status == 200
except:
    ok = False

check("GET /docs (Swagger UI) accessible", ok, "")


# ── Dashboard ────────────────────────────────────────────────────────────────
try:
    with urlopen(f"{BASE}/dashboard", timeout=5) as r:
        body = r.read().decode()
    check("GET /dashboard returns HTML",     "VideoPlayer" in body, "")
except Exception as e:
    check("GET /dashboard returns HTML",     False, str(e))

print("-" * 55)
print(f"\nDone.  ✓ {OK} passed   {'✗ ' + str(FAIL) + ' failed' if FAIL else '🎉 All passed!'}\n")
sys.exit(0 if FAIL == 0 else 1)
