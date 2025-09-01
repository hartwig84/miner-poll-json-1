# server.py (with CSV persistence for 24–72h reload)
import os
import re
import asyncio
import json
import time
import csv
import httpx
from pathlib import Path
from collections import deque
from typing import Dict, List, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# =======================
# Configuration (env vars)
# =======================
JSON_URL = os.getenv("JSON_URL", "http://localhost:8001/sample.json")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "1000"))

# Metrics mapping: { "Display Name": "dot.path[0].to.field" }
DEFAULT_METRICS = {
    "HR 1m (TH/s)": "hashrate1m",
    "HR 5m (TH/s)": "hashrate5m",
    "HR 1h (TH/s)": "hashrate1hr",
    "HR 1d (TH/s)": "hashrate1d",
    "HR 7d (TH/s)": "hashrate7d",
    "Workers": "workers",
    "Shares": "shares",
    "Best Share": "bestshare",
    "Last Share Age (s)": "__computed__.lastshare_age_s",
    "Worker0 HR 1m (TH/s)": "worker[0].hashrate1m",
}
METRICS: Dict[str, str] = json.loads(os.getenv("METRICS_JSON", json.dumps(DEFAULT_METRICS)))

# Optional per-metric scaling (e.g., convert H/s -> TH/s for display)
DEFAULT_SCALES = {
    "HR 1m (TH/s)": 1e-12,
    "HR 5m (TH/s)": 1e-12,
    "HR 1h (TH/s)": 1e-12,
    "HR 1d (TH/s)": 1e-12,
    "HR 7d (TH/s)": 1e-12,
    "Workers": 1,
    "Shares": 1,
    "Best Share": 1,
    "Last Share Age (s)": 1,
    "Worker0 HR 1m (TH/s)": 1e-12,
}
SCALES: Dict[str, float] = json.loads(os.getenv("SCALES_JSON", json.dumps(DEFAULT_SCALES)))

# ---- Persistence config ----
PERSIST_ENABLED = os.getenv("PERSIST_ENABLED", "1") != "0"
PERSIST_PATH = Path(os.getenv("PERSIST_PATH", "data/history.csv")).resolve()
RETENTION_HOURS = float(os.getenv("RETENTION_HOURS", "72"))
COMPACT_INTERVAL_MIN = float(os.getenv("COMPACT_INTERVAL_MIN", "30"))

RETENTION_MS = int(RETENTION_HOURS * 3600 * 1000)

# =======================
# Helpers
# =======================
UNIT_MAP = {"": 1.0, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15}

def parse_unit_number(val) -> float:
    """
    Parses numbers that may have K/M/G/T/P suffixes (e.g., "10.7T", "674G").
    Returns float in base units.
    """
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        m = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([kKmMgGtTpP]?)\s*", val)
        if m:
            num = float(m.group(1))
            unit = m.group(2).upper()
            return num * UNIT_MAP.get(unit, 1.0)
        try:
            return float(val)
        except Exception:
            pass
    raise ValueError(f"Cannot parse numeric value from: {val!r}")

def get_by_dot_path(obj, path: str):
    """
    Dot-path with optional array index, e.g. 'worker[0].hashrate1m' or 'status.cpu'
    """
    cur = obj
    for part in path.split("."):
        m = re.fullmatch(r"([^\[\]]+)(?:\[(\d+)\])?", part)
        if not m:
            raise KeyError(f"Bad path segment: {part}")
        key, idx = m.group(1), m.group(2)
        if not isinstance(cur, dict) or key not in cur:
            raise KeyError(f"Key '{key}' not found in path '{path}'")
        cur = cur[key]
        if idx is not None:
            i = int(idx)
            if not (isinstance(cur, list) and 0 <= i < len(cur)):
                raise KeyError(f"Index [{i}] out of range for '{key}' in path '{path}'")
            cur = cur[i]
    return cur

# =======================
# App & Storage
# =======================
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory time series: metric_name -> deque[(timestamp_ms, value)]
_series: Dict[str, deque] = {name: deque(maxlen=WINDOW_SIZE) for name in METRICS.keys()}

# =======================
# Persistence: CSV (long format)
# =======================
# CSV columns: ts_ms,metric,value
CSV_HEADER = ["ts_ms", "metric", "value"]

def _ensure_persist_dirs():
    if PERSIST_ENABLED:
        PERSIST_PATH.parent.mkdir(parents=True, exist_ok=True)

def _persist_append(ts_ms: int, batch: Dict[str, float]):
    """Append one row per metric to CSV."""
    if not PERSIST_ENABLED:
        return
    _ensure_persist_dirs()
    new_file = not PERSIST_PATH.exists()
    with PERSIST_PATH.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(CSV_HEADER)
        for name, val in batch.items():
            w.writerow([ts_ms, name, f"{val:.12g}"])

def _load_history_into_memory():
    """On startup, load retained history from CSV into _series."""
    if not (PERSIST_ENABLED and PERSIST_PATH.exists()):
        return
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - RETENTION_MS
    try:
        with PERSIST_PATH.open("r", newline="") as f:
            r = csv.DictReader(f)
            if r.fieldnames != CSV_HEADER:
                # different/older header; skip for safety
                return
            for row in r:
                try:
                    ts = int(row["ts_ms"])
                    if ts < cutoff:
                        continue
                    metric = row["metric"]
                    if metric not in _series:
                        continue  # ignore rows for metrics no longer tracked
                    val = float(row["value"])
                    _series[metric].append((ts, val))
                except Exception:
                    continue
    except Exception:
        # If the file is malformed, we skip loading rather than crash.
        pass

def _compact_csv():
    """Rewrite CSV keeping only rows within retention window and current METRICS."""
    if not (PERSIST_ENABLED and PERSIST_PATH.exists()):
        return
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - RETENTION_MS
    tmp_path = PERSIST_PATH.with_suffix(".tmp")
    try:
        with PERSIST_PATH.open("r", newline="") as fin, tmp_path.open("w", newline="") as fout:
            rin = csv.DictReader(fin)
            wout = csv.writer(fout)
            wout.writerow(CSV_HEADER)
            for row in rin:
                try:
                    ts = int(row["ts_ms"])
                    metric = row["metric"]
                    if ts >= cutoff and metric in METRICS:
                        wout.writerow([row["ts_ms"], metric, row["value"]])
                except Exception:
                    continue
        tmp_path.replace(PERSIST_PATH)
    except Exception:
        # Best-effort; ignore compaction failures
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

async def compactor_loop():
    """Periodically compact the CSV to enforce retention on disk."""
    if not PERSIST_ENABLED:
        return
    # Small delay so first poll happens quickly
    await asyncio.sleep(min(5.0, POLL_INTERVAL))
    interval = max(1.0, COMPACT_INTERVAL_MIN * 60.0)
    while True:
        _compact_csv()
        await asyncio.sleep(interval)

# =======================
# Poller
# =======================
async def poller():
    """
    Background task that polls JSON_URL at a fixed interval and updates _series & CSV.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            t_ms = int(time.time() * 1000)
            batch_to_persist: Dict[str, float] = {}
            try:
                resp = await client.get(JSON_URL)
                resp.raise_for_status()
                payload = resp.json()

                for name, path in METRICS.items():
                    try:
                        # Computed metric: seconds since last share
                        if path == "__computed__.lastshare_age_s":
                            now_s = time.time()
                            lastshare = payload.get("lastshare", now_s)
                            value = max(0.0, float(now_s - float(lastshare)))
                        else:
                            raw = get_by_dot_path(payload, path)
                            value = parse_unit_number(raw)

                        value *= float(SCALES.get(name, 1.0))
                        value_f = float(value)

                        _series[name].append((t_ms, value_f))
                        # Drop by time as well (keeps memory strictly within retention window)
                        while _series[name] and (t_ms - _series[name][0][0]) > RETENTION_MS:
                            _series[name].popleft()

                        batch_to_persist[name] = value_f
                    except Exception:
                        continue

                # Append this sample batch to CSV
                if batch_to_persist:
                    _persist_append(t_ms, batch_to_persist)

            except Exception:
                # Optionally log failures
                pass

            await asyncio.sleep(POLL_INTERVAL)



@app.on_event("startup")
async def on_startup():
    # Ensure series deques have up-to-date maxlen
    for k in list(_series.keys()):
        _series[k] = deque(_series[k], maxlen=WINDOW_SIZE)

    # Load retained history into memory
    _load_history_into_memory()

    # Start poller and compactor
    asyncio.create_task(poller())
    asyncio.create_task(compactor_loop())

@app.get("/data")
def get_data():
    return {"series": {name: list(points) for name, points in _series.items()}}

@app.get("/config")
def get_config():
    return {
        "json_url": JSON_URL,
        "poll_interval_sec": POLL_INTERVAL,
        "metrics": METRICS,
        "scales": SCALES,
        "window_size": WINDOW_SIZE,
        "persist_enabled": PERSIST_ENABLED,
        "persist_path": str(PERSIST_PATH),
        "retention_hours": RETENTION_HOURS,
    }

# Serve the frontend (place index.html in ./public)
app.mount("/", StaticFiles(directory="public", html=True), name="static")
