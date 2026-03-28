"""
fetch_player_stats.py
=====================
Standalone script: fetches official per-club statistics for all players
that have a pid in the database, caches results, then rebuilds data.json.

Run this instead of the full fetch_lupl.py when you only need to update
player statistics without re-fetching all game data.

Usage:
  python fetch_player_stats.py
"""

import sqlite3, json, time, logging, os
from datetime import datetime
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

BASE_URL  = "https://api-v2.swissunihockey.ch/api"
DB_PATH   = "swiss_floorball_lupl.db"
JSON_PATH = "data.json"
SLEEP     = 0.3

SESSION = requests.Session()
SESSION.headers["Accept"] = "application/json"


def api_get(endpoint, retries=3):
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(1.5)
    return None


def unwrap(resp):
    if isinstance(resp, dict) and "data" in resp:
        return resp["data"]
    return resp or {}


def cell_text(cell, index=0):
    if not isinstance(cell, dict):
        return str(cell) if cell else ""
    t = cell.get("text", "")
    if isinstance(t, list):
        return t[index] if index < len(t) else (t[0] if t else "")
    return t or ""


def fetch_player_statistics(pid):
    raw = api_get(f"players/{pid}/statistics")
    if not raw:
        return []
    rows = []
    for region in unwrap(raw).get("regions", []):
        for row in region.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) < 7:
                continue
            try:
                games = int(cell_text(cells[3]))
            except (ValueError, TypeError):
                continue  # skip header rows
            rows.append({
                "season":  cell_text(cells[0]),
                "liga":    cell_text(cells[1]),
                "verein":  cell_text(cells[2]),
                "games":   games,
                "goals":   int(cell_text(cells[4]) or 0),
                "assists": int(cell_text(cells[5]) or 0),
                "points":  int(cell_text(cells[6]) or 0),
            })
    return rows


# ── Connect and ensure cache table exists ─────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS player_statistics_cache (
        pid        INTEGER PRIMARY KEY,
        stats_json TEXT    NOT NULL,
        fetched_at TEXT    NOT NULL
    )
""")
conn.commit()

# ── Load existing cache ────────────────────────────────────────────────────────
stats_cache = {pid: json.loads(js) for pid, js in conn.execute(
    "SELECT pid, stats_json FROM player_statistics_cache"
)}
log.info(f"Cache: {len(stats_cache)} players already fetched")

# ── Collect all unique pids from lineups ──────────────────────────────────────
pids = {row[0] for row in conn.execute(
    "SELECT DISTINCT player_id FROM lineups WHERE player_id IS NOT NULL"
)}
log.info(f"Total players with pid: {len(pids)}")

to_fetch = [pid for pid in pids if pid not in stats_cache]
log.info(f"Need to fetch: {len(to_fetch)}  (~{len(to_fetch)*SLEEP/60:.1f} min)")

# ── Fetch missing stats ────────────────────────────────────────────────────────
for i, pid in enumerate(to_fetch, 1):
    stats = fetch_player_statistics(pid)
    stats_cache[pid] = stats
    conn.execute(
        "INSERT OR REPLACE INTO player_statistics_cache (pid, stats_json, fetched_at) VALUES (?, ?, ?)",
        (pid, json.dumps(stats, ensure_ascii=False), datetime.now().isoformat())
    )
    conn.commit()
    if i % 50 == 0 or i == len(to_fetch):
        log.info(f"  {i}/{len(to_fetch)} fetched...")
    time.sleep(SLEEP)

log.info("All player stats fetched.")

# ── Load existing data.json and enrich player_meta with verein_stats ──────────
if not os.path.exists(JSON_PATH):
    log.error(f"{JSON_PATH} not found — run fetch_lupl.py first to generate it")
    raise SystemExit(1)

with open(JSON_PATH, encoding="utf-8") as f:
    data = json.load(f)

player_meta = data.get("player_meta", {})

# Map full_name → abbrev key (same logic as fetch_lupl.py)
full_to_abbrev = {full: abbrev for abbrev, full in conn.execute(
    "SELECT abbrev_name, full_name FROM name_map"
)}

# pid per full_name (most recent season)
player_ids = {}
for name, pid in conn.execute(
    "SELECT player_raw, player_id FROM lineups WHERE player_id IS NOT NULL"
    " GROUP BY player_raw ORDER BY season DESC"
):
    player_ids.setdefault(name, pid)

# Build full_name → pid using abbreviated key lookup
key_to_pid = {}
for full_name, pid in player_ids.items():
    key = full_to_abbrev.get(full_name, full_name)
    key_to_pid[key] = pid

# Attach verein_stats to every player entry that has a pid
enriched = 0
for key, meta in player_meta.items():
    pid = key_to_pid.get(key) or meta.get("pid")
    if not pid:
        continue
    meta["pid"] = pid
    meta["verein_stats"] = stats_cache.get(pid, [])
    enriched += 1

data["player_meta"] = player_meta

with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

size_mb = os.path.getsize(JSON_PATH) / 1024 / 1024
log.info(f"\n✅  Exported to {JSON_PATH}")
log.info(f"    PlayerMeta enriched: {enriched} players")
log.info(f"    Size: {size_mb:.2f} MB")
