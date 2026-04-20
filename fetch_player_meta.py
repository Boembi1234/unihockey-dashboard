"""
fetch_player_meta.py
====================
Fetches per-player metadata (portrait, club, number, position, year_of_birth,
height, weight, license) from the Swiss Unihockey API and upserts into the
Supabase fb_players table.

Scope: only players who appeared in a game in season 2025 (i.e., their
player_id shows up in home_lineup or away_lineup of any fb_games row with
season = '2025').

Usage:
  SUPABASE_SERVICE_KEY=<key> python fetch_player_meta.py
"""

import os, json, time, logging, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

BASE_URL             = "https://api-v2.swissunihockey.ch/api"
SUPABASE_URL         = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
TARGET_SEASON        = "2025"
UPSERT_BATCH         = 200
WORKERS              = 8   # parallel API requests
REQUEST_TIMEOUT      = 15

SESSION = requests.Session()


def sb_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }


def fetch_2025_player_ids():
    """Stream fb_games with season=2025 and union home_lineup + away_lineup."""
    log.info(f"Fetching player IDs from fb_games (season={TARGET_SEASON})…")
    ids = set()
    offset = 0
    page = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/fb_games"
        params = {
            "select":    "home_lineup,away_lineup",
            "season":    f"eq.{TARGET_SEASON}",
            "offset":    str(offset),
            "limit":     str(page),
        }
        r = SESSION.get(url, headers=sb_headers(), params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for row in rows:
            for pid in (row.get("home_lineup") or []):
                if pid: ids.add(int(pid))
            for pid in (row.get("away_lineup") or []):
                if pid: ids.add(int(pid))
        if len(rows) < page:
            break
        offset += page
    log.info(f"  Found {len(ids)} distinct player IDs")
    return sorted(ids)


def parse_player_response(pid, data):
    """Turn API JSON into a fb_players row."""
    try:
        cells = data["data"]["regions"][0]["rows"][0]["cells"]
    except (KeyError, IndexError, TypeError):
        return None

    def txt(cell):
        if not isinstance(cell, dict):
            return None
        arr = cell.get("text")
        if isinstance(arr, list) and arr:
            val = arr[0]
            if isinstance(val, str):
                v = val.strip()
                return v if v and v != "-" else None
        return None

    def img(cell):
        if isinstance(cell, dict):
            im = cell.get("image") or {}
            url = im.get("url")
            return url if url else None
        return None

    # Order from API: portrait, club, number, position, year_of_birth, height, weight, license
    portrait     = img(cells[0]) if len(cells) > 0 else None
    club         = txt(cells[1]) if len(cells) > 1 else None
    number       = txt(cells[2]) if len(cells) > 2 else None
    position     = txt(cells[3]) if len(cells) > 3 else None
    yob_raw      = txt(cells[4]) if len(cells) > 4 else None
    height       = txt(cells[5]) if len(cells) > 5 else None
    weight       = txt(cells[6]) if len(cells) > 6 else None
    license_     = txt(cells[7]) if len(cells) > 7 else None

    yob = None
    if yob_raw:
        try: yob = int(yob_raw)
        except ValueError: yob = None

    player_name = data.get("data", {}).get("subtitle") or None

    row = {
        "player_id":       pid,
        "portrait_url":    portrait,
        "club":            club,
        "number":          number,
        "year_of_birth":   yob,
        "height":          height,
        "weight":          weight,
        "license":         license_,
        "meta_fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # Only overwrite position/player_name if API actually returned them,
    # so we don't blank out existing fb_players data.
    if position:    row["position"]    = position
    if player_name: row["player_name"] = player_name
    return row


def fetch_one(pid):
    url = f"{BASE_URL}/players/{pid}"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return parse_player_response(pid, r.json())
    except Exception as e:
        log.warning(f"  player {pid} failed: {e}")
        return None


def batched(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def upsert_to_supabase(rows):
    # PostgREST requires all rows in one POST to have the exact same key set.
    # We omit position/player_name when the API didn't return them (to avoid
    # overwriting lineup-derived values), so rows can have 4 possible key sets.
    # Group by key-set and upsert each group separately.
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/fb_players"
    groups = {}
    for r in rows:
        key = tuple(sorted(r.keys()))
        groups.setdefault(key, []).append(r)
    for grp in groups.values():
        resp = SESSION.post(url, headers=sb_headers(), data=json.dumps(grp, default=str),
                            timeout=REQUEST_TIMEOUT)
        if resp.status_code not in (200, 201):
            log.error(f"  upsert failed [{resp.status_code}]: {resp.text[:300]}")
            resp.raise_for_status()


def run():
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        return

    ids = fetch_2025_player_ids()
    if not ids:
        log.warning("No player IDs found. Aborting.")
        return

    rows = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_one, pid): pid for pid in ids}
        for fut in as_completed(futures):
            row = fut.result()
            done += 1
            if row:
                rows.append(row)
            if done % 200 == 0:
                rate = done / max(time.time() - t0, 1e-6)
                eta  = (len(ids) - done) / max(rate, 1e-6)
                log.info(f"  {done}/{len(ids)}  ({rate:.1f}/s, ETA {eta/60:.1f} min)")

            # Flush to Supabase in batches as we go
            if len(rows) >= UPSERT_BATCH:
                upsert_to_supabase(rows)
                rows = []

    if rows:
        upsert_to_supabase(rows)

    log.info(f"✅ Done. {done} players processed in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    run()
