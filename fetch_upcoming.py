"""Fetch upcoming games (next 7 days) across all Swiss floorball competitions
and push to Supabase.

Two-stage fetch:
 1. mode=current sweep walks the date slider forward to collect game IDs for
    every competition on each day (NLA/NLB/1.-5. Liga, Damen, Mobiliar Cup,
    juniors — whatever SU runs).
 2. /api/games/<id> on each ID fills in the real team IDs, location with
    coordinates, accurate date/time, referees, and a rich subtitle.

Run daily via GitHub Actions or manually:
    SUPABASE_SERVICE_KEY=... python fetch_upcoming.py
"""

import os, sys, json, time, logging, requests
from datetime import date, timedelta, datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api-v2.swissunihockey.ch/api"
SUPABASE_URL = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SLEEP = 0.3
CURRENT_SEASON = 2026
DAYS_AHEAD = 7

# Normalise SU labels to the canonical names the app filters on.
LEAGUE_MAP = {
    "Herren L-UPL":                  "Herren NLA",
    "Herren SML":                    "Herren NLA",
    "Damen L-UPL":                   "Damen NLA",
    "Mobiliar Unihockey Cup Männer": "Mobiliar Cup Herren",
    "Mobiliar Unihockey Cup Frauen": "Mobiliar Cup Damen",
}

# ── HTTP ──────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers["Accept"] = "application/json"


def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.5)
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def cell_text(cell, index=0):
    if not isinstance(cell, dict):
        return str(cell) if cell else ""
    t = cell.get("text", "")
    if isinstance(t, list):
        return t[index] if index < len(t) else (t[0] if t else "")
    return t or ""


def cell_link_ids(cell):
    link = (cell or {}).get("link") or {}
    return link.get("ids") or []


def parse_iso_date(s):
    """Accepts 'DD.MM.YYYY' or 'YYYY-MM-DD', returns (iso, weekday)."""
    if not s:
        return None, None
    s = s.strip().split(" ")[0]
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d.isoformat(), d.strftime("%A")
        except ValueError:
            pass
    return None, None


def phase_from_label(label):
    if not label:
        return "Qualifikation"
    low = label.lower()
    if "cup" in low:
        return "Cup"
    if "final" in low and "viertel" in low:
        return "Playoff-Viertelfinal"
    if "halbfinal" in low or "semi" in low:
        return "Playoff-Halbfinal"
    if "final" in low:
        return "Playoff-Final"
    if "playoff" in low or "play-off" in low:
        return "Playoffs"
    return "Qualifikation"


def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name


# ── Supabase ──────────────────────────────────────────────────────────────────

def sb_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }


def sb_upsert(table, rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = SESSION.post(url, headers=sb_headers(), data=json.dumps(rows, default=str))
    if r.status_code not in (200, 201):
        log.error(f"  Supabase {table} failed [{r.status_code}]: {r.text[:300]}")
        r.raise_for_status()


# ── Stage 1: collect game IDs via mode=current ───────────────────────────────

def sweep_game_ids(season, days=DAYS_AHEAD):
    """Walk mode=current forward day by day; return [(game_id, region_label)]."""
    today  = date.today()
    cutoff = today + timedelta(days=days)
    ids = []
    seen_ids = set()
    seen_dates = set()
    after_date = None

    for _ in range(20):                        # generous upper bound
        params = {"mode": "current", "season": season}
        if after_date:
            params["after_date"] = after_date

        raw = api_get("games", params)
        if not raw:
            break

        data = raw.get("data", raw) if isinstance(raw, dict) else {}
        ctx  = data.get("context", {}) or {}
        page_date = ctx.get("on_date") or after_date or today.isoformat()

        try:
            d = date.fromisoformat(page_date)
        except ValueError:
            break
        if d > cutoff:
            break
        if page_date in seen_dates:
            break
        seen_dates.add(page_date)

        page_count = 0
        for region in data.get("regions", []):
            region_label = (region.get("text") or region.get("title") or "").strip()
            for row in region.get("rows", []):
                gid = str(row.get("id") or "")
                if not gid or gid in seen_ids:
                    continue
                ids.append((gid, region_label))
                seen_ids.add(gid)
                page_count += 1

        log.info(f"  {page_date}: {page_count} games")

        nxt = (data.get("slider") or {}).get("next", {}).get("set_in_context", {}).get("after_date")
        if not nxt or nxt == after_date:
            break
        after_date = nxt
        time.sleep(SLEEP)

    return ids


# ── Stage 2: per-game detail → fb_games row ──────────────────────────────────

def fetch_game_detail(game_id, region_label, season):
    """Hit /games/<id> and build a row matching the fb_games schema.

    The detail endpoint declares cell order via `headers[*].key`, so we index
    by key instead of position — safe against future column reshuffles."""
    raw = api_get(f"games/{game_id}")
    if not raw:
        return None

    data = raw.get("data", raw) if isinstance(raw, dict) else {}
    headers = data.get("headers", []) or []
    keys = [(h.get("key") or "") for h in headers]
    regions = data.get("regions", []) or []
    if not regions or not regions[0].get("rows"):
        return None

    cells = regions[0]["rows"][0].get("cells", [])
    by_key = {k: cells[i] for i, k in enumerate(keys) if i < len(cells) and k}

    home_cell = by_key.get("home_name") or by_key.get("home_logo") or {}
    away_cell = by_key.get("away_name") or by_key.get("away_logo") or {}

    home_ids = cell_link_ids(home_cell)
    away_ids = cell_link_ids(away_cell)
    home_name = cell_text(by_key.get("home_name"))
    away_name = cell_text(by_key.get("away_name"))

    iso_date, weekday = parse_iso_date(cell_text(by_key.get("date")))
    if not iso_date:
        log.warning(f"  game {game_id}: no date, skipping")
        return None

    time_raw = cell_text(by_key.get("time")) or None
    result   = cell_text(by_key.get("result")) or None
    if result in ("", "-", "-:-"):
        result = None

    loc_cell = by_key.get("location") or {}
    location = cell_text(loc_cell) or None
    # Last whitespace-separated chunk is usually the city (often "Gossau SG").
    location_city = location.split()[-1] if location else None

    subtitle = (data.get("subtitle") or "").strip() or None
    phase = phase_from_label(subtitle or region_label)
    league_label = norm_league(region_label) or region_label

    return {
        "game_id":       game_id,
        "home_team_id":  int(home_ids[0]) if home_ids else None,
        "away_team_id":  int(away_ids[0]) if away_ids else None,
        "home_team_raw": home_name,
        "away_team_raw": away_name,
        "date":          iso_date,
        "weekday":       weekday,
        "time":          time_raw,
        "season":        season,
        "result":        result,
        "location":      location,
        "location_city": location_city,
        "league_group":  region_label or None,
        "subtitle":      subtitle,
        "phase":         phase,
        "league":        league_label,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)

    log.info(f"Stage 1: sweep mode=current for season {CURRENT_SEASON}, next {DAYS_AHEAD} days…")
    id_pairs = sweep_game_ids(CURRENT_SEASON, days=DAYS_AHEAD)
    log.info(f"  Collected {len(id_pairs)} game IDs")

    if not id_pairs:
        log.info("Nothing to fetch.")
        return

    log.info(f"Stage 2: fetching details for {len(id_pairs)} games…")
    games = []
    skipped = 0
    for i, (gid, region_label) in enumerate(id_pairs, 1):
        row = fetch_game_detail(gid, region_label, CURRENT_SEASON)
        if row:
            games.append(row)
        else:
            skipped += 1
        if i % 25 == 0 or i == len(id_pairs):
            log.info(f"  {i}/{len(id_pairs)} fetched ({skipped} skipped)")
        time.sleep(SLEEP)

    # Per-league summary so the log is useful.
    by_league = {}
    for g in games:
        by_league[g["league"]] = by_league.get(g["league"], 0) + 1
    for label, n in sorted(by_league.items(), key=lambda x: (-x[1], x[0])):
        log.info(f"  {n:>4}  {label}")

    log.info(f"\nTotal: {len(games)} games to upsert ({skipped} skipped)")

    if games:
        sb_upsert("fb_games", games)
        log.info(f"Upserted {len(games)} games to Supabase fb_games")


if __name__ == "__main__":
    main()
