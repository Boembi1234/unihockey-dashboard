"""Fetch upcoming games (next 7 days) for all tracked leagues and push to Supabase.

Only fetches the current/latest round from the API (no full-history crawl),
parses game rows, keeps those within the next 7 days, and upserts them into
the fb_games table.  Games without a final result get result = NULL so they
don't affect historical stats (which filter for valid results).

Run daily via GitHub Actions or manually:
    SUPABASE_SERVICE_KEY=... python fetch_upcoming.py
"""

import os, sys, json, time, re, logging, requests
from datetime import date, timedelta, datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api-v2.swissunihockey.ch/api"
SUPABASE_URL = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SLEEP = 0.4
CURRENT_SEASON = 2025

LEAGUES = [
    {"league": 24, "game_class": 11, "label": "Herren L-UPL",   "group": None},
    {"league":  1, "game_class": 11, "label": "Herren NLA",     "group": None},
    {"league":  2, "game_class": 11, "label": "Herren NLB",     "group": None},
    {"league":  3, "game_class": 11, "label": "Herren 1. Liga", "group": "Gruppe 1"},
    {"league":  3, "game_class": 11, "label": "Herren 1. Liga", "group": "Gruppe 2"},
]

LEAGUE_MAP = {
    "Herren L-UPL": "Herren NLA",
    "Herren SML":   "Herren NLA",
}

# ── API helpers ───────────────────────────────────────────────────────────────

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


def cell_text(cell, index=0):
    if not isinstance(cell, dict):
        return str(cell) if cell else ""
    t = cell.get("text", "")
    if isinstance(t, list):
        return t[index] if index < len(t) else (t[0] if t else "")
    return t or ""


def cell_link_id(cell):
    link = cell.get("link") or {}
    ids = link.get("ids", [])
    return str(ids[0]) if ids else None


def team_hash(name):
    return abs(hash(name)) % 10**9 if name else None


def parse_date(s):
    if not s:
        return None
    s = s.strip().split(" ")[0]
    relative = {"heute": 0, "gestern": -1, "morgen": 1}
    if s.lower() in relative:
        d = date.today() + timedelta(days=relative[s.lower()])
        return d.isoformat()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name


# ── Supabase helpers ──────────────────────────────────────────────────────────

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


# ── Fetch upcoming rounds ────────────────────────────────────────────────────

def fetch_upcoming_games(league_id, game_class, season, group=None):
    """Fetch games from the current + next round and return those within 7 days."""
    params = {
        "mode":       "list",
        "league":     league_id,
        "game_class": game_class,
        "season":     season,
    }
    if group:
        params["group"] = group

    today = date.today()
    cutoff = today + timedelta(days=7)
    games = []
    visited = set()

    # Fetch up to 3 rounds (current + next two) to cover the 7-day window
    round_param = None
    for _ in range(3):
        p = dict(params)
        if round_param is not None:
            p["round"] = round_param

        raw = api_get("games", p)
        if not raw:
            break

        data = raw.get("data", raw) if isinstance(raw, dict) else {}
        regions = data.get("regions", [])

        for region in regions:
            region_title = (region.get("title") or region.get("text") or "").strip()
            for row in region.get("rows", []):
                cells = row.get("cells", [])
                gid = None
                for cell in cells:
                    link = cell.get("link") or {}
                    if link.get("page") == "game_detail":
                        ids = link.get("ids", [])
                        if ids:
                            gid = str(ids[0])
                            break
                if not gid:
                    continue

                game = parse_game_row(gid, cells, season, region_title, group)
                if not game:
                    continue

                iso_date = game.get("date")
                if not iso_date:
                    continue

                try:
                    game_date = date.fromisoformat(iso_date)
                except ValueError:
                    continue

                if today <= game_date <= cutoff:
                    games.append(game)

        # Navigate forward to get the next round
        slider = data.get("slider", {})
        nxt = slider.get("next", {}).get("set_in_context", {}).get("round")
        if nxt and nxt not in visited:
            visited.add(nxt)
            round_param = nxt
            time.sleep(SLEEP)
        else:
            break

    return games


def parse_game_row(game_id, cells, season, region_title, group_override):
    """Parse a game row from the API into a dict matching the fb_games schema."""
    if len(cells) >= 8:
        # New API layout
        datetime_raw = cell_text(cells[0], 0)
        parts = datetime_raw.split(" ", 1)
        date_raw = parts[0]
        time_raw = parts[1] if len(parts) > 1 else ""
        loc_raw = cell_text(cells[1], 0)
        loc_city = ""
        home_name = cell_text(cells[2], 0)
        home_id = cell_link_id(cells[2]) or team_hash(home_name)
        away_name = cell_text(cells[6], 0)
        away_id = cell_link_id(cells[6]) or team_hash(away_name)
        result = cell_text(cells[7], 0)
    elif len(cells) >= 6:
        date_raw = cell_text(cells[0], 0)
        time_raw = cell_text(cells[0], 1)
        loc_raw = cell_text(cells[1], 0)
        loc_city = cell_text(cells[1], 1)
        home_name = cell_text(cells[3], 0)
        away_name = cell_text(cells[4], 0)
        result = cell_text(cells[5], 0)
        home_id = team_hash(home_name)
        away_id = team_hash(away_name)
    elif len(cells) >= 5:
        date_raw = cell_text(cells[0], 0)
        time_raw = cell_text(cells[0], 1)
        loc_raw = cell_text(cells[1], 0)
        loc_city = cell_text(cells[1], 1)
        home_name = cell_text(cells[2], 0)
        away_name = cell_text(cells[3], 0)
        result = cell_text(cells[4], 0)
        home_id = team_hash(home_name)
        away_id = team_hash(away_name)
    else:
        return None

    iso_date = parse_date(date_raw)
    if not iso_date:
        return None

    # Treat pending results as null
    if not result or result in ("-:-", "-", ""):
        result = None

    league_group = group_override or region_title or ""

    return {
        "game_id":       game_id,
        "home_team_id":  home_id,
        "away_team_id":  away_id,
        "home_team_raw": home_name,
        "away_team_raw": away_name,
        "date":          iso_date,
        "time":          time_raw or None,
        "season":        season,
        "result":        result,
        "location":      loc_raw or None,
        "location_city": loc_city or None,
        "league_group":  league_group or None,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)

    all_games = []
    seen_ids = set()

    for cfg in LEAGUES:
        label = cfg["label"]
        group = cfg["group"]
        tag = f" {group}" if group else ""
        log.info(f"Fetching {label}{tag}...")

        games = fetch_upcoming_games(
            cfg["league"], cfg["game_class"], CURRENT_SEASON, group
        )

        for g in games:
            if g["game_id"] not in seen_ids:
                g["league"] = norm_league(label)
                seen_ids.add(g["game_id"])
                all_games.append(g)

        log.info(f"  {len(games)} upcoming games found")
        time.sleep(SLEEP)

    log.info(f"\nTotal: {len(all_games)} upcoming games in next 7 days")

    if all_games:
        sb_upsert("fb_games", all_games)
        log.info(f"Upserted {len(all_games)} games to Supabase fb_games")
    else:
        log.info("No upcoming games to sync")


if __name__ == "__main__":
    main()
