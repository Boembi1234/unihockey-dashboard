"""Fetch upcoming games (next 7 days) across all Swiss floorball competitions
and push to Supabase.

Uses the SU API's `mode=current` endpoint — same source the live frontend
uses. No league/game_class filter, so this picks up every competition the
API exposes for the date: NLA / NLB / 1.-5. Liga, Damen, Mobiliar Cup,
juniors, regional play-offs… whatever is on the schedule. The script walks
the date slider forward until it goes past the 7-day cutoff.

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
SLEEP = 0.4
CURRENT_SEASON = 2026
DAYS_AHEAD = 7

# Canonicalise the labels SU uses so app filters stay stable.
LEAGUE_MAP = {
    "Herren L-UPL": "Herren NLA",
    "Herren SML":   "Herren NLA",
    "Damen L-UPL":  "Damen NLA",
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
        return None, None
    s = s.strip().split(" ")[0]
    relative = {"heute": 0, "gestern": -1, "morgen": 1}
    if s.lower() in relative:
        d = date.today() + timedelta(days=relative[s.lower()])
        return d.isoformat(), d.strftime("%A")
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


# ── Game row parser ───────────────────────────────────────────────────────────

def parse_game_row(game_id, cells, season, region_title, subtitle=None, phase=None):
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

    iso_date, weekday = parse_date(date_raw)
    if not iso_date:
        return None

    # Treat pending results as null.
    if not result or result in ("-:-", "-", ""):
        result = None

    return {
        "game_id":       game_id,
        "home_team_id":  home_id,
        "away_team_id":  away_id,
        "home_team_raw": home_name,
        "away_team_raw": away_name,
        "date":          iso_date,
        "weekday":       weekday or None,
        "time":          time_raw or None,
        "season":        season,
        "result":        result,
        "location":      loc_raw or None,
        "location_city": loc_city or None,
        "league_group":  region_title or None,
        "subtitle":      subtitle,
        "phase":         phase or "Qualifikation",
    }


# ── Sweep mode=current across the 7-day window ───────────────────────────────

def fetch_all_upcoming(season, days=DAYS_AHEAD):
    """Walk mode=current forward day-by-day until we pass the cutoff. Every
    region the API returns lands in the result with its own league label,
    so Cup / juniors / regional games come along for the ride."""
    today  = date.today()
    cutoff = today + timedelta(days=days)
    games  = []
    seen_game_ids = set()
    seen_dates    = set()
    after_date = None

    # Generous upper bound — SU's slider often skips empty days, so 7 calls
    # is plenty even on a busy weekend; the slider-out break handles the rest.
    for _ in range(20):
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
            region_title = (region.get("title") or region.get("text") or "").strip()
            league_label = norm_league(region_title) or region_title
            phase = phase_from_label(region_title)

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
                if not gid or gid in seen_game_ids:
                    continue

                game = parse_game_row(gid, cells, season, region_title, phase=phase)
                if not game or not game.get("date"):
                    continue
                try:
                    gd = date.fromisoformat(game["date"])
                except ValueError:
                    continue
                if not (today <= gd <= cutoff):
                    continue

                game["league"] = league_label
                games.append(game)
                seen_game_ids.add(gid)
                page_count += 1

        log.info(f"  {page_date}: {page_count} games")

        # Walk forward via slider.next.set_in_context.after_date.
        nxt = (data.get("slider") or {}).get("next", {}).get("set_in_context", {}).get("after_date")
        if not nxt or nxt == after_date:
            break
        after_date = nxt
        time.sleep(SLEEP)

    return games


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)

    log.info(f"Sweeping mode=current for season {CURRENT_SEASON}, next {DAYS_AHEAD} days…")
    all_games = fetch_all_upcoming(CURRENT_SEASON, days=DAYS_AHEAD)

    # Light summary by league so the log is useful.
    by_league = {}
    for g in all_games:
        by_league[g["league"]] = by_league.get(g["league"], 0) + 1
    for label, n in sorted(by_league.items(), key=lambda x: (-x[1], x[0])):
        log.info(f"  {n:>4}  {label}")

    log.info(f"\nTotal: {len(all_games)} upcoming games in next {DAYS_AHEAD} days")

    if all_games:
        sb_upsert("fb_games", all_games)
        log.info(f"Upserted {len(all_games)} games to Supabase fb_games")
    else:
        log.info("No upcoming games to sync")


if __name__ == "__main__":
    main()
