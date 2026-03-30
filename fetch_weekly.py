"""Weekly refresh: only fetch the current season for all leagues.
INSERT OR IGNORE skips already-stored games, so only new games land in the DB.
"""
import sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time, logging
from fetch_lupl import (
    get_db, fetch_game_rows, store_game, fetch_and_store_goals,
    fetch_and_store_lineup, backfill_game_phases, build_name_map,
    sync_to_supabase, SLEEP,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

CURRENT_SEASON = 2025

# Only the active season for each league — backfills are done manually
WEEKLY_LEAGUES = [
    {"league": 24, "game_class": 11, "label": "Herren L-UPL",   "group": None},
    {"league":  1, "game_class": 11, "label": "Herren NLA",     "group": None},
    {"league":  2, "game_class": 11, "label": "Herren NLB",     "group": None},
    {"league":  3, "game_class": 11, "label": "Herren 1. Liga", "group": "Gruppe 1"},
    {"league":  3, "game_class": 11, "label": "Herren 1. Liga", "group": "Gruppe 2"},
]

conn = get_db()
total_new = total_goals = total_pen = 0

for cfg in WEEKLY_LEAGUES:
    label = cfg["label"]
    group = cfg["group"]
    tag = f" {group}" if group else ""
    log.info(f"{label}{tag}  {CURRENT_SEASON}/{CURRENT_SEASON+1}...")

    rows, region = fetch_game_rows(cfg["league"], cfg["game_class"], CURRENT_SEASON, group=group)
    log.info(f"  Found {len(rows)} games in API")

    new = 0
    for gid, row in rows.items():
        if conn.execute("SELECT 1 FROM games WHERE game_id=?", (gid,)).fetchone():
            continue
        league_group = group if group else region.get(gid)
        res = store_game(conn, gid, row, CURRENT_SEASON, label,
                         league_group_override=league_group)
        if res is None:
            continue
        home_id, away_id, home_name, away_name, iso_date, weekday = res
        time.sleep(SLEEP)
        result = fetch_and_store_goals(conn, gid, home_id, away_id,
                                       home_name, away_name, iso_date, weekday, CURRENT_SEASON)
        ng, np = result if isinstance(result, tuple) else (0, 0)
        conn.commit()
        total_new += 1; total_goals += ng; total_pen += np; new += 1
        time.sleep(SLEEP)
        for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
            time.sleep(SLEEP)
            fetch_and_store_lineup(conn, gid, tid, is_home, tname, CURRENT_SEASON, iso_date)
        conn.commit()

    log.info(f"  {new} new games stored")

log.info(f"\nTotal new: {total_new} games, {total_goals} goals, {total_pen} penalties")

if total_new > 0:
    log.info("Running backfill_game_phases...")
    backfill_game_phases(conn)
    log.info("Running build_name_map...")
    build_name_map(conn)

log.info("Syncing to Supabase...")
sync_to_supabase(conn)
conn.close()
