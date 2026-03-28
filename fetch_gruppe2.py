"""Fetch 1. Liga Gruppe 2 (seasons 2019-2025) + Gruppe 1 season 2019."""
import sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sqlite3, time, logging
from fetch_lupl import (fetch_game_rows, store_game, fetch_and_store_goals,
                        fetch_and_store_lineup, get_db, SLEEP)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

FETCH = [
    {"group": "Gruppe 2", "seasons": [2025, 2024, 2023, 2022, 2021, 2020, 2019]},
    {"group": "Gruppe 1", "seasons": [2025, 2019]},
]

conn = get_db()
total_games = total_goals = total_pen = 0

for cfg in FETCH:
    group = cfg["group"]
    for season in cfg["seasons"]:
        log.info(f"1. Liga {group}  {season}/{season+1}...")
        rows, region = fetch_game_rows(3, 11, season, group=group)
        log.info(f"  Found {len(rows)} games")
        new = 0
        for gid, row in rows.items():
            if conn.execute("SELECT 1 FROM games WHERE game_id=?", (gid,)).fetchone():
                continue
            res = store_game(conn, gid, row, season, "Herren 1. Liga",
                             league_group_override=group)
            if res is None:
                continue
            home_id, away_id, home_name, away_name, iso_date, weekday = res
            time.sleep(SLEEP)
            result = fetch_and_store_goals(conn, gid, home_id, away_id,
                                           home_name, away_name, iso_date, weekday, season)
            ng, np = result if isinstance(result, tuple) else (0, 0)
            conn.commit()
            total_games += 1; total_goals += ng; total_pen += np; new += 1
            time.sleep(SLEEP)
            for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
                time.sleep(SLEEP)
                fetch_and_store_lineup(conn, gid, tid, is_home, tname, season, iso_date)
            conn.commit()
            if new % 20 == 0 and new > 0:
                log.info(f"  ...{new} new so far")
        log.info(f"  Season done: {new} new games")

log.info(f"Total: {total_games} new games, {total_goals} goals, {total_pen} penalties")
for row in conn.execute("SELECT league, COUNT(*) FROM games GROUP BY league ORDER BY COUNT(*) DESC"):
    log.info(f"  {row[0]}: {row[1]}")
conn.close()
