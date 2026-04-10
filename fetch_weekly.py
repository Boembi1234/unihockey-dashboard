"""Weekly refresh: fetch current season for all leagues, sync to Supabase.

Handles:
- Auto-discovery of groups per league
- Lineup fetching with correct home/away assignment
- Name resolution (abbreviated → full names from lineups)
- Backfilling penalties/lineups/phases for existing games
- Incremental: only fetches games not already in the DB
"""
import sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time, logging
from fetch_lupl import (
    get_db, fetch_game_rows, store_game, fetch_and_store_goals,
    fetch_and_store_lineup, discover_groups, backfill_penalties,
    backfill_lineups, backfill_game_phases, build_name_map,
    sync_to_supabase, TARGET_LEAGUES, SLEEP,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def run():
    log.info("=== Weekly Refresh ===")
    conn = get_db()
    total_new = total_goals = total_pen = 0

    for lg_cfg in TARGET_LEAGUES:
        league     = lg_cfg["league"]
        game_class = lg_cfg["game_class"]
        label      = lg_cfg["label"]
        explicit_group = lg_cfg.get("group")

        for season in lg_cfg.get("seasons", []):
            # Discover groups if none specified
            if explicit_group:
                groups_to_fetch = [explicit_group]
            else:
                time.sleep(SLEEP)
                discovered = discover_groups(league, game_class, season)
                if discovered:
                    groups_to_fetch = discovered
                    log.info(f"  {label} {season}: discovered {len(discovered)} groups: {discovered}")
                else:
                    groups_to_fetch = [None]

            for grp in groups_to_fetch:
                tag = f" ({grp})" if grp else ""
                log.info(f"\n{label}{tag}  {season}/{season+1}…")

                rows, region = fetch_game_rows(league, game_class, season, group=grp)
                log.info(f"  Found {len(rows)} games in API")

                league_group_label = grp if grp else None
                new = 0

                for gid, row in rows.items():
                    if conn.execute("SELECT 1 FROM games WHERE game_id=?", (gid,)).fetchone():
                        continue

                    res = store_game(conn, gid, row, season, label,
                                     league_group_override=league_group_label or region.get(gid))
                    if res is None:
                        continue

                    home_id, away_id, home_name, away_name, iso_date, weekday = res
                    time.sleep(SLEEP)
                    result = fetch_and_store_goals(conn, gid, home_id, away_id,
                                                   home_name, away_name, iso_date, weekday, season)
                    ng, np = result if isinstance(result, tuple) else (0, 0)
                    conn.commit()
                    total_new += 1
                    total_goals += ng
                    total_pen += np
                    new += 1

                    # Fetch lineups
                    time.sleep(SLEEP)
                    for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
                        time.sleep(SLEEP)
                        fetch_and_store_lineup(conn, gid, tid, is_home, tname, season, iso_date)
                    conn.commit()

                log.info(f"  {new} new games stored")

    log.info(f"\n── Summary ─────────────────")
    log.info(f"  New games: {total_new}")
    log.info(f"  Goals:     {total_goals}")
    log.info(f"  Penalties: {total_pen}")

    if total_new > 0:
        log.info("\n── Backfilling…")
        backfill_penalties(conn)
        backfill_lineups(conn)
        backfill_game_phases(conn)

        log.info("\n── Rebuilding name map…")
        conn.execute("DELETE FROM name_map")
        conn.commit()
        build_name_map(conn)

    log.info("\n── Syncing to Supabase…")
    sync_to_supabase(conn)
    conn.close()
    log.info("\n✓ Weekly refresh complete.")


if __name__ == "__main__":
    run()
