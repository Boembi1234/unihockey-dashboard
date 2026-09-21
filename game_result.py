"""Reading the result of a game from the swiss unihockey API. Pure functions.

The result cell of GET /api/games/<id> is a list of texts:

    ["6:10", "(1:2, 3:6, 2:2)"]                played
    ["3:4", "(1:0, 0:1, 2:2, 0:1)", "n.V."]     overtime: the extra period is listed
    ["3:2", "(0:2, 1:0, 1:0, 0:0)", "n.P."]     shootout: the deciding goal is in the headline only
    ["0:5 (ff)", "(-:-, -:-, -:-)"]             forfeit
    ["-:-"]                                     cancelled
    [""]                                        not played yet

Two things make the headline unsafe to copy as it is:

* While a game is running it is the LIVE score — in every league, down to
  Junioren D, because every league has an event feed. The app scores Tipps
  against fb_games.result once and never again, so an interim score written
  there is frozen as the final result. `feed_has_ended` is the check.
* It can be plain wrong: game 1096439 had "0:0" next to periods that add up to
  3:12 and an event feed with 15 goals; 1098346 was "0:0" and later "-:-".
  All three 0:0 among the 606 games imported since April 2026 were bogus, so
  `check_result` never passes a 0:0 on: the game is reported with what the
  periods and the feed say, and someone enters the result by hand.

Why nothing is derived from goals instead: neither the period scores nor the
event feed are a result. Cup games often list periods that miss goals (38 of
606), a shootout winner is in the headline only, and in 1096879 / 1097057 feed
AND periods count one goal twice (logged as "Torschütze" and as "Eigentor" in
the same second) while the headline is the official result. A headline that is
not 0:0 is therefore taken as it is.
"""
import re

_SCORE_RE  = re.compile(r"^\s*(\d+)\s*:\s*(\d+)")
_PERIOD_RE = re.compile(r"(\d+|-)\s*:\s*(\d+|-)")
_EVENT_SCORE_RE = re.compile(r"(\d+)\s*:\s*(\d+)")

OK     = "ok"        # headline is the result
REVIEW = "review"    # 0:0 — not written, a person has to look
NONE   = "none"      # no result: not played, cancelled, unreadable


def parse_result_cell(texts):
    """Split the result cell → {headline, score, periods}.

    score   (home, away) from the headline, None without a result ("" / "-:-")
    periods [(home, away), …] or None when missing or not numeric (forfeit)
    """
    parts = [t.strip() for t in (texts or []) if isinstance(t, str) and t.strip()]
    headline = parts[0] if parts else ""
    m = _SCORE_RE.match(headline)
    periods = None
    for p in parts[1:]:
        if p.startswith("("):
            found = _PERIOD_RE.findall(p)
            if found and all("-" not in pair for pair in found):
                periods = [(int(h), int(a)) for h, a in found]
    return {
        "headline": headline,
        "score":    (int(m.group(1)), int(m.group(2))) if m else None,
        "periods":  periods,
    }


def check_result(texts, feed_score=None):
    """→ (result text for fb_games.result | None, status, note).

    `feed_score` (see `last_feed_score`) only adds evidence to the note of a
    0:0 — pass it when you have it.
    """
    cell = parse_result_cell(texts)
    score, periods = cell["score"], cell["periods"]
    if score is None:
        return None, NONE, None
    if score != (0, 0) or "ff" in cell["headline"].lower():
        return cell["headline"], OK, None

    evidence = []
    if periods:
        total = (sum(h for h, _ in periods), sum(a for _, a in periods))
        if sum(total):
            evidence.append(f"periods add up to {total[0]}:{total[1]}")
    if feed_score and sum(feed_score):
        evidence.append(f"event feed ends at {feed_score[0]}:{feed_score[1]}")
    if evidence:
        return None, REVIEW, "headline 0:0, but " + " and ".join(evidence)
    return None, REVIEW, "headline 0:0 and no goals anywhere — enter it by hand if it really ended 0:0"


# ── Event feed (GET /api/game_events/<id>) ───────────────────────────────────

def _event_texts(event_rows):
    for row in event_rows or []:
        cells = row.get("cells") or []
        if len(cells) > 1 and isinstance(cells[1], dict):
            text = cells[1].get("text") or [""]
            yield text[0] if isinstance(text, list) else str(text)


def feed_has_ended(event_rows):
    """True once the feed has its "Spielende" row — the same test the app's
    gameday-resolver uses."""
    return any("spielende" in (t or "").lower() for t in _event_texts(event_rows))


def last_feed_score(event_rows):
    """Highest score of any goal row ("Torschütze 3:2", "Eigentor 3:3") — the
    feed is newest-first, but this does not rely on the order. None without goals."""
    best = None
    for t in _event_texts(event_rows):
        low = (t or "").lower()
        if "torsch" not in low and "eigentor" not in low:
            continue
        m = _EVENT_SCORE_RE.search(t)
        if m:
            s = (int(m.group(1)), int(m.group(2)))
            if best is None or sum(s) > sum(best):
                best = s
    return best
