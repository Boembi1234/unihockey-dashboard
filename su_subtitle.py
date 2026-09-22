"""The subtitle of GET /api/games/<id> carries league, group, round and phase in
one line. This is the one place that takes it apart. Pure functions.

    "Junioren U16 B Gruppe 2 Runde 10 2026/27"
        → league "Junioren U16 B", group "Gruppe 2", round 10, phase "Qualifikation"
    "Herren GF 1. Liga Auf-/Abstiegs-Playoffs Auf-/Abstiegs-Playoffs 2025/26"
        → league "Herren 1. Liga", phase "Auf-/Abstieg"
    "Herren KF 3. Liga Gruppe 1 Runde 3 2025/26"
        → league "Herren 3. Liga KF" (Kleinfeld goes at the end, as fb_games has it)
    "Mobiliar Unihockey Cup Männer 1/8-Final 2025/26"
        → league "Mobiliar Unihockey Cup Männer", phase "Final"

League names follow fb_games: "Herren GF L-UPL" is "Herren NLA", the top women's
league is "Damen L-UPL" (app migration 20260730100000 merged "Damen NLA" into it),
"Junioren U21A" is "Junioren U21 A", cups keep their full name. Phases are the
vocabulary fb_games already holds (Qualifikation, Playoff, Playout, Viertelfinal,
Halbfinal, Final, Superfinal) plus "Auf-/Abstieg" for the promotion/relegation
rounds, which no importer had ever picked up. Cup rounds ("1/64-Final",
"Achtelfinal") count as "Final", as the daily import has always filed them.
"""
import re

_SEASON_RE = re.compile(r"\s*\b(\d{4})/(\d{2})\s*$")
_GROUP_RE  = re.compile(r"\bGruppe\s+(\d+)\b")
_ROUND_RE  = re.compile(r"\bRunde\s+(\d+)\b")

_TOP = {"L-UPL": "NLA", "NLB": "NLB"}

# (pattern on the text with season/group/round removed, league name builder)
_LEAGUE_RES = [
    (re.compile(r"^(Herren|Damen)\s+(GF|KF)\s+(L-UPL|NLB|\d\.\s*Liga)\b"),
     lambda m: _active(m.group(1), m.group(2), m.group(3))),
    (re.compile(r"^(Junioren|Juniorinnen)\s+U(\d{2})\s*([A-D])\b"),
     lambda m: f"{m.group(1)} U{m.group(2)} {m.group(3)}"),
    (re.compile(r"^(Junioren|Juniorinnen)\s+([A-E])\s+Regional\b"),
     lambda m: f"{m.group(1)} {m.group(2)} Regional"),
    (re.compile(r"^(Mobiliar Unihockey Cup (?:Männer|Frauen)|Mobiliar Ligacup (?:Männer|Frauen)"
                r"|Supercup (?:Männer|Frauen))\b"),
     lambda m: m.group(1)),
]

_PHASES = [
    ("superfinal",      "Superfinal"),
    ("auf-/abstieg",    "Auf-/Abstieg"),
    ("auf-abstieg",     "Auf-/Abstieg"),
    ("aufstieg",        "Auf-/Abstieg"),
    ("abstieg",         "Auf-/Abstieg"),
    ("relegation",      "Auf-/Abstieg"),
    ("barrage",         "Auf-/Abstieg"),
    ("playout",         "Playout"),
    ("viertelfinal",    "Viertelfinal"),
    ("halbfinal",       "Halbfinal"),
    ("final",           "Final"),
    ("playoff",         "Playoff"),
    ("play-off",        "Playoff"),
]


def _active(sex, field, tier):
    tier = re.sub(r"(\d)\.\s*Liga", r"\1. Liga", tier)
    if tier in _TOP:
        if sex == "Damen" and tier == "L-UPL":
            return "Damen L-UPL"
        return f"{sex} {_TOP[tier]}"
    return f"{sex} {tier}" + (" KF" if field == "KF" else "")


def phase_from_text(text):
    low = (text or "").lower()
    for needle, phase in _PHASES:
        if needle in low:
            return phase
    return "Qualifikation"


def parse_subtitle(subtitle):
    """→ {"league", "group", "round", "phase", "season", "rest"}; league is None
    when the text matches no known competition (rest then holds the whole text)."""
    text = " ".join((subtitle or "").split())
    season = None
    m = _SEASON_RE.search(text)
    if m:
        season = int(m.group(1))
        text = text[:m.start()].strip()

    group = round_ = None
    m = _GROUP_RE.search(text)
    if m:
        group = f"Gruppe {m.group(1)}"
        text = (text[:m.start()] + text[m.end():]).strip()
    m = _ROUND_RE.search(text)
    if m:
        round_ = int(m.group(1))
        text = (text[:m.start()] + text[m.end():]).strip()

    league = None
    rest = text
    for pattern, build in _LEAGUE_RES:
        m = pattern.match(text)
        if m:
            league = build(m)
            rest = text[m.end():].strip()
            break

    return {
        "league": league,
        "group":  group,
        "round":  round_,
        "phase":  phase_from_text(rest),
        "season": season,
        "rest":   " ".join(rest.split()),
    }
