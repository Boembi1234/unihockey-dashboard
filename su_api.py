"""The Swiss Unihockey API. This is the one place that talks to it.

api-v2.swissunihockey.ch was open; its endpoints now live under
office.swissunihockey.ch/api/legacy and want an auth_token as URL parameter:

    POST /api/legacy/bo/session/auth   api_key=…  secret=…   → {"token": "…"}
    GET  /api/legacy/games/<id>?auth_token=<token>

Paths and parameters below /api/legacy are the ones api-v2 had under /api
(games, games/<id>, games/<id>/teams/<is_home>/players, game_events/<id>,
players/<id>, players/<id>/statistics, teams/<id>/players).

The token is fetched on the first call and again when the API answers 401.
It is part of every URL, and requests puts the URL into its error messages —
so nothing leaves this module with the token in it: not a response's url, not
an exception, not a log line.

    SU_API_KEY=... SU_API_SECRET=... python fetch_upcoming.py
"""
import os, time, logging, threading, requests

log = logging.getLogger(__name__)

BASE_URL = "https://office.swissunihockey.ch/api/legacy"
AUTH_URL = f"{BASE_URL}/bo/session/auth"
SU_API_KEY    = os.environ.get("SU_API_KEY", "")
SU_API_SECRET = os.environ.get("SU_API_SECRET", "")

SESSION = requests.Session()
SESSION.headers["Accept"] = "application/json"

_lock = threading.Lock()
_token = None


class SuAuthError(RuntimeError):
    """No token: credentials missing or refused. Retrying does not help."""


def _authenticate():
    if not SU_API_KEY or not SU_API_SECRET:
        raise SuAuthError("SU_API_KEY / SU_API_SECRET not set")
    error = None
    for attempt in range(3):
        try:
            r = SESSION.post(AUTH_URL, data={"api_key": SU_API_KEY, "secret": SU_API_SECRET},
                             timeout=15)
        except requests.RequestException as e:
            error = e
            log.warning(f"  SU auth attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.5)
            continue
        if r.status_code in (401, 403):
            raise SuAuthError(f"SU auth refused [{r.status_code}]: {r.text[:200]}")
        try:
            r.raise_for_status()
            token = r.json().get("token")
        except (requests.RequestException, ValueError, AttributeError) as e:
            error = e
            log.warning(f"  SU auth attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.5)
            continue
        if not token:
            raise SuAuthError("SU auth answered without a token")
        # GitHub Actions: keep the token out of the log whoever prints it.
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::add-mask::{token}", flush=True)
        return token
    raise SuAuthError(f"SU auth failed: {error}")


def _get_token(stale=None):
    """The current token. `stale` is a token the API just refused: it is
    replaced, unless another thread already did."""
    global _token
    with _lock:
        if _token is None or _token == stale:
            _token = _authenticate()
        return _token


def _redact(text, token):
    return str(text).replace(token, "***")


def get(endpoint, params=None, timeout=15):
    """One GET, authenticated. Returns the response whatever its status; a 401
    gets one retry with a fresh token."""
    url = f"{BASE_URL}/{endpoint}"
    token = _get_token()
    for retry in (True, False):
        try:
            r = SESSION.get(url, params={**(params or {}), "auth_token": token}, timeout=timeout)
        except requests.RequestException as e:
            raise type(e)(_redact(e, token)) from None
        r.url = _redact(r.url, token)
        if r.status_code != 401 or not retry:
            break
        token = _get_token(stale=token)
    return r


def api_get(endpoint, params=None, retries=3):
    """The JSON of an endpoint, or None after `retries` failed attempts."""
    for attempt in range(retries):
        try:
            r = get(endpoint, params)
            r.raise_for_status()
            return r.json()
        except SuAuthError:
            raise
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(1.5)
    return None
