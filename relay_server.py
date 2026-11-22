"""
relay_server.py — Cricket Control Panel relay (VPS-side)

Architecture:
    Entity Sports push ─┐
    Entity Sports REST ─┼─→ this server (holds tokens) ─→ per-user WS sessions
                        │                                    ├─ panel(s)
                        │                                    └─ clicker
    Auth: every WS/REST request carries `token=usr_xxx` (user license key)

Runs on VPS behind nginx/caddy TLS. Clients connect to:
    wss://relay.yourdomain.com/relay?token=usr_xxx&role=panel
    wss://relay.yourdomain.com/relay?token=usr_xxx&role=clicker
    https://relay.yourdomain.com/api/matches       (Bearer usr_xxx)
    https://relay.yourdomain.com/api/match/{key}   (Bearer usr_xxx)
    https://relay.yourdomain.com/api/click         (Bearer usr_xxx, POST)
"""
import asyncio
import gzip
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ── optional fast paths ─────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

try:
    import orjson as _orjson
except ImportError:
    _orjson = None


def _dumps(obj) -> str:
    if _orjson is not None:
        try:
            return _orjson.dumps(obj).decode("utf-8")
        except Exception:
            pass
    return json.dumps(obj, separators=(",", ":"), default=str)


# ═══════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════
log = logging.getLogger("relay")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PUSH_TOKEN = os.environ.get("ES_PUSH_TOKEN", "").strip()
REST_TOKEN = os.environ.get("ES_REST_TOKEN", "").strip()
PUSH_URL   = os.environ.get("ES_PUSH_URL", "ws://webhook.entitysport.com:8087/connect")

_REST_RAW  = os.environ.get("ES_REST_BASE", "https://restapi.entitysport.com/v2").strip()
REST_BASE  = _REST_RAW.split("?")[0].rstrip("/")
REST_BASE  = re.sub(r"/matches/?$", "", REST_BASE).rstrip("/")

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8443"))

USERS_FILE = Path(os.environ.get("USERS_FILE", "users.json"))
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

STATUS_MAP = {1: "not_started", 2: "completed", 3: "started", 4: "cancelled"}

MAX_CACHE = 500
CACHE_FRESH_MS = 60_000
REST_REFRESH_S = 25
CONN_QUEUE_SIZE = 500

# ═══════════════════════════════════════════════════════════
#  USER DB
# ═══════════════════════════════════════════════════════════
USERS: dict[str, dict] = {}


def _load_users():
    global USERS
    try:
        if USERS_FILE.exists():
            USERS = json.loads(USERS_FILE.read_text(encoding="utf-8"))
            log.info("loaded %d users from %s", len(USERS), USERS_FILE)
        else:
            log.warning("users file not found: %s (all auth will fail)", USERS_FILE)
    except Exception as e:
        log.error("users load error: %r", e)


def check_user_key(key: str) -> bool:
    if not key or not key.startswith("usr_"):
        return False
    u = USERS.get(key)
    if not u:
        return False
    exp = u.get("expires")
    if exp and exp < time.time():
        return False
    return True


# ═══════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════
client = httpx.AsyncClient(timeout=15)

UP = {"connected": False, "msgs": 0, "last": 0.0}
LATEST_OBJ: dict[str, dict] = {}
FIX: dict[str, dict] = {}
_fix_pending: set[str] = set()

_rest = {
    "ts": 0.0, "rows": [], "last_error": "",
    "last_ok_ts": 0.0, "refreshing": False,
}
_rest_lock = asyncio.Lock()

# ═══════════════════════════════════════════════════════════
#  HELPERS (from your original main.py)
# ═══════════════════════════════════════════════════════════
def _i(v, d=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return d


def _decode(raw):
    if isinstance(raw, bytes):
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        raw = raw.decode("utf-8", errors="replace")
    return json.loads(raw)


_W_RE = re.compile(r"^\d*w$", re.I)


def fixture_row(mi):
    ta, tb = mi.get("teama") or {}, mi.get("teamb") or {}
    return {
        "key": str(mi.get("match_id")),
        "title": mi.get("title") or mi.get("short_title") or "",
        "format": mi.get("format_str") or "",
        "status": STATUS_MAP.get(_i(mi.get("status")), "not_started"),
        "start_at": mi.get("timestamp_start"),
        "teams": {"a": {"name": ta.get("name")}, "b": {"name": tb.get("name")}},
    }


def slim_comm(live):
    out = []
    for e in (live.get("commentaries") or []):
        if not isinstance(e, dict):
            continue
        if e.get("event") not in ("ball", "wicket"):
            continue
        eid = str(e.get("event_id") or "")
        if not eid:
            continue
        out.append({
            "id": eid, "ov": _i(e.get("over")), "b": _i(e.get("ball")),
            "x": _i(e.get("xball")), "run": _i(e.get("run")), "bat": _i(e.get("bat_run")),
            "wd": _i(e.get("wide_run")), "nb": _i(e.get("noball_run")),
            "by": _i(e.get("bye_run")), "lb": _i(e.get("legbye_run")),
            "four": bool(e.get("four")), "six": bool(e.get("six")),
            "w": e.get("event") == "wicket" or bool(_W_RE.match(str(e.get("score", "")).strip())),
            "dis": str(e.get("dismissal") or ""), "fh": bool(e.get("freehit")),
        })
    return out[-30:]


def slim_match(msg, now):
    r = msg.get("response") or {}
    mi = r.get("match_info") or {}
    live = r.get("live") if isinstance(r.get("live"), dict) else {}
    ta, tb = mi.get("teama") or {}, mi.get("teamb") or {}
    li = live.get("live_inning") if isinstance(live.get("live_inning"), dict) else {}
    ls = live.get("live_score") if isinstance(live.get("live_score"), dict) else {}
    inn_no = _i(live.get("live_inning_number"))

    bat_id = str(li.get("batting_team_id", ""))
    bat = "a" if bat_id and bat_id == str(ta.get("team_id")) else (
        "b" if bat_id and bat_id == str(tb.get("team_id")) else None)

    mx = _i(li.get("max_over"))
    if not mx:
        for inn in ((r.get("scorecard") or {}).get("innings") or []):
            if _i(inn.get("number")) == inn_no or not mx:
                mx = _i(inn.get("max_over")) or mx

    score = None
    if ls and ls.get("runs") is not None:
        score = {
            "runs": _i(ls.get("runs")), "wk": _i(ls.get("wickets")),
            "ov": str(ls.get("overs", 0)), "target": _i(ls.get("target")),
            "rrr": ls.get("required_runrate"),
        }

    lw = li.get("last_wicket") if isinstance(li.get("last_wicket"), dict) else None
    status = STATUS_MAP.get(_i(mi.get("status")), "started")
    result = mi.get("result") or (mi.get("status_note") if status == "completed" else "") or ""

    def team(t):
        return {"id": t.get("team_id"), "name": t.get("name"),
                "short": t.get("short_name"), "sc": t.get("scores_full") or ""}

    ts = msg.get("timestamp")
    out = {
        "t": "match",
        "id": str(r.get("match_id") or mi.get("match_id") or live.get("mid") or ""),
        "ts": ts if isinstance(ts, (int, float)) else None,
        "_server_ts": now * 1000,
        "status": status,
        "game": str(mi.get("game_state_str") or live.get("game_state_str") or ""),
        "fmt": mi.get("format_str") or "",
        "title": mi.get("title") or "",
        "result": result,
        "toss": (mi.get("toss") or {}).get("text") or "",
        "day": mi.get("day"), "session": mi.get("session"),
        "start": mi.get("timestamp_start"),
        "a": team(ta), "b": team(tb),
        "inn": inn_no, "bat": bat, "max": mx,
        "score": score,
        "lw": ({"ov": str(lw.get("overs_at_dismissal", "")),
                "dis": str(lw.get("dismissal") or ""),
                "sc": _i(lw.get("score_at_dismissal"))} if lw else None),
        "comm": slim_comm(live),
    }
    if isinstance(ts, (int, float)):
        lag = round(now * 1000 - ts)
        if 0 <= lag < 60000:
            out["_lag_ms"] = lag
    return out


def _fresh(obj):
    d = dict(obj)
    d["_server_ts"] = time.time() * 1000
    d.pop("_lag_ms", None)
    return d


# ═══════════════════════════════════════════════════════════
#  REST POLLING
# ═══════════════════════════════════════════════════════════
async def _fetch_status(status: int, per_page: int):
    try:
        r = await client.get(
            f"{REST_BASE}/matches/",
            params={"status": status, "per_page": per_page, "paged": 1, "token": REST_TOKEN},
        )
        if not r.is_success:
            return {"status": status, "error": f"HTTP {r.status_code}", "rows": []}
        resp = r.json().get("response")
        if not isinstance(resp, dict):
            return {"status": status, "error": "unexpected response type", "rows": []}
        items = resp.get("items") or []
        rows = [fixture_row(i) for i in items if isinstance(i, dict) and i.get("match_id")]
        return {"status": status, "error": "", "rows": rows}
    except Exception as e:
        return {"status": status, "error": f"exception: {e!r}", "rows": []}


async def _do_rest_fetch():
    if not REST_TOKEN:
        return
    async with _rest_lock:
        if _rest["refreshing"]:
            return
        _rest["refreshing"] = True
        try:
            parts = await asyncio.gather(
                _fetch_status(3, 50),
                _fetch_status(1, 50),
                _fetch_status(2, 50),
            )
            errors = [p["error"] for p in parts if p["error"]]
            merged: dict[str, dict] = {}
            for p in parts:
                for row in p["rows"]:
                    if row["key"] not in merged:
                        merged[row["key"]] = row
            rows = list(merged.values())
            now = time.time()
            _rest["rows"] = rows
            _rest["ts"] = now
            if not errors:
                _rest["last_error"] = ""
                _rest["last_ok_ts"] = now
            else:
                _rest["last_error"] = "; ".join(errors)
                log.warning("REST partial failure: %s", _rest["last_error"])
            log.info("REST refresh: %d fixtures", len(rows))
        finally:
            _rest["refreshing"] = False


async def _fixture_refresh_loop():
    if not REST_TOKEN:
        log.warning("ES_REST_TOKEN missing — fixture limited to push only")
        return
    try:
        await _do_rest_fetch()
    except Exception as e:
        log.warning("initial REST fetch failed: %r", e)
    while True:
        await asyncio.sleep(REST_REFRESH_S)
        try:
            await _do_rest_fetch()
        except Exception as e:
            log.warning("periodic REST fetch failed: %r", e)


# ═══════════════════════════════════════════════════════════
#  FIX (push fallback metadata)
# ═══════════════════════════════════════════════════════════
def _create_stub_fix(mid: str):
    if mid in FIX:
        return
    FIX[mid] = {
        "key": mid, "title": f"Match {mid}", "format": "",
        "status": "started", "start_at": None,
        "teams": {"a": {}, "b": {}},
    }
    if len(FIX) > MAX_CACHE:
        FIX.pop(next(iter(FIX)), None)


async def _fetch_fix_metadata(mid: str):
    try:
        if not REST_TOKEN:
            _create_stub_fix(mid)
            return
        r = await client.get(
            f"{REST_BASE}/matches/{mid}/info",
            params={"token": REST_TOKEN},
        )
        if not r.is_success:
            _create_stub_fix(mid)
            return
        resp = r.json().get("response") or {}
        mi = resp.get("match_info") if isinstance(resp.get("match_info"), dict) else resp
        if isinstance(mi, dict) and mi.get("match_id"):
            FIX[mid] = fixture_row(mi)
            log.info("FIX populated via REST: %s → %s", mid, FIX[mid].get("title"))
        else:
            _create_stub_fix(mid)
    except Exception as e:
        log.warning("FIX fetch %s failed: %r", mid, e)
        _create_stub_fix(mid)
    finally:
        _fix_pending.discard(mid)


def _ensure_fix(mid: str):
    if mid in FIX:
        return
    if REST_TOKEN:
        if mid not in _fix_pending:
            _fix_pending.add(mid)
            asyncio.create_task(_fetch_fix_metadata(mid))
        return
    _create_stub_fix(mid)


# ═══════════════════════════════════════════════════════════
#  PER-USER SESSIONS
# ═══════════════════════════════════════════════════════════
class UserSession:
    def __init__(self, user_key: str):
        self.user_key = user_key
        self.panels: set[WebSocket] = set()
        self.clicker: WebSocket | None = None
        self.watching: str = ""

    def to_panels(self, data: dict) -> list[str]:
        return [_dumps(data)]

    async def send_to_panels(self, data: dict):
        if not self.panels:
            return
        txt = _dumps(data)
        dead = []
        for p in list(self.panels):
            try:
                await p.send_text(txt)
            except Exception:
                dead.append(p)
        for p in dead:
            self.panels.discard(p)

    async def send_to_clicker(self, data: dict):
        if self.clicker is None:
            return
        try:
            await self.clicker.send_text(_dumps(data))
        except Exception:
            pass


_sessions: dict[str, UserSession] = {}


def _session(user_key: str) -> UserSession:
    s = _sessions.get(user_key)
    if s is None:
        s = UserSession(user_key)
        _sessions[user_key] = s
    return s


def _watching_users(mid: str) -> list[UserSession]:
    return [s for s in _sessions.values() if s.watching == mid]


# ═══════════════════════════════════════════════════════════
#  UPSTREAM — Entity Sports push
# ═══════════════════════════════════════════════════════════
async def _dispatch_event(mid: str, out: dict):
    """Push an event to every user watching this match."""
    for sess in _watching_users(mid):
        await sess.send_to_panels(out)
        # Clicker needs the same event to drive Playwright
        if sess.clicker is not None:
            await sess.send_to_clicker(out)


async def _dispatch_match(mid: str, out: dict):
    """Push a full match object to every user watching."""
    for sess in _watching_users(mid):
        await sess.send_to_panels(out)


def handle_upstream(raw):
    """Sync parse + spawn async dispatch. Runs on ES WS thread."""
    try:
        msg = _decode(raw)
    except Exception:
        return
    if not isinstance(msg, dict):
        return
    r = msg.get("response")
    if not isinstance(r, dict):
        return

    mi0 = r.get("match_info") if isinstance(r.get("match_info"), dict) else {}
    live0 = r.get("live") if isinstance(r.get("live"), dict) else {}
    mid = str(r.get("match_id") or mi0.get("match_id") or live0.get("mid") or "")
    if not mid:
        return

    # Update FIX cache
    if isinstance(mi0, dict) and mi0.get("match_id"):
        FIX[mid] = fixture_row(mi0)
        _fix_pending.discard(mid)
        if len(FIX) > MAX_CACHE:
            FIX.pop(next(iter(FIX)), None)
    else:
        _ensure_fix(mid)

    # ball_event
    if "ball_event" in r:
        d = r.get("data") if isinstance(r.get("data"), dict) else {}
        out = {
            "t": "ev", "id": mid,
            "ev": str(r.get("ball_event", "")),
            "code": str(r.get("code", "")),
            "over": d.get("over"), "ball": d.get("ball"),
            "inn": _i(d.get("inn_num")), "iid": d.get("iid"),
            "striker": d.get("striker_name"), "bowler": d.get("bowler_name"),
            "_server_ts": time.time() * 1000,
        }
        asyncio.create_task(_dispatch_event(mid, out))
        return

    # full match
    if "match_info" in r or "live" in r:
        out = slim_match(msg, time.time())
        if not out["id"] or out["id"] != mid:
            return
        LATEST_OBJ[mid] = out
        if len(LATEST_OBJ) > MAX_CACHE:
            LATEST_OBJ.pop(next(iter(LATEST_OBJ)), None)
        asyncio.create_task(_dispatch_match(mid, out))


async def upstream_loop():
    if not PUSH_TOKEN:
        log.warning("ES_PUSH_TOKEN missing — push disabled")
        return
    url = f"{PUSH_URL}?token={PUSH_TOKEN}"
    delay = 3
    while True:
        t0 = time.time()
        msgs0 = UP["msgs"]
        try:
            async with websockets.connect(url, ping_interval=None, max_size=None) as ws:
                UP["connected"] = True
                log.info("Entity push connected")
                async for raw in ws:
                    UP["msgs"] += 1
                    UP["last"] = time.time()
                    try:
                        handle_upstream(raw)
                    except Exception as e:
                        log.warning("push message skip: %r", e)
                    await asyncio.sleep(0)
                log.warning("Entity push server close (code=%s)", ws.close_code)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Entity push disconnected: %r", e)
        UP["connected"] = False
        if time.time() - t0 > 30 or UP["msgs"] > msgs0:
            delay = 3
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


# ═══════════════════════════════════════════════════════════
#  APP + LIFESPAN
# ═══════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_users()
    tasks = []
    if PUSH_TOKEN:
        tasks.append(asyncio.create_task(upstream_loop()))
    if REST_TOKEN:
        tasks.append(asyncio.create_task(_fixture_refresh_loop()))
    log.info("relay up | REST=%s | push=%s", bool(REST_TOKEN), bool(PUSH_TOKEN))
    yield
    for t in tasks:
        t.cancel()
        try:
            await t
        except BaseException:
            pass
    await client.aclose()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════
#  AUTH HELPER
# ═══════════════════════════════════════════════════════════
def _auth_header(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    key = authorization[7:].strip()
    if not check_user_key(key):
        raise HTTPException(401, "invalid or expired user key")
    return key


# ═══════════════════════════════════════════════════════════
#  WS /relay
# ═══════════════════════════════════════════════════════════
@app.websocket("/relay")
async def relay_ws(ws: WebSocket):
    token = ws.query_params.get("token", "")
    role = ws.query_params.get("role", "panel").lower()

    if not check_user_key(token):
        await ws.close(code=4401, reason="invalid user key")
        return

    await ws.accept()
    sess = _session(token)

    # ---------------- CLICKER ----------------
    if role == "clicker":
        old = sess.clicker
        sess.clicker = ws
        if old is not None and old is not ws:
            try:
                await old.close(code=4000, reason="replaced by new clicker")
            except Exception:
                pass
        log.info("clicker connected: user=%s...", token[:10])

        try:
            while True:
                raw = await ws.receive_text()
                if raw == "ping":
                    await ws.send_text("pong")
                    continue
                try:
                    m = json.loads(raw)
                except Exception:
                    continue
                t = m.get("type")
                if t == "watch":
                    sess.watching = (m.get("key") or "").strip()
                    log.info("watch: user=%s... key=%s", token[:10], sess.watching or "(none)")
                elif t == "ping":
                    await ws.send_text("pong")
        except WebSocketDisconnect:
            pass
        except Exception as e:
            log.warning("clicker err: %r", e)
        finally:
            if sess.clicker is ws:
                sess.clicker = None
            log.info("clicker disconnected: user=%s...", token[:10])
        return

    # ---------------- PANEL ----------------
    sess.panels.add(ws)
    log.info("panel connected: user=%s... (%d panels)", token[:10], len(sess.panels))
    try:
        while True:
            raw = await ws.receive_text()
            if raw == "ping":
                await ws.send_text("pong")
                continue
            if raw.startswith("watch:"):
                new_key = raw[6:].strip()
                sess.watching = new_key
                log.info("watch: user=%s... key=%s", token[:10], new_key or "(none)")
                # forward to clicker
                if sess.clicker:
                    try:
                        await sess.clicker.send_text(
                            _dumps({"type": "watch", "key": new_key})
                        )
                    except Exception:
                        pass
                # baseline reply
                if new_key:
                    obj = LATEST_OBJ.get(new_key)
                    if obj:
                        await ws.send_text(_dumps(_fresh(obj)))
                    else:
                        asyncio.create_task(_send_baseline(ws, sess, new_key))
                continue
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        sess.panels.discard(ws)
        log.info("panel disconnected: user=%s... (%d panels left)", token[:10], len(sess.panels))


async def _send_baseline(ws: WebSocket, sess: UserSession, mid: str):
    """REST fetch for a match the panel just started watching."""
    if not REST_TOKEN:
        return
    try:
        r = await client.get(
            f"{REST_BASE}/matches/{mid}/info",
            params={"token": REST_TOKEN},
        )
        if not r.is_success:
            return
        resp = r.json().get("response") or {}
        mi = resp.get("match_info") if isinstance(resp.get("match_info"), dict) else resp
        if not isinstance(mi, dict) or not mi.get("match_id"):
            return
        obj = slim_match({"response": {"match_id": mid, "match_info": mi}}, time.time())
        obj["id"] = str(mid)
        LATEST_OBJ[mid] = obj
        if sess.watching != mid:
            return
        try:
            await ws.send_text(_dumps(_fresh(obj)))
        except Exception:
            pass
    except Exception as e:
        log.warning("baseline fetch failed %s: %r", mid, e)


# ═══════════════════════════════════════════════════════════
#  REST API
# ═══════════════════════════════════════════════════════════
@app.get("/api/matches")
async def api_matches(authorization: str | None = Header(None)):
    _auth_header(authorization)
    merged: dict[str, dict] = {}

    for row in _rest["rows"]:
        merged[row["key"]] = dict(row)

    for k, row in FIX.items():
        is_stub = str(row.get("title", "")).startswith("Match ") and not row.get("format")
        if is_stub and k in merged:
            continue
        if k in merged:
            base = merged[k]
            for kk, vv in row.items():
                if vv not in (None, "", {}):
                    base[kk] = vv
        else:
            merged[k] = dict(row)

    order = {"started": 0, "not_started": 1, "completed": 2, "cancelled": 3}
    matches = sorted(merged.values(), key=lambda r: order.get(r.get("status"), 9))
    return {"status": "ok", "data": {"matches": matches}}


@app.get("/api/match/{match_key}")
async def api_match(match_key: str, authorization: str | None = Header(None)):
    _auth_header(authorization)

    # 1. cache
    obj = LATEST_OBJ.get(match_key)
    if obj:
        return {"status": "ok", "data": _fresh(obj)}

    # 2. REST
    if REST_TOKEN:
        try:
            r = await client.get(
                f"{REST_BASE}/matches/{match_key}/info",
                params={"token": REST_TOKEN},
            )
            if r.is_success:
                try:
                    resp = r.json().get("response") or {}
                except Exception:
                    resp = {}
                if not (isinstance(resp, dict) and resp.get("error")):
                    mi = resp.get("match_info") if isinstance(resp.get("match_info"), dict) else resp
                    if isinstance(mi, dict) and mi.get("match_id"):
                        obj = slim_match(
                            {"response": {"match_id": match_key, "match_info": mi}},
                            time.time(),
                        )
                        obj["id"] = str(match_key)
                        LATEST_OBJ[match_key] = obj
                        return {"status": "ok", "data": _fresh(obj)}
        except Exception as e:
            log.warning("baseline %s err: %r", match_key, e)

    # 3. FIX fallback
    fix = FIX.get(match_key)
    if fix:
        return {
            "status": "ok",
            "data": {
                "t": "match", "id": str(match_key),
                "_server_ts": time.time() * 1000,
                "status": fix.get("status", "started"),
                "fmt": fix.get("format", ""),
                "title": fix.get("title", ""),
                "a": {"name": fix.get("teams", {}).get("a", {}).get("name", "")},
                "b": {"name": fix.get("teams", {}).get("b", {}).get("name", "")},
                "score": None, "comm": [],
            },
        }

    # 4. nothing
    return JSONResponse(
        {"status": "error", "message": "match info not available"},
        status_code=404,
    )


@app.post("/api/click")
async def api_click(req: Request, authorization: str | None = Header(None)):
    user_key = _auth_header(authorization)
    try:
        body = await req.json()
    except Exception:
        body = {}

    sess = _sessions.get(user_key)
    if sess and sess.clicker:
        try:
            await sess.clicker.send_text(
                _dumps({"type": "manual_click", "payload": body})
            )
        except Exception:
            pass
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════
#  HEALTH
# ═══════════════════════════════════════════════════════════
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "push_connected": UP["connected"],
        "push_msgs": UP["msgs"],
        "push_last_age_s": round(time.time() - UP["last"], 1) if UP["last"] else None,
        "matches_cached": len(LATEST_OBJ),
        "fixtures_cached": len(FIX),
        "rest_token_set": bool(REST_TOKEN),
        "rest_cache_age_s": round(time.time() - _rest["ts"], 1) if _rest["ts"] else None,
        "rest_rows": len(_rest["rows"]),
        "sessions": len(_sessions),
        "users_online": len([s for s in _sessions.values() if s.panels or s.clicker]),
        "panels_total": sum(len(s.panels) for s in _sessions.values()),
        "clickers_total": sum(1 for s in _sessions.values() if s.clicker),
    }


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "relay_server:app",
        host=HOST,
        port=PORT,
        workers=1,
        access_log=False,
        log_level="warning",
        ws_per_message_deflate=False,
    )
