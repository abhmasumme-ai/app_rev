"""
main.py (desktop) — thin proxy to relay server.
- Serves index.html/css/js locally
- /ws → relay?role=panel (transparent pipe)
- /api/* → relay with Bearer user_key
"""
import asyncio
import json
import os
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response

BASE_DIR = Path(__file__).resolve().parent

# ── config.json from %APPDATA%\CricketPanel\ ────────────
_APPDATA = Path(os.environ.get("APPDATA") or Path.home())
_CFG = _APPDATA / "CricketPanel" / "config.json"


def _read_cfg():
    try:
        return json.loads(_CFG.read_text())
    except Exception:
        return {}


CFG = _read_cfg()
RELAY_HTTP = (CFG.get("relay_url") or "").replace("wss://", "https://").replace("ws://", "http://").rstrip("/")
RELAY_WS = (CFG.get("relay_url") or "").rstrip("/")
USER_KEY = CFG.get("user_key") or ""

app = FastAPI()
_http = httpx.AsyncClient(timeout=15)


@app.get("/api/matches")
async def api_matches():
    if not RELAY_HTTP or not USER_KEY:
        return JSONResponse({"status": "ok", "data": {"matches": []}}, status_code=200)
    try:
        r = await _http.get(f"{RELAY_HTTP}/api/matches",
                            headers={"Authorization": f"Bearer {USER_KEY}"})
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=200)


@app.get("/api/match/{key}")
async def api_match(key: str):
    if not RELAY_HTTP or not USER_KEY:
        return JSONResponse({"status": "error", "message": "not configured"}, status_code=503)
    try:
        r = await _http.get(f"{RELAY_HTTP}/api/match/{key}",
                            headers={"Authorization": f"Bearer {USER_KEY}"})
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=503)


@app.post("/api/click")
async def api_click(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    if RELAY_HTTP and USER_KEY:
        try:
            await _http.post(f"{RELAY_HTTP}/api/click",
                             headers={"Authorization": f"Bearer {USER_KEY}"},
                             json=body)
        except Exception:
            pass
    return {"status": "ok"}


@app.websocket("/ws")
async def ws_proxy(ws: WebSocket):
    """Transparent pipe panel ↔ relay."""
    await ws.accept()
    role = ws.query_params.get("role", "panel")
    if not RELAY_WS or not USER_KEY:
        await ws.close(code=1011, reason="relay not configured")
        return
    url = f"{RELAY_WS}?token={USER_KEY}&role={role}"

    try:
        async with websockets.connect(url, ping_interval=None, max_size=None) as up:
            async def c2s():
                try:
                    async for msg in ws.iter_text():
                        await up.send(msg)
                except Exception:
                    pass

            async def s2c():
                try:
                    async for msg in up:
                        try:
                            await ws.send_text(msg if isinstance(msg, str) else msg.decode())
                        except Exception:
                            return
                except Exception:
                    pass

            await asyncio.gather(c2s(), s2c(), return_exceptions=True)
    except Exception:
        pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


def _file(name: str):
    f = BASE_DIR / name
    if not f.exists():
        return JSONResponse({"detail": f"{name} not found"}, status_code=404)
    return FileResponse(f)


@app.get("/")
async def index():
    return _file("index.html")


@app.get("/index.css")
async def css():
    return _file("index.css")


@app.get("/script.js")
async def js():
    return _file("script.js")


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "relay_configured": bool(RELAY_HTTP and USER_KEY),
        "relay_url": RELAY_HTTP or None,
        "user_key_set": bool(USER_KEY),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=5000,
                access_log=False, log_level="warning")
