"""
ForexFlow SixFilter — Spot Forex Execution + Futures Volume Brain
Single file: main.py
Railway: uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
Requirements: fastapi, uvicorn, requests, pydantic
"""

import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# =============================
# CONFIG (Railway env vars)
# =============================
OANDA_API_KEY = os.getenv("OANDA_API_KEY", "")
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_ENV = os.getenv("OANDA_ENV", "practice")  # "practice" or "live"
AUTO_TRADE = os.getenv("AUTO_TRADE", "false").lower() == "true"
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "500"))
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "65"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

OANDA_BASE = (
    "https://api-fxpractice.oanda.com"
    if OANDA_ENV == "practice"
    else "https://api-fxtrade.oanda.com"
)

# Pair -> CME futures contract (the real-volume brain)
PAIR_MAP = {
    "EUR_USD": {"future": "6E", "pip": 0.0001, "atr_mult": 1.0},
    "GBP_USD": {"future": "6B", "pip": 0.0001, "atr_mult": 1.0},
    "USD_JPY": {"future": "6J", "pip": 0.01,   "atr_mult": 1.0},
}

# Session filter (UTC hours): London + NY only
SESSIONS = [(7, 11), (12, 16)]  # 7-11 London, 12-16 NY overlap+PM

app = FastAPI(title="ForexFlow SixFilter", version="1.0.0")

# =============================
# STATE
# =============================
STATE: Dict[str, Any] = {
    "volume": {},          # futures volume brain: {"6E": {...}}
    "daily": {"date": "", "trades": 0, "pnl": 0.0},
    "open_positions": {},
    "log": [],
}


def log_event(msg: str):
    STATE["log"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "msg": msg,
    })
    STATE["log"] = STATE["log"][-200:]
    print(msg)


def telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10,
        )
    except Exception as e:
        log_event(f"Telegram failed: {e}")


# =============================
# OANDA CLIENT
# =============================
def oanda_headers():
    return {
        "Authorization": f"Bearer {OANDA_API_KEY}",
        "Content-Type": "application/json",
    }


def oanda_get(path: str):
    r = requests.get(f"{OANDA_BASE}{path}", headers=oanda_headers(), timeout=15)
    r.raise_for_status()
    return r.json()


def oanda_post(path: str, payload: dict):
    r = requests.post(
        f"{OANDA_BASE}{path}",
        headers=oanda_headers(),
        json=payload,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def get_balance() -> float:
    acct = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/summary")
    return float(acct["account"]["balance"])


def get_candles(pair: str, count: int = 60, gran: str = "M15") -> List[dict]:
    data = oanda_get(
        f"/v3/instruments/{pair}/candles?count={count}&granularity={gran}&price=M"
    )
    out = []
    for c in data.get("candles", []):
        if not c.get("complete", True):
            continue
        out.append({
            "time": c["time"],
            "o": float(c["mid"]["o"]),
            "h": float(c["mid"]["h"]),
            "l": float(c["mid"]["l"]),
            "c": float(c["mid"]["c"]),
        })
    return out


def place_market_order(pair: str, units: int, sl: float, tp: float):
    payload = {
        "order": {
            "instrument": pair,
            "units": str(units),
            "type": "MARKET",
            "positionFill": "DEFAULT",
            "stopLossOnFill": {"price": f"{sl:.5f}"},
            "takeProfitOnFill": {"price": f"{tp:.5f}"},
        }
    }
    return oanda_post(f"/v3/accounts/{OANDA_ACCOUNT_ID}/orders", payload)


# =============================
# INDICATORS
# =============================
def ema(values: List[float], period: int) -> float:
    if len(values) < period:
        return values[-1]
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(-period, 0):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def atr(candles: List[dict], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.001
    trs = []
    for i in range(-period, 0):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / period


def vwap(candles: List[dict]) -> float:
    # Session VWAP proxy on M15 candles (typical price, equal weight fallback
    # when futures volume not yet loaded for this session window)
    num = den = 0.0
    for c in candles[-32:]:
        tp = (c["h"] + c["l"] + c["c"]) / 3
        num += tp
        den += 1
    return num / den if den else candles[-1]["c"]


# =============================
# SIX FILTER ENGINE (forex-adapted)
# =============================
def six_filter_analyze(pair: str) -> Dict[str, Any]:
    cfg = PAIR_MAP[pair]
    candles = get_candles(pair, count=60, gran="M15")
    if len(candles) < 30:
        return {"proceed": False, "reason": "not enough candles"}

    closes = [c["c"] for c in candles]
    price = closes[-1]
    a = atr(candles)
    r = rsi(closes)
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    vw = vwap(candles)

    filters = {}

    # F1 — VWAP deviation (LMSR-style mean reversion/trend gate)
    dev = (price - vw) / a if a else 0
    filters["vwap_dev"] = abs(dev) < 2.5  # skip over-extended

    # F2 — Trend alignment (institutional flow direction)
    if e20 > e50 and price > e20:
        direction = "LONG"
    elif e20 < e50 and price < e20:
        direction = "SHORT"
    else:
        direction = "NONE"
    filters["trend"] = direction != "NONE"

    # F3 — Futures volume confirmation (the "real volume" brain)
    fv = STATE["volume"].get(cfg["future"], {})
    vol_ok = True
    vol_note = "no futures volume loaded — neutral"
    if fv:
        net = fv.get("net_change_pct", 0)
        if direction == "LONG" and net < -1.5:
            vol_ok = False
            vol_note = f"{cfg['future']} volume net {net}% bearish — blocks LONG"
        elif direction == "SHORT" and net > 1.5:
            vol_ok = False
            vol_note = f"{cfg['future']} volume net {net}% bullish — blocks SHORT"
        else:
            vol_note = f"{cfg['future']} volume confirms ({net}%)"
    filters["futures_volume"] = vol_ok

    # F4 — RSI divergence / exhaustion guard
    filters["rsi_ok"] = not (
        (direction == "LONG" and r > 75) or (direction == "SHORT" and r < 25)
    )

    # F5 — Session filter (Bayesian time-of-day edge)
    hour = datetime.now(timezone.utc).hour
    in_session = any(s <= hour < e for s, e in SESSIONS)
    filters["session"] = in_session

    # F6 — EV gap: 2:1 RR minimum with ATR-scaled stops
    sl_dist = 1.5 * a
    tp_dist = 3.0 * a
    rr = tp_dist / sl_dist if sl_dist else 0
    filters["ev_gap"] = rr >= 2.0

    passed = sum(1 for v in filters.values() if v)
    confidence = round(passed / 6 * 100, 1)

    proceed = (
        all(filters.values())
        and direction != "NONE"
        and confidence >= MIN_CONFIDENCE
    )

    if direction == "LONG":
        entry, sl, tp = price, price - sl_dist, price + tp_dist
    else:
        entry, sl, tp = price, price + sl_dist, price - tp_dist

    return {
        "pair": pair,
        "proceed": proceed,
        "direction": direction,
        "confidence": confidence,
        "filters": filters,
        "volume_note": vol_note,
        "price": round(price, 5),
        "entry": round(entry, 5),
        "stop_loss": round(sl, 5),
        "take_profit": round(tp, 5),
        "atr": round(a, 5),
        "rsi": round(r, 1),
        "rr": round(rr, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================
# SIZING (Kelly-lite)
# =============================
def calc_units(pair: str, entry: float, sl: float, balance: float) -> int:
    cfg = PAIR_MAP[pair]
    risk_dollars = balance * (RISK_PER_TRADE_PCT / 100)
    sl_pips = abs(entry - sl) / cfg["pip"]
    if sl_pips <= 0:
        return 0
    pip_value = 0.10 if "JPY" not in pair else 6.5  # per 1k units approx
    units = int(risk_dollars / (sl_pips * pip_value) * 1000)
    return max(1000, min(units, 100000))


def reset_daily():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if STATE["daily"]["date"] != today:
        STATE["daily"] = {"date": today, "trades": 0, "pnl": 0.0}


# =============================
# MODELS
# =============================
class VolumeUpdate(BaseModel):
    future: str               # "6E", "6B", "6J"
    session_volume: Optional[int] = None
    prev_volume: Optional[int] = None
    net_change_pct: Optional[float] = None
    open_interest: Optional[int] = None
    note: Optional[str] = None


class ManualTrade(BaseModel):
    pair: str
    direction: str            # LONG or SHORT


# =============================
# ENDPOINTS
# =============================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "oanda": {
            "env": OANDA_ENV,
            "key_set": bool(OANDA_API_KEY),
            "account_set": bool(OANDA_ACCOUNT_ID),
        },
        "auto_trade": AUTO_TRADE,
        "pairs": list(PAIR_MAP.keys()),
        "volume_loaded": list(STATE["volume"].keys()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/balance")
def balance():
    try:
        b = get_balance()
        return {"balance": b, "env": OANDA_ENV, "status": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/volume-update")
def volume_update(v: VolumeUpdate):
    STATE["volume"][v.future] = v.dict()
    STATE["volume"][v.future]["updated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )
    log_event(f"Volume updated {v.future}: net {v.net_change_pct}%")
    return {"saved": True, "future": v.future}


@app.get("/volume-status")
def volume_status():
    return STATE["volume"]


@app.get("/analyze/{pair}")
def analyze(pair: str):
    pair = pair.upper()
    if pair not in PAIR_MAP:
        raise HTTPException(status_code=404, detail=f"Unknown pair {pair}")
    try:
        return six_filter_analyze(pair)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scan")
def scan():
    reset_daily()
    results = []
    for pair in PAIR_MAP:
        try:
            results.append(six_filter_analyze(pair))
        except Exception as e:
            results.append({"pair": pair, "proceed": False, "reason": str(e)})
    return {
        "results": results,
        "daily": STATE["daily"],
        "auto_trade": AUTO_TRADE,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def execute_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    reset_daily()
    if STATE["daily"]["trades"] >= MAX_TRADES_PER_DAY:
        return {"executed": False, "reason": "max trades per day reached"}
    if STATE["daily"]["pnl"] <= -abs(DAILY_LOSS_LIMIT):
        return {"executed": False, "reason": "daily loss limit hit"}
    if not sig.get("proceed"):
        return {"executed": False, "reason": "signal not qualified"}

    balance_amt = get_balance()
    units = calc_units(sig["pair"], sig["entry"], sig["stop_loss"], balance_amt)
    if sig["direction"] == "SHORT":
        units = -units

    resp = place_market_order(
        sig["pair"], units, sig["stop_loss"], sig["take_profit"]
    )
    STATE["daily"]["trades"] += 1
    msg = (
        f"TRADE {sig['direction']} {sig['pair']} units={units} "
        f"entry~{sig['entry']} sl={sig['stop_loss']} tp={sig['take_profit']} "
        f"conf={sig['confidence']}%"
    )
    log_event(msg)
    telegram(msg)
    return {"executed": True, "oanda_response": resp, "signal": sig}


@app.post("/trade")
def trade(t: ManualTrade):
    pair = t.pair.upper()
    if pair not in PAIR_MAP:
        raise HTTPException(status_code=404, detail=f"Unknown pair {pair}")
    sig = six_filter_analyze(pair)
    if t.direction.upper() != sig.get("direction"):
        sig["direction"] = t.direction.upper()
        sig["proceed"] = True  # manual override
    try:
        return execute_signal(sig)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auto-scan")
def auto_scan():
    """One-shot: scan all pairs and execute qualified signals (if AUTO_TRADE)."""
    results = scan()["results"]
    executions = []
    for sig in results:
        if sig.get("proceed") and AUTO_TRADE:
            try:
                executions.append(execute_signal(sig))
            except Exception as e:
                executions.append({"executed": False, "reason": str(e)})
    return {"results": results, "executions": executions}


@app.get("/logs")
def logs():
    return STATE["log"]


@app.get("/dashboard")
def dashboard():
    return {
        "daily": STATE["daily"],
        "volume": STATE["volume"],
        "recent_logs": STATE["log"][-20:],
        "config": {
            "risk_pct": RISK_PER_TRADE_PCT,
            "max_trades": MAX_TRADES_PER_DAY,
            "daily_loss_limit": DAILY_LOSS_LIMIT,
            "min_confidence": MIN_CONFIDENCE,
            "auto_trade": AUTO_TRADE,
            "env": OANDA_ENV,
        },
    }


# =============================
# BACKGROUND SCANNER (optional auto-loop)
# =============================
def scanner_loop():
    interval = int(os.getenv("SCAN_INTERVAL_SEC", "900"))  # 15 min default
    while True:
        time.sleep(interval)
        try:
            if AUTO_TRADE:
                auto_scan()
            else:
                scan()
        except Exception as e:
            log_event(f"Scanner error: {e}")


@app.on_event("startup")
def startup():
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    log_event("ForexFlow SixFilter started")
