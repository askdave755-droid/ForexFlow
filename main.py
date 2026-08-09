"""
ForexFlow EightFilter — Spot Forex + Futures Volume + COT + FRED Macro
Single file: main.py
Railway: uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
Requirements: fastapi, uvicorn, requests, pydantic
v2.1: Adds Filter 8 (FRED macro dollar trend), fixes JPY volume inversion
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
OANDA_ENV = os.getenv("OANDA_ENV", "practice")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
AUTO_TRADE = os.getenv("AUTO_TRADE", "false").lower() == "true"
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "500"))
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "65"))
MACRO_BLOCK_PCT = float(os.getenv("MACRO_BLOCK_PCT", "0.8"))  # 5-day % move that blocks counter-trend
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

OANDA_BASE = (
    "https://api-fxpractice.oanda.com"
    if OANDA_ENV == "practice"
    else "https://api-fxtrade.oanda.com"
)

# fred_series: official daily FX rate from St. Louis Fed
# fred_up_means: what a rising series means for the SPOT pair direction
PAIR_MAP = {
    "EUR_USD": {
        "future": "6E", "pip": 0.0001,
        "vol_invert": False, "cot_invert": False,
        "fred_series": "DEXUSEU",       # USD per EUR
        "fred_up_means": "LONG",        # rising = EUR stronger = pair up
    },
    "GBP_USD": {
        "future": "6B", "pip": 0.0001,
        "vol_invert": False, "cot_invert": False,
        "fred_series": "DEXUSUK",       # USD per GBP
        "fred_up_means": "LONG",
    },
    "USD_JPY": {
        "future": "6J", "pip": 0.01,
        "vol_invert": True, "cot_invert": True,
        "fred_series": "DEXJPUS",       # JPY per USD
        "fred_up_means": "LONG",        # rising = USD stronger = pair up
    },
}

SESSIONS = [(7, 11), (12, 16)]  # UTC: London + NY

app = FastAPI(title="ForexFlow EightFilter", version="2.1.0")

# =============================
# STATE
# =============================
STATE: Dict[str, Any] = {
    "volume": {},
    "cot": {},
    "fred": {},            # {"EUR_USD": {"change_5d_pct": ..., ...}}
    "daily": {"date": "", "trades": 0, "pnl": 0.0},
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
# FRED CLIENT (St. Louis Fed)
# =============================
def fred_series_latest(series_id: str, n: int = 8) -> List[dict]:
    """Last n daily observations, skipping weekends/holiday gaps."""
    if not FRED_API_KEY:
        return []
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&file_type=json&sort_order=desc&limit={n}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    obs = []
    for o in r.json().get("observations", []):
        if o.get("value") not in (None, ".", ""):
            obs.append({"date": o["date"], "value": float(o["value"])})
    return obs  # newest first


def refresh_fred():
    """Pull latest FRED data for all pairs, compute 5-day % change."""
    for pair, cfg in PAIR_MAP.items():
        try:
            obs = fred_series_latest(cfg["fred_series"], n=8)
            if len(obs) < 6:
                continue
            latest = obs[0]
            five_back = obs[5]
            chg = (latest["value"] - five_back["value"]) / five_back["value"] * 100
            STATE["fred"][pair] = {
                "series": cfg["fred_series"],
                "latest_date": latest["date"],
                "latest_value": latest["value"],
                "change_5d_pct": round(chg, 3),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            log_event(f"FRED {pair} ({cfg['fred_series']}): 5d {chg:+.2f}%")
        except Exception as e:
            log_event(f"FRED refresh failed {pair}: {e}")


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
    num = den = 0.0
    for c in candles[-32:]:
        tp = (c["h"] + c["l"] + c["c"]) / 3
        num += tp
        den += 1
    return num / den if den else candles[-1]["c"]


# =============================
# EIGHT FILTER ENGINE
# =============================
def eight_filter_analyze(pair: str) -> Dict[str, Any]:
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

    # F1 — VWAP deviation gate
    dev = (price - vw) / a if a else 0
    filters["vwap_dev"] = abs(dev) < 2.5

    # F2 — Trend alignment
    if e20 > e50 and price > e20:
        direction = "LONG"
    elif e20 < e50 and price < e20:
        direction = "SHORT"
    else:
        direction = "NONE"
    filters["trend"] = direction != "NONE"

    # F3 — Futures volume (with JPY inversion fix)
    fv = STATE["volume"].get(cfg["future"], {})
    vol_ok = True
    vol_note = "no futures volume loaded — neutral"
    if fv:
        net = fv.get("net_change_pct", 0)
        if cfg["vol_invert"]:
            net = -net  # yen strength = USD_JPY weakness
        if direction == "LONG" and net < -1.5:
            vol_ok = False
            vol_note = f"{cfg['future']} volume bearish ({net}%) — blocks LONG"
        elif direction == "SHORT" and net > 1.5:
            vol_ok = False
            vol_note = f"{cfg['future']} volume bullish ({net}%) — blocks SHORT"
        else:
            vol_note = f"{cfg['future']} volume confirms ({net}%)"
    filters["futures_volume"] = vol_ok

    # F4 — RSI exhaustion guard
    filters["rsi_ok"] = not (
        (direction == "LONG" and r > 75) or (direction == "SHORT" and r < 25)
    )

    # F5 — Session filter
    hour = datetime.now(timezone.utc).hour
    in_session = any(s <= hour < e for s, e in SESSIONS)
    filters["session"] = in_session

    # F6 — EV gap (2:1 RR with ATR stops)
    sl_dist = 1.5 * a
    tp_dist = 3.0 * a
    rr = tp_dist / sl_dist if sl_dist else 0
    filters["ev_gap"] = rr >= 2.0

    # F7 — COT positioning (with JPY inversion)
    cot = STATE["cot"].get(cfg["future"], {})
    cot_ok = True
    cot_note = "no COT loaded — neutral"
    if cot:
        net = cot.get("net_noncommercial", 0)
        if cfg["cot_invert"]:
            net = -net
        if direction == "LONG" and net < -20000:
            cot_ok = False
            cot_note = f"COT smart money heavily short ({net}) — blocks LONG"
        elif direction == "SHORT" and net > 20000:
            cot_ok = False
            cot_note = f"COT smart money heavily long ({net}) — blocks SHORT"
        else:
            cot_note = f"COT aligned or neutral (net {net})"
        cot_note += f" [report {cot.get('report_date', '?')}]"
    filters["cot_positioning"] = cot_ok

    # F8 — FRED macro dollar trend
    fd = STATE["fred"].get(pair, {})
    fred_ok = True
    fred_note = "no FRED data — neutral"
    if fd:
        chg = fd.get("change_5d_pct", 0)
        # rising series means "fred_up_means" direction
        macro_dir = cfg["fred_up_means"] if chg > 0 else (
            "SHORT" if cfg["fred_up_means"] == "LONG" else "LONG"
        )
        if direction != "NONE" and direction != macro_dir and abs(chg) > MACRO_BLOCK_PCT:
            fred_ok = False
            fred_note = (
                f"FRED {fd.get('series')} 5d {chg:+.2f}% macro={macro_dir} "
                f"— blocks {direction}"
            )
        else:
            fred_note = f"FRED 5d {chg:+.2f}% ({fd.get('latest_date')}) — aligned/neutral"
    filters["fred_macro"] = fred_ok

    passed = sum(1 for v in filters.values() if v)
    confidence = round(passed / 8 * 100, 1)

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
        "cot_note": cot_note,
        "fred_note": fred_note,
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
# SIZING
# =============================
def calc_units(pair: str, entry: float, sl: float, balance: float) -> int:
    cfg = PAIR_MAP[pair]
    risk_dollars = balance * (RISK_PER_TRADE_PCT / 100)
    sl_pips = abs(entry - sl) / cfg["pip"]
    if sl_pips <= 0:
        return 0
    pip_value = 0.10 if "JPY" not in pair else 6.5
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
    future: str
    session_volume: Optional[int] = None
    prev_volume: Optional[int] = None
    net_change_pct: Optional[float] = None
    open_interest: Optional[int] = None
    note: Optional[str] = None


class CotUpdate(BaseModel):
    future: str
    report_date: str
    noncomm_long: int
    noncomm_short: int
    open_interest: Optional[int] = None


class ManualTrade(BaseModel):
    pair: str
    direction: str


# =============================
# ENDPOINTS
# =============================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "2.1.0",
        "engine": "eight-filter",
        "oanda": {
            "env": OANDA_ENV,
            "key_set": bool(OANDA_API_KEY),
            "account_set": bool(OANDA_ACCOUNT_ID),
        },
        "fred_key_set": bool(FRED_API_KEY),
        "auto_trade": AUTO_TRADE,
        "pairs": list(PAIR_MAP.keys()),
        "volume_loaded": list(STATE["volume"].keys()),
        "cot_loaded": list(STATE["cot"].keys()),
        "fred_loaded": list(STATE["fred"].keys()),
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


@app.post("/cot-update")
def cot_update(c: CotUpdate):
    net = c.noncomm_long - c.noncomm_short
    STATE["cot"][c.future] = {
        "report_date": c.report_date,
        "noncomm_long": c.noncomm_long,
        "noncomm_short": c.noncomm_short,
        "net_noncommercial": net,
        "open_interest": c.open_interest,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    log_event(f"COT updated {c.future}: net {net} (report {c.report_date})")
    return {"saved": True, "future": c.future, "net_noncommercial": net}


@app.get("/cot-status")
def cot_status():
    return STATE["cot"]


@app.post("/fred-refresh")
def fred_refresh():
    if not FRED_API_KEY:
        raise HTTPException(status_code=400, detail="FRED_API_KEY not set")
    refresh_fred()
    return {"fred": STATE["fred"]}


@app.get("/fred-status")
def fred_status():
    return STATE["fred"]


@app.get("/analyze/{pair}")
def analyze(pair: str):
    pair = pair.upper()
    if pair not in PAIR_MAP:
        raise HTTPException(status_code=404, detail=f"Unknown pair {pair}")
    try:
        return eight_filter_analyze(pair)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scan")
def scan():
    reset_daily()
    results = []
    for pair in PAIR_MAP:
        try:
            results.append(eight_filter_analyze(pair))
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
    sig = eight_filter_analyze(pair)
    if t.direction.upper() != sig.get("direction"):
        sig["direction"] = t.direction.upper()
        sig["proceed"] = True
    try:
        return execute_signal(sig)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auto-scan")
def auto_scan():
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
        "cot": STATE["cot"],
        "fred": STATE["fred"],
        "recent_logs": STATE["log"][-20:],
        "config": {
            "risk_pct": RISK_PER_TRADE_PCT,
            "max_trades": MAX_TRADES_PER_DAY,
            "daily_loss_limit": DAILY_LOSS_LIMIT,
            "min_confidence": MIN_CONFIDENCE,
            "macro_block_pct": MACRO_BLOCK_PCT,
            "auto_trade": AUTO_TRADE,
            "env": OANDA_ENV,
        },
    }


# =============================
# BACKGROUND SCANNER
# =============================
def scanner_loop():
    interval = int(os.getenv("SCAN_INTERVAL_SEC", "900"))
    fred_counter = 0
    while True:
        time.sleep(interval)
        try:
            # Refresh FRED macro data roughly every 8 cycles (~2h at 15min)
            fred_counter += 1
            if FRED_API_KEY and fred_counter >= 8:
                fred_counter = 0
                refresh_fred()
            if AUTO_TRADE:
                auto_scan()
            else:
                scan()
        except Exception as e:
            log_event(f"Scanner error: {e}")


@app.on_event("startup")
def startup():
    if FRED_API_KEY:
        try:
            refresh_fred()
        except Exception as e:
            log_event(f"FRED startup refresh failed: {e}")
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    log_event("ForexFlow EightFilter started")
