"""
ForexFlow EightFilter v2.4.0 — Institutional-style forex signal engine
Full major-pairs expansion: 7 CME-futures-backed pairs (where banks trade)
New in v2.4.0:
  - 7 pairs: EUR_USD, GBP_USD, USD_JPY, AUD_USD, NZD_USD, USD_CAD, USD_CHF
  - Session chop skip: first CHOP_SKIP_MIN of each session ignored (London 07:30-11:00, NY 12:30-16:00 UTC)
  - Dynamic pip-value sizing (works for USD-base and USD-quote pairs)
  - Volume/COT accepted for 6E, 6B, 6J, 6A, 6N, 6C, 6S
Eight filters:
  F1 VWAP deviation | F2 EMA20/50 trend | F3 CME futures volume | F4 RSI exhaustion
  F5 Session (chop-skipped) | F6 EV gap (2:1 RR) | F7 COT positioning | F8 FRED macro
Risk: 1% per trade, max 3/day, $500 daily loss LOCKDOWN, margin-aware sizing,
      FIFO-safe, one trade per pair per day.
"""
import os, time, math, threading, logging
from datetime import datetime, timezone, date
import requests
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("forexflow")

# ---------------- CONFIG ----------------
OANDA_API_KEY   = os.getenv("OANDA_API_KEY", "")
OANDA_ACCOUNT   = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_ENV       = os.getenv("OANDA_ENV", "practice")
FRED_API_KEY    = os.getenv("FRED_API_KEY", "")
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

AUTO_TRADE        = os.getenv("AUTO_TRADE", "false").lower() == "true"
RISK_PCT          = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
DAILY_LOSS_LIMIT  = float(os.getenv("DAILY_LOSS_LIMIT", "500"))
MAX_TRADES_DAY    = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
MAX_UNITS         = int(os.getenv("MAX_UNITS", "2000000"))
MIN_CONFIDENCE    = float(os.getenv("MIN_CONFIDENCE", "65"))
MACRO_BLOCK_PCT   = float(os.getenv("MACRO_BLOCK_PCT", "0.8"))
SCAN_INTERVAL     = int(os.getenv("SCAN_INTERVAL_SEC", "900"))
LEVERAGE          = float(os.getenv("LEVERAGE", "30"))
MARGIN_USAGE_PCT  = float(os.getenv("MARGIN_USAGE_PCT", "50"))
CHOP_SKIP_MIN     = int(os.getenv("CHOP_SKIP_MIN", "30"))  # skip first N min of each session

BASE = "https://api-fxpractice.oanda.com" if OANDA_ENV == "practice" else "https://api-fxtrade.oanda.com"
HEADERS = {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}

# Sessions in UTC hours: (name, open, close) — chop skip applied to open
SESSIONS = [("London", 7.0, 11.0), ("NewYork", 12.0, 16.0)]

# ---------------- PAIR MAP ----------------
# vol_invert / cot_invert: True when the CME contract is quoted foreign/USD
#   and the pair is USD/foreign (6J, 6C, 6S) -> strength in contract = pair down.
# fred_series + fred_up_means: expected pair direction when series rises.
PAIR_MAP = {
    "EUR_USD": {"future": "6E", "pip": 0.0001, "dec": 5, "vol_invert": False, "cot_invert": False, "fred_series": "DEXUSEU", "fred_up_means": "LONG"},
    "GBP_USD": {"future": "6B", "pip": 0.0001, "dec": 5, "vol_invert": False, "cot_invert": False, "fred_series": "DEXUSUK", "fred_up_means": "LONG"},
    "AUD_USD": {"future": "6A", "pip": 0.0001, "dec": 5, "vol_invert": False, "cot_invert": False, "fred_series": "DEXUSAL", "fred_up_means": "LONG"},
    "NZD_USD": {"future": "6N", "pip": 0.0001, "dec": 5, "vol_invert": False, "cot_invert": False, "fred_series": "DEXUSNZ", "fred_up_means": "LONG"},
    "USD_JPY": {"future": "6J", "pip": 0.01,   "dec": 3, "vol_invert": True,  "cot_invert": True,  "fred_series": "DEXJPUS", "fred_up_means": "LONG"},
    "USD_CAD": {"future": "6C", "pip": 0.0001, "dec": 5, "vol_invert": True,  "cot_invert": True,  "fred_series": "DEXCAUS", "fred_up_means": "LONG"},
    "USD_CHF": {"future": "6S", "pip": 0.0001, "dec": 5, "vol_invert": True,  "cot_invert": True,  "fred_series": "DEXSZUS", "fred_up_means": "LONG"},
}
PAIRS = list(PAIR_MAP.keys())

# ---------------- STATE (in-memory; wipes on redeploy) ----------------
LOGS = []
VOLUME = {}   # future -> {"net_pct": float, "ts": str}
COT = {}      # future -> {"net": float, "ts": str}
FRED = {}     # pair -> 5d pct change
DAILY = {"date": None, "trades": 0, "start_balance": None, "traded_pairs": [], "lockdown": False}

def logmsg(msg):
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg}
    LOGS.append(entry)
    if len(LOGS) > 500:
        del LOGS[:-500]
    log.info(msg)

def tg(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT, "text": msg}, timeout=10)
    except Exception as e:
        logmsg(f"Telegram error: {e}")

# ---------------- OANDA HELPERS ----------------
def oanda_get(path):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()

def get_account():
    a = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/summary")["account"]
    return {"balance": float(a["balance"]), "nav": float(a["NAV"]),
            "margin_available": float(a.get("marginAvailable", a["balance"])),
            "unrealized_pl": float(a.get("unrealizedPL", 0.0))}

def get_candles(pair, count=60, gran="M15"):
    d = oanda_get(f"/v3/instruments/{pair}/candles?count={count}&granularity={gran}&price=M")
    out = []
    for c in d["candles"]:
        if c["complete"]:
            out.append({"t": c["time"], "o": float(c["mid"]["o"]), "h": float(c["mid"]["h"]),
                        "l": float(c["mid"]["l"]), "c": float(c["mid"]["c"]), "v": float(c.get("volume", 0))})
    return out

def get_price(pair):
    d = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/pricing?instruments={pair}")
    p = d["prices"][0]
    bid = float(p["bids"][0]["price"]); ask = float(p["asks"][0]["price"])
    return (bid + ask) / 2.0

def get_open_position_pairs():
    d = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/openPositions")
    return {p["instrument"] for p in d.get("positions", [])}

def fmt_price(pair, price):
    return f"{price:.{PAIR_MAP[pair]['dec']}f}"

# ---------------- DAILY / RISK ----------------
def reset_daily():
    today = date.today().isoformat()
    if DAILY["date"] != today:
        try:
            bal = get_account()["balance"]
        except Exception:
            bal = None
        DAILY.update({"date": today, "trades": 0, "start_balance": bal,
                      "traded_pairs": [], "lockdown": False})
        logmsg(f"Daily reset {today}: start_balance={bal}")

def daily_pnl():
    if DAILY["start_balance"] is None:
        return 0.0
    try:
        return get_account()["balance"] - DAILY["start_balance"]
    except Exception:
        return 0.0

def pip_value_per_unit(pair, price):
    """USD pip value per unit. Quote=USD -> pip. Base=USD -> pip/price."""
    pip = PAIR_MAP[pair]["pip"]
    if pair.endswith("_USD"):
        return pip
    return pip / price

def calc_units(pair, price, stop_dist, balance, margin_avail):
    risk_amt = balance * (RISK_PCT / 100.0)
    pv = pip_value_per_unit(pair, price)
    risk_units = int(risk_amt / (stop_dist * pv)) if pv > 0 else 0
    notional_per_unit = price if pair.endswith("_USD") else 1.0
    margin_units = int(margin_avail * LEVERAGE * (MARGIN_USAGE_PCT / 100.0) / notional_per_unit)
    units = min(risk_units, margin_units, MAX_UNITS)
    capped = "risk" if units == risk_units and units < MAX_UNITS else ("margin" if units == margin_units else "max")
    return {"units": max(units, 0), "risk_units": risk_units, "margin_units": margin_units, "capped_by": capped}

def place_market_order(pair, direction, units, price, sl, tp):
    u = units if direction == "LONG" else -units
    order = {"order": {"type": "MARKET", "instrument": pair, "units": str(u),
                       "stopLossOnFill": {"price": fmt_price(pair, sl)},
                       "takeProfitOnFill": {"price": fmt_price(pair, tp)}}}
    try:
        r = requests.post(f"{BASE}/v3/accounts/{OANDA_ACCOUNT}/orders",
                          headers=HEADERS, json=order, timeout=15)
        d = r.json()
        if r.status_code in (200, 201) and "orderFillTransaction" in d:
            fill = float(d["orderFillTransaction"]["price"])
            return {"ok": True, "fill": fill}
        reason = (d.get("orderCancelTransaction", {}).get("cancelReason")
                  or d.get("orderRejectTransaction", {}).get("rejectReason")
                  or d.get("errorMessage") or f"HTTP {r.status_code}")
        return {"ok": False, "reason": reason}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

# ---------------- INDICATORS ----------------
def ema(vals, n):
    k = 2 / (n + 1); e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes, n=14):
    if len(closes) < n + 1:
        return 50.0
    gains, losses = [], []
    for i in range(-n, 0):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    ag = sum(gains) / n; al = sum(losses) / n
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)

def atr(candles, n=14):
    if len(candles) < n + 1:
        return 0.0
    trs = []
    for i in range(-n, 0):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / n

def vwap(candles):
    pv = sum(((c["h"] + c["l"] + c["c"]) / 3) * c["v"] for c in candles)
    vv = sum(c["v"] for c in candles)
    return pv / vv if vv > 0 else candles[-1]["c"]

# ---------------- FRED ----------------
def fred_series_latest(series_id, n=8):
    if not FRED_API_KEY:
        return None
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": series_id, "api_key": FRED_API_KEY,
                                 "file_type": "json", "sort_order": "desc", "limit": n},
                         timeout=15)
        vals = [float(o["value"]) for o in r.json().get("observations", []) if o["value"] not in (".", "")]
        return vals  # newest first
    except Exception as e:
        logmsg(f"FRED error {series_id}: {e}")
        return None

def refresh_fred():
    for pair, cfg in PAIR_MAP.items():
        vals = fred_series_latest(cfg["fred_series"])
        if vals and len(vals) >= 6:
            chg = (vals[0] - vals[5]) / vals[5] * 100.0
            FRED[pair] = round(chg, 2)
            logmsg(f"FRED {pair} ({cfg['fred_series']}): 5d {chg:+.2f}%")

# ---------------- EIGHT-FILTER ENGINE ----------------
def analyze_pair(pair):
    cfg = PAIR_MAP[pair]
    candles = get_candles(pair)
    if len(candles) < 30:
        return {"pair": pair, "ok": False, "reason": "insufficient candles"}
    closes = [c["c"] for c in candles]
    price = closes[-1]
    votes = []  # (filter, +1 long / -1 short / 0 neutral, note)

    # F1 VWAP deviation
    vw = vwap(candles[-20:])
    dev = (price - vw) / vw * 100
    votes.append(("F1_vwap", 1 if dev > 0.02 else (-1 if dev < -0.02 else 0), f"dev {dev:+.3f}%"))

    # F2 EMA trend
    e20, e50 = ema(closes[-60:], 20), ema(closes[-60:], 50)
    votes.append(("F2_ema", 1 if e20 > e50 else -1, f"E20 {'>' if e20 > e50 else '<'} E50"))

    # F3 Futures volume (manual CME updates)
    fv = VOLUME.get(cfg["future"])
    if fv is None:
        votes.append(("F3_volume", 0, "no data"))
    else:
        net = fv["net_pct"]
        if cfg["vol_invert"]:
            net = -net
        votes.append(("F3_volume", 1 if net > 0.15 else (-1 if net < -0.15 else 0), f"{cfg['future']} net {fv['net_pct']:+.2f}%"))

    # F4 RSI exhaustion (fade extreme against trend)
    r = rsi(closes)
    if r > 70:
        votes.append(("F4_rsi", -1, f"RSI {r:.0f} overbought"))
    elif r < 30:
        votes.append(("F4_rsi", 1, f"RSI {r:.0f} oversold"))
    else:
        votes.append(("F4_rsi", 0, f"RSI {r:.0f}"))

    # F5 Session (with chop skip)
    now = datetime.now(timezone.utc)
    h = now.hour + now.minute / 60.0
    in_session = False
    for name, op, cl in SESSIONS:
        if (op + CHOP_SKIP_MIN / 60.0) <= h < cl:
            in_session = True
            break
    votes.append(("F5_session", 0, f"{'in session' if in_session else 'out of session/chop'}"))
    if not in_session:
        return {"pair": pair, "ok": False, "reason": "out of session (chop-skip or closed)",
                "votes": [(v[0], v[1], v[2]) for v in votes]}

    # F6 EV gap / RR geometry
    a = atr(candles)
    if a <= 0:
        return {"pair": pair, "ok": False, "reason": "ATR zero"}
    votes.append(("F6_rr", 0, f"ATR {a:.5f} | stop 1.5x target 3.0x (2:1)"))

    # F7 COT positioning
    cot = COT.get(cfg["future"])
    if cot is None:
        votes.append(("F7_cot", 0, "no data"))
    else:
        net = cot["net"]
        disp = net
        if cfg["cot_invert"]:
            net = -net
        if net > 20000:
            votes.append(("F7_cot", 1, f"{cfg['future']} COT {disp:+,.0f}"))
        elif net < -20000:
            votes.append(("F7_cot", -1, f"{cfg['future']} COT {disp:+,.0f}"))
        else:
            votes.append(("F7_cot", 0, f"{cfg['future']} COT {disp:+,.0f} neutral"))

    # F8 FRED macro
    fchg = FRED.get(pair)
    if fchg is None:
        votes.append(("F8_fred", 0, "no data"))
    else:
        votes.append(("F8_fred", 1 if fchg > 0 else -1, f"5d {fchg:+.2f}%"))

    score_long = sum(1 for v in votes if v[1] > 0)
    score_short = sum(1 for v in votes if v[1] < 0)
    direction = "LONG" if score_long > score_short else "SHORT"
    total = len(votes)
    conf = max(score_long, score_short) / total * 100.0

    # Macro veto (FRED against direction beyond threshold)
    if fchg is not None and abs(fchg) >= MACRO_BLOCK_PCT:
        fred_dir = "LONG" if fchg > 0 else "SHORT"
        if cfg["fred_up_means"] == "SHORT":
            fred_dir = "SHORT" if fchg > 0 else "LONG"
        if fred_dir != direction:
            return {"pair": pair, "ok": False, "reason": f"FRED macro veto ({fchg:+.2f}% vs {direction})",
                    "confidence": round(conf, 1), "direction": direction,
                    "votes": [(v[0], v[1], v[2]) for v in votes]}

    if conf < MIN_CONFIDENCE or score_long == score_short:
        return {"pair": pair, "ok": False, "reason": f"confidence {conf:.0f}% < {MIN_CONFIDENCE:.0f}%",
                "confidence": round(conf, 1), "direction": direction,
                "votes": [(v[0], v[1], v[2]) for v in votes]}

    stop_dist = 1.5 * a
    tgt_dist = 3.0 * a
    if direction == "LONG":
        sl, tp = price - stop_dist, price + tgt_dist
    else:
        sl, tp = price + stop_dist, price - tgt_dist

    return {"pair": pair, "ok": True, "direction": direction, "confidence": round(conf, 1),
            "price": price, "sl": sl, "tp": tp, "stop_dist": stop_dist, "atr": a,
            "votes": [(v[0], v[1], v[2]) for v in votes]}

# ---------------- EXECUTION ----------------
def execute_signal(sig):
    reset_daily()
    pair = sig["pair"]
    pnl = daily_pnl()
    if DAILY["lockdown"] or pnl <= -abs(DAILY_LOSS_LIMIT):
        DAILY["lockdown"] = True
        logmsg(f"LOCKDOWN: daily loss limit hit (pnl {pnl:.2f} <= -{DAILY_LOSS_LIMIT})")
        return {"executed": False, "reason": "daily loss lockdown"}
    if DAILY["trades"] >= MAX_TRADES_DAY:
        return {"executed": False, "reason": "max trades reached"}
    if pair in DAILY["traded_pairs"]:
        return {"executed": False, "reason": "pair already traded today"}
    if pair in get_open_position_pairs():
        return {"executed": False, "reason": "open position (FIFO)"}

    acct = get_account()
    sz = calc_units(pair, sig["price"], sig["stop_dist"], acct["balance"], acct["margin_available"])
    if sz["units"] <= 0:
        return {"executed": False, "reason": "zero units"}

    res = place_market_order(pair, sig["direction"], sz["units"], sig["price"], sig["sl"], sig["tp"])
    if res["ok"]:
        DAILY["trades"] += 1
        DAILY["traded_pairs"].append(pair)
        u = sz["units"] if sig["direction"] == "LONG" else -sz["units"]
        msg = (f"TRADE {sig['direction']} {pair} units={u} fill={res['fill']} "
               f"sl={fmt_price(pair, sig['sl'])} tp={fmt_price(pair, sig['tp'])} "
               f"conf={sig['confidence']}% (sized_by={sz['capped_by']})")
        logmsg(msg)
        tg(f"✅ {msg}")
        return {"executed": True, "fill": res["fill"], "units": u, "sized_by": sz["capped_by"]}
    else:
        logmsg(f"REJECTED {pair}: {res['reason']}")
        tg(f"⚠️ REJECTED {pair}: {res['reason']}")
        return {"executed": False, "reason": res["reason"]}

def run_scan():
    reset_daily()
    results = []
    for pair in PAIRS:
        try:
            sig = analyze_pair(pair)
            if sig.get("ok"):
                results.append(execute_signal(sig))
            else:
                results.append({"pair": pair, "skipped": sig.get("reason", "?")})
        except Exception as e:
            logmsg(f"scan error {pair}: {e}")
            results.append({"pair": pair, "error": str(e)})
    return results

# ---------------- SCANNER THREAD ----------------
_cycle = [0]
def scanner_loop():
    while True:
        try:
            _cycle[0] += 1
            if _cycle[0] % 8 == 1:
                refresh_fred()
            run_scan()
        except Exception as e:
            logmsg(f"scanner error: {e}")
        time.sleep(SCAN_INTERVAL)

# ---------------- API ----------------
app = FastAPI(title="ForexFlow EightFilter", version="2.4.0")

class VolumeUpdate(BaseModel):
    future: str
    net_pct: float

class CotUpdate(BaseModel):
    future: str
    net: float

@app.on_event("startup")
def startup():
    logmsg("ForexFlow EightFilter v2.4.0 started (7 pairs, chop-skip)")
    threading.Thread(target=scanner_loop, daemon=True).start()

@app.get("/health")
def health():
    try:
        acct = get_account()
        return {"status": "ok", "version": "2.4.0", "env": OANDA_ENV,
                "oanda": "connected", "balance": acct["balance"],
                "auto_trade": AUTO_TRADE, "pairs": PAIRS}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}

@app.get("/balance")
def balance():
    a = get_account()
    return {"balance": a["balance"], "nav": a["nav"],
            "margin_available": a["margin_available"],
            "unrealized_pl": a["unrealized_pl"], "env": OANDA_ENV, "status": "connected"}

@app.get("/daily")
def daily():
    reset_daily()
    return {"date": DAILY["date"], "trades": DAILY["trades"],
            "start_balance": DAILY["start_balance"], "traded_pairs": DAILY["traded_pairs"],
            "realized_pnl": round(daily_pnl(), 2), "loss_limit": DAILY_LOSS_LIMIT,
            "lockdown": DAILY["lockdown"]}

@app.post("/volume-update")
def volume_update(v: VolumeUpdate):
    VOLUME[v.future.upper()] = {"net_pct": v.net_pct, "ts": datetime.now(timezone.utc).isoformat()}
    logmsg(f"Volume updated {v.future.upper()}: net {v.net_pct:+.2f}%")
    return {"ok": True, "volume": VOLUME}

@app.get("/volume-status")
def volume_status():
    return VOLUME

@app.post("/cot-update")
def cot_update(c: CotUpdate):
    COT[c.future.upper()] = {"net": c.net, "ts": datetime.now(timezone.utc).isoformat()}
    logmsg(f"COT updated {c.future.upper()}: net {c.net:+,.0f}")
    return {"ok": True, "cot": COT}

@app.get("/cot-status")
def cot_status():
    return COT

@app.post("/fred-refresh")
def fred_refresh():
    refresh_fred()
    return FRED

@app.get("/fred-status")
def fred_status():
    return FRED

@app.get("/analyze/{pair}")
def analyze(pair: str):
    if pair not in PAIR_MAP:
        return {"error": f"unknown pair; valid: {PAIRS}"}
    return analyze_pair(pair)

@app.get("/scan")
def scan():
    return {"auto_trade": AUTO_TRADE, "results": run_scan()}

@app.post("/trade")
def trade(pair: str):
    if pair not in PAIR_MAP:
        return {"error": f"unknown pair; valid: {PAIRS}"}
    sig = analyze_pair(pair)
    if not sig.get("ok"):
        return sig
    return execute_signal(sig)

@app.get("/logs")
def logs():
    return LOGS[-100:]

@app.get("/dashboard")
def dashboard():
    reset_daily()
    return {"version": "2.4.0", "auto_trade": AUTO_TRADE, "pairs": PAIRS,
            "daily": {"date": DAILY["date"], "trades": DAILY["trades"],
                      "pnl": round(daily_pnl(), 2), "lockdown": DAILY["lockdown"],
                      "traded_pairs": DAILY["traded_pairs"]},
            "volume": VOLUME, "cot": COT, "fred": FRED,
            "chop_skip_min": CHOP_SKIP_MIN, "sessions": SESSIONS,
            "recent_logs": LOGS[-20:]}
