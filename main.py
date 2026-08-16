"""
ForexFlow EightFilter v3.2.1 — "Carry Trend"
Institutional-style forex signal engine — 7 CME-futures-backed pairs.

v3.2.1 hotfix — AUTO-COT market names:
  - CFTC renamed two CME markets; old names returned stale 2022 rows:
      6B: "BRITISH POUND STERLING" -> "BRITISH POUND"
      6N: "NEW ZEALAND DOLLAR"     -> "NZ DOLLAR"
  - All 7 contracts now pull current weekly reports from the Socrata feed.

v3.2.0 — AUTO-COT:
  - COT data now SELF-UPDATES from the CFTC's free public Socrata feed
    (publicreporting.cftc.gov, legacy futures-only report, no API key).
  - maybe_refresh_cot() runs on scanner cycles, throttled to 12h; pulls
    net non-commercial positioning for all 7 CME FX contracts and stores
    report_date + source="cftc-auto". Manual /cot-update still works and
    overrides. /cot-refresh forces a pull on demand.
  - The Friday screenshot ritual is retired. (Volume stays manual —
    CME futures volume has no free public API; that one is real.)

v3.1.1 hotfix:
  - /close/{pair}: OANDA path fixed /position/ -> /positions/ (404 bug;
    reverted accidentally in the v3.1.0 revamp). Emergency flatten works again.

v3.1.0:
  - /backtest/pnl `pair` query param + /live-check endpoint pinned to
    LIVE_PAIR (trend core, daily bars). The 7-pair blend is research-only.

v3.0.0 — the first evidence-backed live core:
  - LIVE ENGINE = USD/JPY DAILY trend (the split-sample survivor:
    PF 1.30 in 2007-2016, PF 1.26 in 2017-2026, 412 trades over 19y)
  - Signal: price_votes on D1 candles, >=2 of 3 agree, once per daily bar
  - Overlays: FRED DEXJPUS = daily macro VETO, 6J COT = weekly VETO
  - All brakes unchanged: 1% risk, $500 daily lockdown, ledger,
    outcome tracker, Telegram.

Eight filters:
  F1 VWAP dev | F2 EMA20/50 | F3 CME volume | F4 RSI | F5 session (chop-skip)
  F6 EV/spread gate | F7 COT | F8 FRED macro
Risk: 1%/trade, max 3/day, $500 daily loss LOCKDOWN, margin-aware sizing,
      FIFO-safe, one trade per pair per day.
"""
import os, time, math, threading, logging, sqlite3, json, statistics
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
CHOP_SKIP_MIN     = int(os.getenv("CHOP_SKIP_MIN", "30"))
SPREAD_STOP_MULT  = float(os.getenv("SPREAD_STOP_MULT", "3.0"))  # F6: stop >= 3x spread
COT_REFRESH_SEC   = int(os.getenv("COT_REFRESH_SEC", "43200"))   # auto-COT every 12h
DB_PATH           = os.getenv("LEDGER_DB", "forexflow_ledger.db")

BASE = "https://api-fxpractice.oanda.com" if OANDA_ENV == "practice" else "https://api-fxtrade.oanda.com"
HEADERS = {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}

SESSIONS = [("London", 7.0, 11.0), ("NewYork", 12.0, 16.0)]

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
VOTABLE = 6  # F1,F2,F3,F4,F7,F8 — F5 gates session, F6 gates EV
LIVE_PAIR = "USD_JPY"          # v3.0: the split-sample survivor
LAST_BAR = {}                  # pair -> last traded daily-bar date
COT_VETO = 20000               # net positioning beyond this vs direction = veto

# CFTC legacy futures-only COT market names (Socrata dataset 6dca-aqww)
# NOTE: names must match CFTC exactly — 6B/6N were renamed by CFTC (v3.2.1 fix)
COT_MARKETS = {
    "6E": "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "6B": "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "6J": "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "6A": "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "6N": "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "6C": "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "6S": "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
}
COT_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
COT_LAST_PULL = [0.0]

# ---------------- LEDGER (SQLite) ----------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, pair TEXT, direction TEXT,
        confidence REAL, votes TEXT, executed INTEGER, reason TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts_open TEXT, pair TEXT, direction TEXT,
        units INTEGER, fill REAL, sl REAL, tp REAL, confidence REAL,
        status TEXT DEFAULT 'open', ts_close TEXT, pnl REAL)""")
    conn.commit()
    return conn

def ledger_signal(pair, direction, conf, votes, executed, reason):
    try:
        c = db()
        c.execute("INSERT INTO signals(ts,pair,direction,confidence,votes,executed,reason) VALUES(?,?,?,?,?,?,?)",
                  (datetime.now(timezone.utc).isoformat(), pair, direction, conf,
                   json.dumps(votes), 1 if executed else 0, reason))
        c.commit(); c.close()
    except Exception as e:
        logmsg(f"ledger signal error: {e}")

def ledger_trade_open(pair, direction, units, fill, sl, tp, conf):
    try:
        c = db()
        cur = c.execute("INSERT INTO trades(ts_open,pair,direction,units,fill,sl,tp,confidence,status) VALUES(?,?,?,?,?,?,?,?,'open')",
                        (datetime.now(timezone.utc).isoformat(), pair, direction, units, fill, sl, tp, conf))
        tid = cur.lastrowid
        c.commit(); c.close()
        return tid
    except Exception as e:
        logmsg(f"ledger trade error: {e}")
        return None

def ledger_trade_close(tid, pnl):
    try:
        c = db()
        c.execute("UPDATE trades SET status='closed', ts_close=?, pnl=? WHERE id=?",
                  (datetime.now(timezone.utc).isoformat(), pnl, tid))
        c.commit(); c.close()
    except Exception as e:
        logmsg(f"ledger close error: {e}")

def track_open_trades():
    """Close out ledger trades whose OANDA position has disappeared."""
    try:
        c = db()
        rows = c.execute("SELECT id, pair, direction, fill, ts_open FROM trades WHERE status='open'").fetchall()
        c.close()
        if not rows:
            return
        open_pairs = get_open_position_pairs()
        for tid, pair, direction, fill, ts_open in rows:
            if pair in open_pairs:
                continue
            pnl = None
            try:
                d = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/trades?state=CLOSED&instrument={pair}&count=10")
                for t in reversed(d.get("trades", [])):
                    if t.get("state") == "CLOSED" and t.get("openTime", "") >= ts_open[:19]:
                        pnl = float(t.get("realizedPL", 0.0)) + float(t.get("financing", 0.0))
                        break
            except Exception:
                pass
            ledger_trade_close(tid, pnl)
            emoji = "✅" if (pnl or 0) > 0 else "❌"
            msg = f"{emoji} CLOSED {direction} {pair} fill={fill} pnl={pnl if pnl is not None else 'n/a'}"
            logmsg(msg); tg(msg)
    except Exception as e:
        logmsg(f"tracker error: {e}")

# ---------------- STATE ----------------
LOGS = []
VOLUME, COT, FRED = {}, {}, {}
DAILY = {"date": None, "trades": 0, "start_balance": None, "traded_pairs": [], "lockdown": False}

def logmsg(msg):
    LOGS.append({"ts": datetime.now(timezone.utc).isoformat(), "msg": msg})
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

# ---------------- OANDA ----------------
def oanda_get(path):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, timeout=20)
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

def get_quote(pair):
    d = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/pricing?instruments={pair}")
    p = d["prices"][0]
    bid = float(p["bids"][0]["price"]); ask = float(p["asks"][0]["price"])
    return {"mid": (bid + ask) / 2.0, "spread": ask - bid}

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
    pip = PAIR_MAP[pair]["pip"]
    return pip if pair.endswith("_USD") else pip / price

def calc_units(pair, price, stop_dist, balance, margin_avail):
    risk_amt = balance * (RISK_PCT / 100.0)
    pv = pip_value_per_unit(pair, price)
    stop_pips = stop_dist / PAIR_MAP[pair]["pip"]
    risk_units = int(risk_amt / (stop_pips * pv)) if pv > 0 and stop_pips > 0 else 0
    notional_per_unit = price if pair.endswith("_USD") else 1.0
    margin_units = int(margin_avail * LEVERAGE * (MARGIN_USAGE_PCT / 100.0) / notional_per_unit)
    units = min(risk_units, margin_units, MAX_UNITS)
    capped = "risk" if units == risk_units and units < MAX_UNITS else ("margin" if units == margin_units else "max")
    return {"units": max(units, 0), "capped_by": capped}

def place_market_order(pair, direction, units, sl, tp):
    u = units if direction == "LONG" else -units
    order = {"order": {"type": "MARKET", "instrument": pair, "units": str(u),
                       "stopLossOnFill": {"price": fmt_price(pair, sl)},
                       "takeProfitOnFill": {"price": fmt_price(pair, tp)}}}
    try:
        r = requests.post(f"{BASE}/v3/accounts/{OANDA_ACCOUNT}/orders",
                          headers=HEADERS, json=order, timeout=15)
        d = r.json()
        if r.status_code in (200, 201) and "orderFillTransaction" in d:
            return {"ok": True, "fill": float(d["orderFillTransaction"]["price"])}
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
    gains = losses = 0.0
    for i in range(-n, 0):
        ch = closes[i] - closes[i - 1]
        if ch > 0: gains += ch
        else: losses -= ch
    if losses == 0:
        return 100.0
    return 100 - 100 / (1 + (gains / n) / (losses / n))

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

def hour_of(ts):
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.hour + dt.minute / 60.0
    except Exception:
        return -1

def in_session(h):
    for name, op, cl in SESSIONS:
        if (op + CHOP_SKIP_MIN / 60.0) <= h < cl:
            return True
    return False

# ---------------- FRED ----------------
def fred_series_latest(series_id, n=8):
    if not FRED_API_KEY:
        return None
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": series_id, "api_key": FRED_API_KEY,
                                 "file_type": "json", "sort_order": "desc", "limit": n},
                         timeout=15)
        return [float(o["value"]) for o in r.json().get("observations", []) if o["value"] not in (".", "")]
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

# ---------------- AUTO-COT (CFTC Socrata, free public feed) ----------------
def fetch_cot():
    """Pull latest net non-commercial positioning for all 7 CME FX contracts."""
    pulled = 0
    for code, name in COT_MARKETS.items():
        try:
            r = requests.get(COT_URL,
                             params={"market_and_exchange_names": name,
                                     "$order": "report_date_as_yyyy_mm_dd DESC",
                                     "$limit": 1},
                             headers={"User-Agent": "ForexFlow/3.2 (personal research bot)"},
                             timeout=20)
            d = r.json()
            if not isinstance(d, list) or not d:
                logmsg(f"COT auto {code}: no rows for '{name}' (check market name)")
                continue
            row = d[0]
            net = int(float(row["noncomm_positions_long_all"])) - int(float(row["noncomm_positions_short_all"]))
            rep = str(row.get("report_date_as_yyyy_mm_dd", ""))[:10]
            COT[code] = {"net": net, "ts": datetime.now(timezone.utc).isoformat(),
                         "report_date": rep, "source": "cftc-auto"}
            logmsg(f"COT auto {code}: net {net:+,} (report {rep})")
            pulled += 1
        except Exception as e:
            logmsg(f"COT auto error {code}: {e}")
    if pulled:
        tg(f"📊 COT auto-update: {pulled}/7 contracts pulled from CFTC")
    return pulled

def maybe_refresh_cot():
    if time.time() - COT_LAST_PULL[0] >= COT_REFRESH_SEC:
        COT_LAST_PULL[0] = time.time()
        fetch_cot()

# ---------------- CORE PRICE FILTERS (shared by live + backtest) ----------------
def price_votes(candles):
    """F1/F2/F4 votes from candles alone. Returns list of (name, vote, note)."""
    closes = [c["c"] for c in candles]
    price = closes[-1]
    votes = []
    vw = vwap(candles[-20:])
    dev = (price - vw) / vw * 100
    votes.append(("F1_vwap", 1 if dev > 0.02 else (-1 if dev < -0.02 else 0), f"dev {dev:+.3f}%"))
    e20, e50 = ema(closes[-60:], 20), ema(closes[-60:], 50)
    votes.append(("F2_ema", 1 if e20 > e50 else -1, ""))
    r = rsi(closes)
    if r > 70: votes.append(("F4_rsi", -1, f"RSI {r:.0f} OB"))
    elif r < 30: votes.append(("F4_rsi", 1, f"RSI {r:.0f} OS"))
    else: votes.append(("F4_rsi", 0, f"RSI {r:.0f}"))
    return votes

# ---------------- LIVE EIGHT-FILTER ENGINE (research path) ----------------
def analyze_pair(pair):
    cfg = PAIR_MAP[pair]
    candles = get_candles(pair)
    if len(candles) < 30:
        return {"pair": pair, "ok": False, "reason": "insufficient candles"}
    price = candles[-1]["c"]
    votes = price_votes(candles)

    fv = VOLUME.get(cfg["future"])
    if fv is None:
        votes.append(("F3_volume", 0, "no data"))
    else:
        net = -fv["net_pct"] if cfg["vol_invert"] else fv["net_pct"]
        votes.append(("F3_volume", 1 if net > 0.15 else (-1 if net < -0.15 else 0),
                      f"{cfg['future']} net {fv['net_pct']:+.2f}%"))

    now = datetime.now(timezone.utc)
    h = now.hour + now.minute / 60.0
    if not in_session(h):
        ledger_signal(pair, "NONE", 0, [(v[0], v[1]) for v in votes], False, "out of session")
        return {"pair": pair, "ok": False, "reason": "out of session (chop-skip or closed)",
                "votes": [(v[0], v[1], v[2]) for v in votes]}

    cot = COT.get(cfg["future"])
    if cot is None:
        votes.append(("F7_cot", 0, "no data"))
    else:
        net = -cot["net"] if cfg["cot_invert"] else cot["net"]
        v = 1 if net > 20000 else (-1 if net < -20000 else 0)
        votes.append(("F7_cot", v, f"{cfg['future']} COT {cot['net']:+,.0f}"))

    fchg = FRED.get(pair)
    votes.append(("F8_fred", (1 if fchg > 0 else -1) if fchg is not None else 0,
                  f"5d {fchg:+.2f}%" if fchg is not None else "no data"))

    score_long = sum(1 for v in votes if v[1] > 0)
    score_short = sum(1 for v in votes if v[1] < 0)
    if score_long == score_short:
        ledger_signal(pair, "NONE", 50, [(v[0], v[1]) for v in votes], False, "tie")
        return {"pair": pair, "ok": False, "reason": "tie",
                "votes": [(v[0], v[1], v[2]) for v in votes]}
    direction = "LONG" if score_long > score_short else "SHORT"
    conf = max(score_long, score_short) / VOTABLE * 100.0

    if fchg is not None and abs(fchg) >= MACRO_BLOCK_PCT:
        fred_dir = "LONG" if fchg > 0 else "SHORT"
        if cfg["fred_up_means"] == "SHORT":
            fred_dir = "SHORT" if fchg > 0 else "LONG"
        if fred_dir != direction:
            ledger_signal(pair, direction, conf, [(v[0], v[1]) for v in votes], False, "fred veto")
            return {"pair": pair, "ok": False, "reason": f"FRED macro veto ({fchg:+.2f}% vs {direction})",
                    "confidence": round(conf, 1), "direction": direction,
                    "votes": [(v[0], v[1], v[2]) for v in votes]}

    a = atr(candles)
    if a <= 0:
        return {"pair": pair, "ok": False, "reason": "ATR zero"}
    stop_dist, tgt_dist = 1.5 * a, 3.0 * a

    try:
        spread = get_quote(pair)["spread"]
    except Exception:
        spread = 0.0
    if spread > 0 and stop_dist < SPREAD_STOP_MULT * spread:
        ledger_signal(pair, direction, conf, [(v[0], v[1]) for v in votes], False,
                      f"EV gate: stop {stop_dist:.5f} < {SPREAD_STOP_MULT}x spread {spread:.5f}")
        return {"pair": pair, "ok": False,
                "reason": f"EV gate: stop too thin vs spread ({stop_dist/spread:.1f}x < {SPREAD_STOP_MULT}x)",
                "confidence": round(conf, 1), "direction": direction,
                "votes": [(v[0], v[1], v[2]) for v in votes]}

    if conf < MIN_CONFIDENCE:
        ledger_signal(pair, direction, conf, [(v[0], v[1]) for v in votes], False, f"conf {conf:.0f}%")
        return {"pair": pair, "ok": False, "reason": f"confidence {conf:.0f}% < {MIN_CONFIDENCE:.0f}%",
                "confidence": round(conf, 1), "direction": direction,
                "votes": [(v[0], v[1], v[2]) for v in votes]}

    if direction == "LONG":
        sl, tp = price - stop_dist, price + tgt_dist
    else:
        sl, tp = price + stop_dist, price - tgt_dist

    return {"pair": pair, "ok": True, "direction": direction, "confidence": round(conf, 1),
            "price": price, "sl": sl, "tp": tp, "stop_dist": stop_dist, "spread": spread,
            "votes": [(v[0], v[1], v[2]) for v in votes]}

# ---------------- LIVE DAILY CORE (v3.0 — mirrors the proven backtest) ----------------
def analyze_daily(pair=LIVE_PAIR):
    """USD/JPY daily trend: >=2 of 3 price votes agree on a NEW daily bar.
    FRED + COT act as vetoes only (never create a trade)."""
    cfg = PAIR_MAP[pair]
    candles = get_candles(pair, count=60, gran="D")
    if len(candles) < 30:
        return {"pair": pair, "ok": False, "reason": "insufficient daily candles"}
    bar_date = candles[-1]["t"][:10]
    if LAST_BAR.get(pair) == bar_date:
        return {"pair": pair, "ok": False, "reason": f"bar {bar_date} already evaluated"}
    votes = price_votes(candles)
    sc_l = sum(1 for v in votes if v[1] > 0)
    sc_s = sum(1 for v in votes if v[1] < 0)
    if max(sc_l, sc_s) < 2 or sc_l == sc_s:
        ledger_signal(pair, "NONE", 0, [(v[0], v[1]) for v in votes], False, "daily: no 2-of-3 agreement")
        LAST_BAR[pair] = bar_date
        return {"pair": pair, "ok": False, "reason": "no 2-of-3 agreement",
                "bar": bar_date, "votes": [(v[0], v[1], v[2]) for v in votes]}
    direction = "LONG" if sc_l > sc_s else "SHORT"
    conf = max(sc_l, sc_s) / 3.0 * 100.0

    fchg = FRED.get(pair)
    if fchg is not None and abs(fchg) >= MACRO_BLOCK_PCT:
        fred_dir = "LONG" if fchg > 0 else "SHORT"
        if cfg["fred_up_means"] == "SHORT":
            fred_dir = "SHORT" if fchg > 0 else "LONG"
        if fred_dir != direction:
            ledger_signal(pair, direction, conf, [(v[0], v[1]) for v in votes], False, "fred veto")
            LAST_BAR[pair] = bar_date
            return {"pair": pair, "ok": False, "reason": f"FRED veto ({fchg:+.2f}% vs {direction})",
                    "direction": direction, "confidence": round(conf, 1), "bar": bar_date}

    cot = COT.get(cfg["future"])
    if cot is not None:
        net = -cot["net"] if cfg["cot_invert"] else cot["net"]
        if (direction == "LONG" and net < -COT_VETO) or (direction == "SHORT" and net > COT_VETO):
            ledger_signal(pair, direction, conf, [(v[0], v[1]) for v in votes], False, "cot veto")
            LAST_BAR[pair] = bar_date
            return {"pair": pair, "ok": False, "reason": f"COT veto ({cfg['future']} net {cot['net']:+,.0f} vs {direction})",
                    "direction": direction, "confidence": round(conf, 1), "bar": bar_date}

    a = atr(candles)
    if a <= 0:
        return {"pair": pair, "ok": False, "reason": "ATR zero"}
    stop_dist, tgt_dist = 1.5 * a, 3.0 * a
    price = candles[-1]["c"]
    try:
        spread = get_quote(pair)["spread"]
    except Exception:
        spread = 0.0
    if spread > 0 and stop_dist < SPREAD_STOP_MULT * spread:
        return {"pair": pair, "ok": False, "reason": "EV gate (daily stop vs spread)"}

    if direction == "LONG":
        sl, tp = price - stop_dist, price + tgt_dist
    else:
        sl, tp = price + stop_dist, price - tgt_dist
    LAST_BAR[pair] = bar_date
    return {"pair": pair, "ok": True, "direction": direction, "confidence": round(conf, 1),
            "price": price, "sl": sl, "tp": tp, "stop_dist": stop_dist, "spread": spread,
            "bar": bar_date, "votes": [(v[0], v[1], v[2]) for v in votes]}

# ---------------- EXECUTION ----------------
def execute_signal(sig):
    reset_daily()
    pair = sig["pair"]
    if not AUTO_TRADE:
        logmsg(f"AUTO_TRADE=false: signal {sig['direction']} {pair} conf={sig.get('confidence')}% NOT executed")
        ledger_signal(pair, sig["direction"], sig.get("confidence", 0),
                      [(v[0], v[1]) for v in sig.get("votes", [])], False, "auto_trade off")
        return {"executed": False, "reason": "AUTO_TRADE is false (signal logged)"}
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

    res = place_market_order(pair, sig["direction"], sz["units"], sig["sl"], sig["tp"])
    if res["ok"]:
        DAILY["trades"] += 1
        DAILY["traded_pairs"].append(pair)
        u = sz["units"] if sig["direction"] == "LONG" else -sz["units"]
        tid = ledger_trade_open(pair, sig["direction"], u, res["fill"], sig["sl"], sig["tp"], sig["confidence"])
        ledger_signal(pair, sig["direction"], sig["confidence"],
                      [(v[0], v[1]) for v in sig["votes"]], True, "filled")
        msg = (f"TRADE {sig['direction']} {pair} units={u} fill={res['fill']} "
               f"sl={fmt_price(pair, sig['sl'])} tp={fmt_price(pair, sig['tp'])} "
               f"conf={sig['confidence']}% (sized_by={sz['capped_by']}, ledger#{tid})")
        logmsg(msg); tg(f"✅ {msg}")
        return {"executed": True, "fill": res["fill"], "units": u, "ledger_id": tid}
    else:
        ledger_signal(pair, sig["direction"], sig["confidence"],
                      [(v[0], v[1]) for v in sig["votes"]], False, res["reason"])
        logmsg(f"REJECTED {pair}: {res['reason']}"); tg(f"⚠️ REJECTED {pair}: {res['reason']}")
        return {"executed": False, "reason": res["reason"]}

def run_scan():
    reset_daily()
    track_open_trades()
    try:
        sig = analyze_daily(LIVE_PAIR)
        if sig.get("ok"):
            return [execute_signal(sig)]
        return [{"pair": LIVE_PAIR, "skipped": sig.get("reason", "?"), "bar": sig.get("bar")}]
    except Exception as e:
        logmsg(f"scan error {LIVE_PAIR}: {e}")
        return [{"pair": LIVE_PAIR, "error": str(e)}]

# ---------------- BACKTEST (Kalshi-style /backtest/pnl) ----------------
def backtest_pair(pair, candles, spread_est, risk_usd=1000.0, core="trend", hold_bars=96,
                  skip_session=False):
    """Replay a signal core over history. Returns trade list (hour, pair, R).
    Cores:
      trend  — F1/F2/F4 votes, >=2 agree, trade WITH majority, 2:1 RR
      invert — same triggers, opposite direction
      revert — fade RSI extremes at VWAP stretch, 1:1 RR"""
    trades = []
    i = 60
    while i < len(candles) - 1:
        win = candles[i - 59:i + 1]
        h = hour_of(win[-1]["t"])
        if not skip_session and not in_session(h):
            i += 1; continue
        closes = [c["c"] for c in win]
        r_val = rsi(closes)
        if core == "revert":
            vw = vwap(win[-20:])
            dev = (closes[-1] - vw) / vw * 100
            if r_val > 70 and dev > 0.05:
                direction = "SHORT"
            elif r_val < 30 and dev < -0.05:
                direction = "LONG"
            else:
                i += 1; continue
            tp_mult = 1.0
        else:
            votes = price_votes(win)
            sl_votes = sum(1 for v in votes if v[1] > 0)
            ss_votes = sum(1 for v in votes if v[1] < 0)
            if max(sl_votes, ss_votes) < 2 or sl_votes == ss_votes:
                i += 1; continue
            direction = "LONG" if sl_votes > ss_votes else "SHORT"
            if core == "invert":
                direction = "SHORT" if direction == "LONG" else "LONG"
            tp_mult = 2.0
        a = atr(win)
        if a <= 0:
            i += 1; continue
        stop_dist = 1.5 * a
        if stop_dist < SPREAD_STOP_MULT * spread_est:
            i += 1; continue
        entry = candles[i]["c"]
        cost_in_R = spread_est / stop_dist
        R = None; bars_held = 0
        for j in range(i + 1, min(i + hold_bars + 1, len(candles))):
            b = candles[j]; bars_held += 1
            if direction == "LONG":
                hit_sl = b["l"] <= entry - stop_dist
                hit_tp = b["h"] >= entry + tp_mult * stop_dist
            else:
                hit_sl = b["h"] >= entry + stop_dist
                hit_tp = b["l"] <= entry - tp_mult * stop_dist
            if hit_sl and hit_tp:
                R = -1.0; break
            if hit_sl:
                R = -1.0; break
            if hit_tp:
                R = tp_mult; break
        if R is None:
            exit_p = candles[min(i + hold_bars, len(candles) - 1)]["c"]
            move = (exit_p - entry) if direction == "LONG" else (entry - exit_p)
            R = move / stop_dist
        R -= cost_in_R
        trades.append((int(h), pair, R))
        i += bars_held + 1
    return trades

def grade(trades, risk_usd=1000.0):
    if not trades:
        return {"trades": 0}
    wins = [t for t in trades if t[2] > 0]
    gp = sum(t[2] for t in wins)
    gl = -sum(t[2] for t in trades if t[2] < 0)
    eq = peak = maxdd = 0.0
    for t in trades:
        eq += t[2] * risk_usd
        peak = max(peak, eq)
        maxdd = max(maxdd, peak - eq)
    by_hour, by_pair = {}, {}
    for h, p, r in trades:
        by_hour.setdefault(h, []).append(r)
        by_pair.setdefault(p, []).append(r)
    return {
        "trades": len(trades),
        "win_rate": round(len(wins) / len(trades) * 100, 1),
        "profit_factor": round(gp / gl, 2) if gl > 0 else None,
        "total_R": round(sum(t[2] for t in trades), 1),
        "pnl_usd_at_1pct": round(sum(t[2] for t in trades) * risk_usd, 0),
        "max_drawdown_usd": round(maxdd, 0),
        "by_hour": {str(h): {"n": len(v), "R": round(sum(v), 1),
                             "WR": round(sum(1 for x in v if x > 0) / len(v) * 100, 0)}
                    for h, v in sorted(by_hour.items())},
        "by_pair": {p: {"n": len(v), "R": round(sum(v), 1),
                        "WR": round(sum(1 for x in v if x > 0) / len(v) * 100, 0)}
                    for p, v in sorted(by_pair.items())},
    }

# ---------------- SCANNER THREAD ----------------
_cycle = [0]
def scanner_loop():
    while True:
        try:
            _cycle[0] += 1
            if _cycle[0] % 8 == 1:
                refresh_fred()
            maybe_refresh_cot()
            run_scan()
        except Exception as e:
            logmsg(f"scanner error: {e}")
        time.sleep(SCAN_INTERVAL)

# ---------------- API ----------------
app = FastAPI(title="ForexFlow EightFilter", version="3.2.1")

class VolumeUpdate(BaseModel):
    future: str
    net_pct: float

class CotUpdate(BaseModel):
    future: str
    net: float

@app.on_event("startup")
def startup():
    db()
    logmsg("ForexFlow EightFilter v3.2.1 started (auto-COT, market names fixed)")
    threading.Thread(target=scanner_loop, daemon=True).start()

@app.get("/health")
def health():
    try:
        acct = get_account()
        return {"status": "ok", "version": "3.2.1", "env": OANDA_ENV,
                "oanda": "connected", "balance": acct["balance"],
                "auto_trade": AUTO_TRADE, "pairs": PAIRS, "live_pair": LIVE_PAIR}
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
    COT[c.future.upper()] = {"net": c.net, "ts": datetime.now(timezone.utc).isoformat(),
                             "source": "manual"}
    logmsg(f"COT updated {c.future.upper()}: net {c.net:+,.0f} (manual)")
    return {"ok": True, "cot": COT}

@app.post("/cot-refresh")
def cot_refresh():
    pulled = fetch_cot()
    COT_LAST_PULL[0] = time.time()
    return {"pulled": pulled, "cot": COT}

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

@app.get("/ledger")
def ledger(limit: int = 50):
    c = db()
    trades = [dict(zip(["id","ts_open","pair","direction","units","fill","sl","tp","confidence","status","ts_close","pnl"], r))
              for r in c.execute("SELECT id,ts_open,pair,direction,units,fill,sl,tp,confidence,status,ts_close,pnl FROM trades ORDER BY id DESC LIMIT ?", (limit,))]
    signals = [dict(zip(["id","ts","pair","direction","confidence","votes","executed","reason"], r))
               for r in c.execute("SELECT id,ts,pair,direction,confidence,votes,executed,reason FROM signals ORDER BY id DESC LIMIT ?", (limit,))]
    closed = [t for t in trades if t["status"] == "closed" and t["pnl"] is not None]
    c.close()
    grade_live = {"closed_trades": len(closed),
                  "total_pnl": round(sum(t["pnl"] for t in closed), 2),
                  "win_rate": round(sum(1 for t in closed if t["pnl"] > 0) / len(closed) * 100, 1) if closed else None}
    return {"grade": grade_live, "trades": trades, "signals": signals}

@app.get("/backtest/pnl")
def backtest_pnl(days: int = 50, risk_usd: float = 1000.0, core: str = "all",
                 gran: str = "M15", hold_bars: int = 96,
                 from_year: int = 0, to_year: int = 9999,
                 pair: str = ""):
    """Replay signal cores over real candles.
    core=trend|invert|revert|all. gran=M15|H1|H4|D.
    pair="" (default) tests all 7 pairs — research/comparison only.
    pair="USD_JPY" isolates the actual live engine. See /live-check.
    Volume/COT/FRED not backfillable. Spread = current live (constant)."""
    if pair and pair not in PAIR_MAP:
        return {"error": f"unknown pair; valid: {PAIRS}"}
    test_pairs = [pair] if pair else PAIRS

    bars_per_day = {"M15": 96, "H1": 24, "H4": 6, "D": 1}.get(gran, 96)
    bars = max(500, min(5000, int(days * bars_per_day)))
    cores = ["trend", "invert", "revert"] if core == "all" else [core]
    tape, spreads, errors = {}, {}, {}
    for p in test_pairs:
        try:
            tape[p] = get_candles(p, count=bars, gran=gran)
            spreads[p] = get_quote(p)["spread"]
            if from_year or to_year != 9999:
                tape[p] = [c for c in tape[p]
                           if from_year <= int(c["t"][:4]) <= to_year]
        except Exception as e:
            errors[p] = str(e)
    results = {}
    for c in cores:
        all_trades, per_pair = [], {}
        for p, candles in tape.items():
            tr = backtest_pair(p, candles, spreads[p], risk_usd, core=c,
                               hold_bars=hold_bars, skip_session=(gran == "D"))
            per_pair[p] = grade(tr, risk_usd)
            all_trades.extend(tr)
        results[c] = {"overall": grade(all_trades, risk_usd), "per_pair": per_pair}
    return {"version": "3.2.1-backtest", "bars_per_pair": bars, "cores": cores,
            "pair_filter": pair or "all_7_legacy_blend",
            "assumptions": {"risk_per_trade_usd": risk_usd,
                            "rr": "2:1 trend/invert, 1:1 revert",
                            "spread": "current live, held constant",
                            "overlays_not_backfilled": ["F3 volume", "F7 COT", "F8 FRED"],
                            "same_bar_sl_tp": "stop assumed first (conservative)"},
            "results": results, "errors": errors}

@app.get("/live-check")
def live_check(days: int = 1000, risk_usd: float = 1000.0):
    """Zero-config health check for the ACTUAL live engine: always tests
    LIVE_PAIR on daily bars with the trend core."""
    return backtest_pnl(days=days, risk_usd=risk_usd, core="trend",
                        gran="D", hold_bars=96, pair=LIVE_PAIR)

@app.post("/close/{pair}")
def close_position(pair: str):
    """Emergency flatten: close any open position in pair."""
    try:
        d = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT}/positions/{pair}")
        pos = d["position"]
        body = {"longUnits": "ALL", "shortUnits": "ALL"}
        r = requests.put(f"{BASE}/v3/accounts/{OANDA_ACCOUNT}/positions/{pair}/close",
                         headers=HEADERS, json=body, timeout=15)
        logmsg(f"MANUAL CLOSE {pair}: HTTP {r.status_code}")
        tg(f"🔴 MANUAL CLOSE {pair}: HTTP {r.status_code}")
        return r.json()
    except Exception as e:
        return {"error": str(e)}

@app.get("/logs")
def logs():
    return LOGS[-100:]

@app.get("/dashboard")
def dashboard():
    reset_daily()
    return {"version": "3.2.1", "auto_trade": AUTO_TRADE, "pairs": PAIRS, "live_pair": LIVE_PAIR,
            "daily": {"date": DAILY["date"], "trades": DAILY["trades"],
                      "pnl": round(daily_pnl(), 2), "lockdown": DAILY["lockdown"],
                      "traded_pairs": DAILY["traded_pairs"]},
            "volume": VOLUME, "cot": COT, "fred": FRED,
            "chop_skip_min": CHOP_SKIP_MIN, "spread_stop_mult": SPREAD_STOP_MULT,
            "recent_logs": LOGS[-20:]}
