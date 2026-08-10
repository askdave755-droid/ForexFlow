"""
ForexFlow v2.2 — OANDA Forex Auto-Trader (FIXED)
Fixes: FIFO open-position check | fill-counting only | one-pair-per-day | proper unit sizing
"""

import os
import logging
from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

import requests
import numpy as np
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger("forexflow-v2.2")

# ─── Config ───────────────────────────────────────────────────────────────────
OANDA_API_KEY     = os.getenv("OANDA_API_KEY", "")
OANDA_ACCOUNT_ID  = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_ENV         = os.getenv("OANDA_ENV", "practice").lower()
BASE_URL          = (
    "https://api-fxpractice.oanda.com"
    if OANDA_ENV == "practice"
    else "https://api-fxtrade.oanda.com"
)

RISK_PERCENT      = float(os.getenv("RISK_PERCENT", "0.01"))
MAX_DAILY_TRADES  = int(os.getenv("MAX_DAILY_TRADES", "3"))
MAX_UNITS         = int(os.getenv("MAX_UNITS", "2000000"))

PAIRS = os.getenv("PAIRS", "GBP_USD,EUR_USD,USD_JPY,AUD_USD,USD_CAD").split(",")

HEADERS = {
    "Authorization": f"Bearer {OANDA_API_KEY}",
    "Content-Type": "application/json"
}

# ─── Data Models ──────────────────────────────────────────────────────────────
class Direction(str, Enum):
    LONG  = "LONG"
    SHORT = "SHORT"
    NONE  = "NONE"

@dataclass
class Candle:
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass
class Signal:
    direction: Direction
    entry_price: float
    stop_price: float
    target_price: float
    confidence: float
    reason: str
    size: int = 0

@dataclass
class TradeState:
    daily_fill_count: int = 0
    traded_pairs_today: Set[str] = field(default_factory=set)
    last_reset_date: Optional[datetime] = None

    def reset_if_new_day(self):
        now = datetime.utcnow()
        if self.last_reset_date is None or now.date() != self.last_reset_date.date():
            logger.info("New day — resetting trade counters")
            self.daily_fill_count = 0
            self.traded_pairs_today.clear()
            self.last_reset_date = now

STATE = TradeState()

# ─── OANDA Client ─────────────────────────────────────────────────────────────
class OandaClient:
    def __init__(self):
        self.account_url = f"{BASE_URL}/v3/accounts/{OANDA_ACCOUNT_ID}"

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.account_url}/{endpoint}" if not endpoint.startswith("http") else endpoint
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"OANDA GET error: {e}")
            return {}

    def _post(self, endpoint: str, payload: dict) -> dict:
        url = f"{self.account_url}/{endpoint}"
        try:
            r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            return {
                "status_code": r.status_code,
                "json": r.json() if r.text else {},
                "text": r.text
            }
        except requests.RequestException as e:
            logger.error(f"OANDA POST error: {e}")
            return {"status_code": 0, "json": {}, "text": str(e)}

    def get_balance(self) -> float:
        data = self._get("summary")
        balance = data.get("account", {}).get("balance")
        return float(balance) if balance else 0.0

    def get_open_positions(self) -> List[dict]:
        data = self._get("openPositions")
        return data.get("positions", [])

    def has_open_position(self, pair: str) -> bool:
        for pos in self.get_open_positions():
            if pos.get("instrument") == pair:
                long_units = float(pos.get("long", {}).get("units", 0))
                short_units = float(pos.get("short", {}).get("units", 0))
                if long_units != 0 or short_units != 0:
                    return True
        return False

    def get_candles(self, pair: str, granularity: str = "M5", count: int = 50) -> List[Candle]:
        url = f"{BASE_URL}/v3/instruments/{pair}/candles"
        params = {"granularity": granularity, "count": count, "price": "M"}
        data = self._get(url, params)
        candles = []
        for c in data.get("candles", []):
            if not c.get("complete"):
                continue
            mid = c["mid"]
            candles.append(Candle(
                time=c["time"],
                open=float(mid["o"]),
                high=float(mid["h"]),
                low=float(mid["l"]),
                close=float(mid["c"]),
                volume=c["volume"]
            ))
        return candles

    def place_market_order(self, pair: str, units: int, stop_loss: float, take_profit: float) -> dict:
        payload = {
            "order": {
                "type": "MARKET",
                "instrument": pair,
                "units": str(units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "stopLossOnFill": {
                    "price": f"{stop_loss:.5f}",
                    "timeInForce": "GTC"
                },
                "takeProfitOnFill": {
                    "price": f"{take_profit:.5f}",
                    "timeInForce": "GTC"
                }
            }
        }
        return self._post("orders", payload)

# ─── SixFilter Engine ─────────────────────────────────────────────────────────
class SixFilterEngine:
    def __init__(self):
        self.lookback = 50

    def _ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        alpha = 2.0 / (period + 1)
        ema = np.zeros_like(prices)
        ema[0] = prices[0]
        for i in range(1, len(prices)):
            ema[i] = alpha * prices[i] + (1 - alpha) * ema[i - 1]
        return ema

    def _atr(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
        if len(highs) < period + 1:
            return 0.0
        tr_list = []
        for i in range(1, period + 1):
            idx = -i
            tr1 = highs[idx] - lows[idx]
            tr2 = abs(highs[idx] - closes[idx - 1])
            tr3 = abs(lows[idx] - closes[idx - 1])
            tr_list.append(max(tr1, tr2, tr3))
        return float(np.mean(tr_list))

    def _rsi(self, closes: np.ndarray, period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _vwap(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, volumes: np.ndarray) -> float:
        tp = (highs + lows + closes) / 3.0
        return float(np.sum(tp * volumes) / np.sum(volumes))

    def analyze(self, candles: List[Candle]) -> Signal:
        if len(candles) < 20:
            return Signal(Direction.NONE, 0, 0, 0, 0, "Insufficient data")

        closes  = np.array([c.close for c in candles])
        highs   = np.array([c.high for c in candles])
        lows    = np.array([c.low for c in candles])
        volumes = np.array([c.volume for c in candles], dtype=float)

        current_price = closes[-1]
        ema20 = self._ema(closes, 20)[-1]
        atr   = self._atr(highs, lows, closes, 14)
        rsi   = self._rsi(closes, 14)
        vwap  = self._vwap(highs, lows, closes, volumes)

        deviation = (current_price - vwap) / vwap if vwap != 0 else 0
        lmsr_pass = abs(deviation) > 0.0005

        trend_up   = current_price > ema20 and closes[-5] < ema20
        trend_down = current_price < ema20 and closes[-5] > ema20

        if atr == 0:
            return Signal(Direction.NONE, 0, 0, 0, 0, "No ATR")
        stop_distance = atr * 1.5
        target_distance = atr * 3.0
        ev_pass = target_distance >= stop_distance * 2

        div_bear = highs[-1] > highs[-5] and rsi < self._rsi(closes[:-5])
        div_bull = lows[-1] < lows[-5] and rsi > self._rsi(closes[:-5])

        hour = datetime.utcnow().hour
        time_pass = hour not in [20, 21, 22, 23]

        stoikov_level = (ema20 + vwap) / 2.0
        stoikov_pass = abs(current_price - stoikov_level) < atr * 0.5

        long_ok = lmsr_pass and trend_up and ev_pass and div_bull and time_pass and stoikov_pass
        short_ok = lmsr_pass and trend_down and ev_pass and div_bear and time_pass and stoikov_pass

        if not (long_ok or short_ok):
            return Signal(Direction.NONE, 0, 0, 0, 0,
                f"Filters: LMSR={lmsr_pass}, TREND={'UP' if trend_up else 'DOWN' if trend_down else 'NONE'}, EV={ev_pass}, TIME={time_pass}")

        direction = Direction.LONG if long_ok else Direction.SHORT
        entry = current_price
        if direction == Direction.LONG:
            stop = entry - stop_distance
            target = entry + target_distance
        else:
            stop = entry + stop_distance
            target = entry - target_distance

        confidence = 70.0
        if lmsr_pass: confidence += 5
        if ev_pass: confidence += 10
        if time_pass: confidence += 5
        confidence = min(confidence, 95.0)

        return Signal(direction, entry, stop, target, confidence,
            f"SixFilter aligned | ATR={atr:.5f} | RSI={rsi:.1f} | VWAP={vwap:.5f}")

# ─── Risk Manager ─────────────────────────────────────────────────────────────
class RiskManager:
    def __init__(self, client: OandaClient):
        self.client = client

    def calculate_units(self, pair: str, entry: float, stop: float) -> int:
        balance = self.client.get_balance()
        if balance <= 0:
            logger.warning("Balance zero or unavailable")
            return 0

        risk_amount = balance * RISK_PERCENT
        stop_pips = abs(entry - stop)
        if stop_pips == 0:
            return 0

        units = int(risk_amount / (stop_pips * 10))

        if units > MAX_UNITS:
            logger.warning(
                f"Calculated units ({units:,}) exceeds MAX_UNITS ({MAX_UNITS:,}). Capping."
            )
            units = MAX_UNITS

        if units <= 0:
            return 0

        actual_risk = units * stop_pips * 10
        logger.info(
            f"Risk calc | Balance: ${balance:,.2f} | Intended: ${risk_amount:,.2f} "
            f"| Units: {units:,} | Actual: ${actual_risk:,.2f} "
            f"({actual_risk/balance*100:.3f}%)"
        )
        return units

# ─── FastAPI App ─────────────────────────────────────────────────────────────
app = FastAPI(title="ForexFlow v2.2", version="2.2.0")

client   = OandaClient()
engine   = SixFilterEngine()
risk_mgr = RiskManager(client)

class TradeRequest(BaseModel):
    pair: Optional[str] = None
    direction: Optional[str] = None

class ScanResult(BaseModel):
    pair: str
    signal: str
    confidence: float
    reason: str
    executed: bool = False
    error: Optional[str] = None

def is_trade_hours() -> bool:
    now = datetime.utcnow()
    if now.weekday() >= 5:
        return False
    if now.hour in [20, 21, 22, 23]:
        return False
    return True

@app.get("/health")
def health():
    balance = client.get_balance()
    return {
        "status": "ok",
        "version": "2.2.0",
        "env": OANDA_ENV,
        "balance": balance,
        "max_units": MAX_UNITS,
        "risk_percent": RISK_PERCENT,
        "max_daily_trades": MAX_DAILY_TRADES,
        "pairs": PAIRS
    }

@app.get("/status")
def status():
    STATE.reset_if_new_day()
    return {
        "daily_fills": STATE.daily_fill_count,
        "max_daily": MAX_DAILY_TRADES,
        "traded_pairs_today": list(STATE.traded_pairs_today),
        "open_positions": [p["instrument"] for p in client.get_open_positions()]
    }

@app.post("/scan")
def scan(req: TradeRequest = None) -> List[ScanResult]:
    STATE.reset_if_new_day()
    pairs = [req.pair] if req and req.pair else PAIRS
    results: List[ScanResult] = []

    for pair in pairs:
        candles = client.get_candles(pair, "M5", 50)
        signal = engine.analyze(candles)
        results.append(ScanResult(
            pair=pair,
            signal=signal.direction.value,
            confidence=signal.confidence,
            reason=signal.reason,
            executed=False
        ))
    return results

@app.post("/trade")
def trade(req: TradeRequest = None) -> List[ScanResult]:
    STATE.reset_if_new_day()
    results: List[ScanResult] = []

    if not is_trade_hours():
        return [ScanResult(pair="ALL", signal="NONE", confidence=0,
                         reason="Outside trade hours", executed=False)]

    if STATE.daily_fill_count >= MAX_DAILY_TRADES:
        return [ScanResult(pair="ALL", signal="NONE", confidence=0,
                         reason=f"Daily fill cap: {STATE.daily_fill_count}/{MAX_DAILY_TRADES}",
                         executed=False)]

    pairs = [req.pair] if req and req.pair else PAIRS

    for pair in pairs:
        if client.has_open_position(pair):
            results.append(ScanResult(
                pair=pair, signal="BLOCKED", confidence=0,
                reason="FIFO: Open position exists", executed=False
            ))
            continue

        if pair in STATE.traded_pairs_today:
            results.append(ScanResult(
                pair=pair, signal="BLOCKED", confidence=0,
                reason="Pair already traded today", executed=False
            ))
            continue

        candles = client.get_candles(pair, "M5", 50)
        signal = engine.analyze(candles)

        if signal.direction == Direction.NONE:
            results.append(ScanResult(
                pair=pair,
                signal=signal.direction.value,
                confidence=signal.confidence,
                reason=signal.reason,
                executed=False
            ))
            continue

        units = risk_mgr.calculate_units(pair, signal.entry_price, signal.stop_price)
        if units <= 0:
            results.append(ScanResult(
                pair=pair, signal=signal.direction.value,
                confidence=signal.confidence, reason="Zero units",
                executed=False, error="Zero units"
            ))
            continue

        response = client.place_market_order(
            pair, units, signal.stop_price, signal.target_price
        )

        order_fill = response.get("json", {}).get("orderFillTransaction")
        status_code = response.get("status_code", 0)

        if status_code == 201 and order_fill:
            STATE.daily_fill_count += 1
            STATE.traded_pairs_today.add(pair)
            fill_price = order_fill.get("price", signal.entry_price)
            logger.info(f"FILL | {pair} | {signal.direction.value} | {units:,} @ {fill_price}")
            results.append(ScanResult(
                pair=pair, signal=signal.direction.value,
                confidence=signal.confidence, reason=signal.reason,
                executed=True
            ))
        else:
            reject_reason = "Unknown"
            order_reject = response.get("json", {}).get("orderRejectTransaction")
            if order_reject:
                reject_reason = order_reject.get("rejectReason", "Unknown")
            elif response.get("text"):
                reject_reason = response["text"][:100]
            logger.warning(f"REJECTED | {pair} | {reject_reason} — NOT counted")
            results.append(ScanResult(
                pair=pair, signal=signal.direction.value,
                confidence=signal.confidence, reason=signal.reason,
                executed=False, error=f"REJECTED: {reject_reason}"
            ))

    return results

@app.post("/trade-manual")
def trade_manual(req: TradeRequest) -> ScanResult:
    STATE.reset_if_new_day()
    pair = req.pair
    if not pair:
        raise HTTPException(400, "pair required")

    if client.has_open_position(pair):
        return ScanResult(pair=pair, signal="BLOCKED", confidence=0,
                         reason="FIFO: Open position exists", executed=False)

    candles = client.get_candles(pair, "M5", 50)
    signal = engine.analyze(candles)

    if req.direction:
        signal.direction = Direction(req.direction.upper())

    units = risk_mgr.calculate_units(pair, signal.entry_price, signal.stop_price)
    if units <= 0:
        return ScanResult(pair=pair, signal=signal.direction.value,
                         confidence=0, reason="Zero units", executed=False)

    response = client.place_market_order(pair, units, signal.stop_price, signal.target_price)
    order_fill = response.get("json", {}).get("orderFillTransaction")

    if response.get("status_code") == 201 and order_fill:
        STATE.daily_fill_count += 1
        STATE.traded_pairs_today.add(pair)
        return ScanResult(pair=pair, signal=signal.direction.value,
                         confidence=signal.confidence, reason="Manual override",
                         executed=True)
    else:
        reject = response.get("json", {}).get("orderRejectTransaction", {})
        return ScanResult(pair=pair, signal=signal.direction.value,
                         confidence=0, reason="Manual failed",
                         executed=False, error=reject.get("rejectReason", "Unknown"))

@app.on_event("startup")
async def startup():
    logger.info("ForexFlow v2.2 started")
    logger.info(f"Env: {OANDA_ENV} | Max Units: {MAX_UNITS:,} | Risk: {RISK_PERCENT*100}%")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
