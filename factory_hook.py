"""
factory_hook.py — optional report-only integration with the trading-factory
Postgres ledger.

ForexFlow dual-writes its trading activity to the factory database WITHOUT
touching the execution path. If FACTORY_DATABASE_URL is unset or psycopg2 is
not installed, every function below is a silent no-op (returns None/False)
and the bot runs identically to today.

DEPENDENCY: psycopg2-binary must be added to requirements.txt (NOT done in
this PR on purpose — add it when enabling the integration):
    psycopg2-binary>=2.9

CALL SITES in main.py (all fire-and-forget, AFTER the fact, never in the
order path's critical section):

1. report_signal(...) — in execute_signal(sig), immediately AFTER
   place_market_order() returns res["ok"] == True and ledger_trade_open()
   has been recorded. At that point you have:
       pair, sig["direction"], res["fill"], sig["sl"], sig["tp"], sz["units"]
   Example:
       report_signal(pair, pair, sig["direction"], res["fill"],
                     sig["sl"], sig["tp"], sz["units"])
   (config_name = the pair, e.g. "EUR_USD", matching its strategy_configs row.)
   Do NOT call this before/between order submission — the fill must already
   exist so the factory never blocks or delays an order.

2. report_fill(...) — in track_open_trades(), right after
   ledger_trade_close(tid, pnl), once the realized pnl has been fetched from
   OANDA. At that point you have: pair, pnl. Example:
       report_fill(pair, pair, pnl)
   This also upserts today's daily_perf row (trades+1, wins+1 if pnl>0,
   pnl+=pnl) in the factory.

3. heartbeat(config_name) — optional: call once at startup (e.g. in the
   FastAPI `startup()` handler) per LIVE_PAIRS entry to verify each config
   exists in the factory:
       for p in LIVE_PAIRS:
           heartbeat(p)

SAFETY: every function is synchronous, wrapped in try/except, and NEVER
raises. The factory is strictly read-only with respect to execution — it
receives reports; it never sends orders or modifies ForexFlow state.
"""

import os
from typing import Optional

FACTORY_DATABASE_URL = os.getenv("FACTORY_DATABASE_URL", "")

try:
    import psycopg2
    import psycopg2.extras  # noqa: F401
except ImportError:  # psycopg2-binary not installed -> full no-op mode
    psycopg2 = None


def _connect():
    """Return a factory DB connection, or None if integration is disabled.
    Never raises."""
    try:
        if not FACTORY_DATABASE_URL or psycopg2 is None:
            return None
        return psycopg2.connect(FACTORY_DATABASE_URL, connect_timeout=5)
    except Exception:
        return None


def _config_id(cur, config_name):
    cur.execute("SELECT id FROM strategy_configs WHERE name = %s", (config_name,))
    row = cur.fetchone()
    return row[0] if row else None


def report_signal(config_name: str, symbol: str, direction: str,
                  entry_price: float, stop_price: float, target_price: float,
                  size: float) -> Optional[str]:
    """Insert a live signal into the factory. Returns signal id or None.
    Silent no-op if disabled, config missing, or any error occurs."""
    try:
        conn = _connect()
        if conn is None:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cid = _config_id(cur, config_name)
                    if cid is None:
                        return None
                    cur.execute(
                        """INSERT INTO signals
                           (config_id, mode, symbol, direction, entry_price,
                            stop_price, target_price, size)
                           VALUES (%s, 'live', %s, %s, %s, %s, %s, %s)
                           RETURNING id""",
                        (cid, symbol, direction, entry_price, stop_price,
                         target_price, size),
                    )
                    return str(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception:
        return None


def report_fill(config_name: str, symbol: str, pnl: float,
                signal_id: Optional[str] = None) -> bool:
    """Insert a live fill and upsert today's daily_perf row.
    execution_grade 'A' if linked to a signal, else 'B'; brier_score NULL.
    Returns True on success, False otherwise. Never raises."""
    try:
        conn = _connect()
        if conn is None:
            return False
        try:
            with conn:
                with conn.cursor() as cur:
                    cid = _config_id(cur, config_name)
                    if cid is None:
                        return False
                    cur.execute(
                        """INSERT INTO fills
                           (signal_id, config_id, mode, pnl, brier_score,
                            execution_grade)
                           VALUES (%s, %s, 'live', %s, NULL, %s)""",
                        (signal_id, cid, pnl, 'A' if signal_id else 'B'),
                    )
                    cur.execute(
                        """INSERT INTO daily_perf (config_id, date, trades, wins, pnl)
                           VALUES (%s, CURRENT_DATE, 1, %s, %s)
                           ON CONFLICT (config_id, date) DO UPDATE SET
                               trades = daily_perf.trades + 1,
                               wins   = daily_perf.wins + EXCLUDED.wins,
                               pnl    = daily_perf.pnl + EXCLUDED.pnl""",
                        (cid, 1 if pnl > 0 else 0, pnl),
                    )
                    return True
        finally:
            conn.close()
    except Exception:
        return False


def heartbeat(config_name: str) -> bool:
    """Verify a config exists in strategy_configs. Never raises."""
    try:
        conn = _connect()
        if conn is None:
            return False
        try:
            with conn.cursor() as cur:
                return _config_id(cur, config_name) is not None
        finally:
            conn.close()
    except Exception:
        return False
