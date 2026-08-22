# ForexFlow × Trading-Factory Integration

This repo now includes `factory_hook.py`, an optional **report-only** bridge
that dual-writes ForexFlow's trading activity to the trading-factory Postgres
ledger. Execution logic is untouched: the factory never sends orders, never
modifies ForexFlow state, and never sits in the order path.

## Call sites (main.py)

| Hook | Where in main.py | When |
|------|------------------|------|
| `report_signal(config_name, symbol, direction, entry_price, stop_price, target_price, size)` | `execute_signal(sig)`, right after `place_market_order()` returns `res["ok"]` and `ledger_trade_open()` records the trade | Fire-and-forget, **after** the fill exists — never before or between order submission |
| `report_fill(config_name, symbol, pnl, signal_id=None)` | `track_open_trades()`, immediately after `ledger_trade_close(tid, pnl)` once realized P&L is known | Also upserts today's `daily_perf` row (trades+1, wins+1 if pnl>0, pnl+=pnl) |
| `heartbeat(config_name)` | optionally in the FastAPI `startup()` handler, once per pair in `LIVE_PAIRS` | Verifies each config exists in `strategy_configs` |

All functions are synchronous, internally `try/except`-guarded, and **never
raise**. If the integration is disabled they return `None`/`False` and the bot
runs byte-for-byte identically to before.

## Setup (Railway)

1. Add dependency: `psycopg2-binary>=2.9` in `requirements.txt`
   (intentionally **not** added in this PR).
2. Set the environment variable on the ForexFlow service:
   ```
   FACTORY_DATABASE_URL = <the factory's Postgres public URL>
   ```
   e.g. `postgresql://user:pass@host.railway.app:5432/factory`
3. Redeploy. With the var unset, the hook is a no-op.

## Per-pair configs

ForexFlow trades per pair (`LIVE_PAIRS = ["USD_JPY", "EUR_USD"]`). Register
**each pair as its own config** in the factory's `strategy_configs` table,
named after the pair (e.g. `USD_JPY`, `EUR_USD`), so the factory ledger
distinguishes their signals, fills, and daily performance. Callers pass the
pair as `config_name`; unknown names are skipped silently.

## Schema expected in the factory DB

- `strategy_configs(id uuid, name text)`
- `signals(id uuid default gen_random_uuid(), config_id uuid, mode text,
  symbol text, direction text, entry_price float, stop_price float,
  target_price float, size float, model_prob float, market_price float,
  created_at timestamptz default now())`
- `fills(id uuid default gen_random_uuid(), signal_id uuid, config_id uuid,
  mode text, pnl float, brier_score float, execution_grade char(1),
  grade_notes text, filled_at timestamptz default now())`
- `daily_perf(id uuid default gen_random_uuid(), config_id uuid, date date,
  trades int, wins int, pnl float, UNIQUE(config_id, date))`

All inserts use `mode = 'live'`; `brier_score` is left NULL;
`execution_grade` is `'A'` when linked to a signal, `'B'` otherwise.

## Read-only guarantee

The factory **only receives reports**. ForexFlow never reads orders,
signals-to-act-on, or any control data from the factory database. Worst case
of a factory outage: a few missed report rows — trading is unaffected.
