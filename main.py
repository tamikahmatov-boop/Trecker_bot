"""
Crypto Alert Bot — v17
Только Bybit бессрочные USDT-контракты (Linear Perpetual).
Цены и свечи исключительно с Bybit V5 API. MEXC и OKX удалены.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import signal
import sqlite3
import time
from contextlib import contextmanager
from typing import Optional

import aiohttp
try:
    from aiohttp_socks import ProxyConnector
    PROXY_AVAILABLE = True
except ImportError:
    PROXY_AVAILABLE = False
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange

import config

# ── Prometheus (опционально) ──────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    PROM_SIGNALS   = Counter("bot_signals_total",   "Всего сигналов")
    PROM_CHECKS    = Counter("bot_checks_total",    "Всего циклов")
    PROM_COINS     = Gauge("bot_tracked_coins",     "Монет в истории")
    PROM_REVERSALS = Counter("bot_reversals_total", "Разворотных сигналов")
    PROM_AVAILABLE = True
except ImportError:
    PROM_AVAILABLE = False

# ================================================================
#  LOGGING
# ================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ================================================================
#  DATABASE
# ================================================================

DB_FILE = "alerts.db"


@contextmanager
def db_connect():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_init():
    with db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                price     REAL    NOT NULL,
                growth    REAL    NOT NULL,
                rsi       REAL,
                macd      REAL,
                source    TEXT,
                ts        REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol);
            CREATE INDEX IF NOT EXISTS idx_alerts_ts     ON alerts(ts);

            CREATE TABLE IF NOT EXISTS alert_levels (
                symbol       TEXT    PRIMARY KEY,
                alert_price  REAL    NOT NULL,
                direction    INTEGER NOT NULL,
                ts           REAL    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS price_stats (
                symbol   TEXT PRIMARY KEY,
                high24h  REAL,
                low24h   REAL,
                vol24h   REAL,
                updated  REAL
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id  INTEGER PRIMARY KEY,
                added_ts REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reversal_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                price       REAL    NOT NULL,
                score       INTEGER NOT NULL,
                factors     TEXT    NOT NULL,
                rsi         REAL,
                macd        REAL,
                stoch_rsi   REAL,
                bb_pct      REAL,
                atr         REAL,
                target1     REAL,
                target2     REAL,
                source      TEXT,
                ts          REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rev_symbol ON reversal_signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_rev_ts     ON reversal_signals(ts);
        """)


db_init()

DB_KEEP_ALERTS_DAYS  = getattr(config, "DB_KEEP_ALERTS_DAYS",  30)
DB_KEEP_LEVELS_DAYS  = getattr(config, "DB_KEEP_LEVELS_DAYS",   7)
DB_VACUUM_INTERVAL_H = getattr(config, "DB_VACUUM_INTERVAL_H", 24)


def db_save_alert(symbol, price, growth, rsi, macd, source):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, macd, source, ts) VALUES (?,?,?,?,?,?,?)",
            (symbol, price, growth, rsi, macd, source, time.time()),
        )


def db_save_reversal(symbol, price, score, factors, rsi, macd,
                     stoch_rsi, bb_pct, atr, target1, target2, source):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO reversal_signals "
            "(symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, atr, target1, target2, source, ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, price, score, json.dumps(factors, ensure_ascii=False),
             rsi, macd, stoch_rsi, bb_pct, atr, target1, target2, source, time.time()),
        )


def db_recent_reversals(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, "
            "atr, target1, target2, source, ts "
            "FROM reversal_signals ORDER BY ts DESC LIMIT ?", (limit,),
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["factors"] = json.loads(d["factors"])
        except Exception:
            d["factors"] = []
        result.append(d)
    return result


def db_get_alert_level(symbol):
    with db_connect() as conn:
        row = conn.execute(
            "SELECT alert_price, direction, ts FROM alert_levels WHERE symbol=?", (symbol,)
        ).fetchone()
    return dict(row) if row else None


def db_set_alert_level(symbol, alert_price, direction):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO alert_levels (symbol, alert_price, direction, ts) VALUES (?,?,?,?)",
            (symbol, alert_price, direction, time.time()),
        )


def db_clear_alert_levels():
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels")


def db_clear_alert_level(symbol):
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels WHERE symbol=?", (symbol,))


def db_cleanup() -> dict:
    now = time.time()
    deleted = {}
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM alerts WHERE ts < ?", (now - DB_KEEP_ALERTS_DAYS * 86400,))
        deleted["alerts"] = cur.rowcount
        cur = conn.execute("DELETE FROM alert_levels WHERE ts < ?", (now - DB_KEEP_LEVELS_DAYS * 86400,))
        deleted["alert_levels"] = cur.rowcount
        cur = conn.execute("DELETE FROM price_stats WHERE updated < ?", (now - 86400,))
        deleted["price_stats"] = cur.rowcount
        cur = conn.execute("DELETE FROM reversal_signals WHERE ts < ?", (now - 14 * 86400,))
        deleted["reversal_signals"] = cur.rowcount
    return deleted


def db_vacuum():
    with db_connect() as conn:
        conn.execute("VACUUM")


def db_size_mb() -> float:
    try:
        return os.path.getsize(DB_FILE) / 1_048_576
    except OSError:
        return 0.0


def db_stats() -> dict:
    with db_connect() as conn:
        alerts_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        oldest       = conn.execute("SELECT MIN(ts) FROM alerts").fetchone()[0]
        levels_count = conn.execute("SELECT COUNT(*) FROM alert_levels").fetchone()[0]
        rev_count    = conn.execute("SELECT COUNT(*) FROM reversal_signals").fetchone()[0]
    return {
        "alerts":       alerts_count,
        "oldest_alert": time.strftime("%d.%m.%Y", time.localtime(oldest)) if oldest else "—",
        "levels":       levels_count,
        "reversals":    rev_count,
        "size_mb":      db_size_mb(),
    }


def db_recent_alerts(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, growth, rsi, macd, source, ts FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def db_top_signals(limit: int = 5) -> list[dict]:
    cutoff = time.time() - 86400
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, growth, price, source, ts FROM alerts "
            "WHERE ts >= ? ORDER BY ABS(growth) DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def db_export_csv(limit: int = 500) -> str:
    rows = db_recent_alerts(limit)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["ts", "symbol", "price", "growth", "rsi", "macd", "source"])
    writer.writeheader()
    for r in rows:
        r["ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
        writer.writerow(r)
    return buf.getvalue()


# ── Подписчики ────────────────────────────────────────────────────────────────

def db_add_subscriber(chat_id: int):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id, added_ts) VALUES (?,?)",
            (chat_id, time.time()),
        )


def db_remove_subscriber(chat_id: int):
    with db_connect() as conn:
        conn.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))


def db_get_subscribers() -> list[int]:
    with db_connect() as conn:
        rows = conn.execute("SELECT chat_id FROM subscribers").fetchall()
    ids = {r["chat_id"] for r in rows}
    ids.add(int(config.CHAT_ID))
    return list(ids)


# ================================================================
#  STATE
# ================================================================

STATE_FILE = "state.json"
last_alert_growth: dict = {}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"last_alert_growth": last_alert_growth}, f)
    except Exception:
        pass


def load_state():
    global last_alert_growth
    try:
        with open(STATE_FILE) as f:
            last_alert_growth = json.load(f).get("last_alert_growth", {})
    except Exception:
        pass


load_state()

# ================================================================
#  GLOBALS
# ================================================================

TOKEN = config.BOT_TOKEN
TG    = f"https://api.telegram.org/bot{TOKEN}"

offset           = 0
price_history:   dict  = {}
signals_count    = 0
reversal_count   = 0
checks_count     = 0
start_time       = time.time()
last_check_time: float = 0.0

current_percent: float = config.PERCENT
current_window:  int   = config.WINDOW

monitor_paused = False
monitor_task: asyncio.Task | None = None

_session: aiohttp.ClientSession | None = None

_alert_cooldown:    dict[str, float] = {}
_reversal_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SEC:    int = getattr(config, "ALERT_COOLDOWN_SEC",    60)
REVERSAL_COOLDOWN_SEC: int = getattr(config, "REVERSAL_COOLDOWN_SEC", 1800)  # 30 мин по умолчанию

# Умный фильтр повторных разворотных сигналов по одной монете:
#   - цена должна измениться минимум на REVERSAL_REPEAT_PRICE_PCT% от прошлого сигнала
#   - или скор должен вырасти минимум на REVERSAL_REPEAT_SCORE_DELTA
_reversal_last: dict[str, dict] = {}   # {sym: {price, score, ts}}
REVERSAL_REPEAT_PRICE_PCT:  float = getattr(config, "REVERSAL_REPEAT_PRICE_PCT",  3.0)   # 3% смены цены
REVERSAL_REPEAT_SCORE_DELTA: int  = getattr(config, "REVERSAL_REPEAT_SCORE_DELTA", 2)    # +2 фактора

_levels_cache: dict[str, dict] = {}

# ── Настройки детектора разворота (все изменяемы через Telegram) ──────────────
REVERSAL_MIN_SCORE:   int   = getattr(config, "REVERSAL_MIN_SCORE",   4)      # из 12
REVERSAL_RSI_OB:      float = getattr(config, "REVERSAL_RSI_OB",      70.0)
REVERSAL_STOCH_OB:    float = getattr(config, "REVERSAL_STOCH_OB",    0.80)
REVERSAL_STOCH_EXT:   float = getattr(config, "REVERSAL_STOCH_EXT",   0.85)
REVERSAL_BB_OB:       float = getattr(config, "REVERSAL_BB_OB",       1.0)
REVERSAL_MACD_SLOPE:  float = getattr(config, "REVERSAL_MACD_SLOPE",  -0.000005)
REVERSAL_ACCEL:       float = getattr(config, "REVERSAL_ACCEL",       0.5)
REVERSAL_HIGH_MARGIN: float = getattr(config, "REVERSAL_HIGH_MARGIN", 0.998)
REVERSAL_MOMENTUM:    float = getattr(config, "REVERSAL_MOMENTUM",    -0.5)
REVERSAL_ATR_MULT:    float = getattr(config, "REVERSAL_ATR_MULT",    3.0)    # ATR перегрев
REVERSAL_VOL_RATIO:   float = getattr(config, "REVERSAL_VOL_RATIO",   0.7)    # объём < 70% средн.

# Минимальный % роста за период для проверки разворота на шорт (кнопки 1/5/10/15/25%)
REVERSAL_GROWTH_MIN_PCT: float = getattr(config, "REVERSAL_GROWTH_MIN_PCT", 5.0)

# Период для расчёта пикового роста разворота (кнопки 5м/30м/1ч/4ч/1д)
REVERSAL_WINDOW_SEC: int = getattr(config, "REVERSAL_WINDOW_SEC", 3600)  # по умолчанию 1 час

# Порог уведомлений о росте/падении (задаётся через /notify_pct или автоматически = current_percent)
NOTIFY_BIG_MOVE_PCT: float = getattr(config, "NOTIFY_BIG_MOVE_PCT", 15.0)
_notify_cooldown: dict[str, float] = {}   # {sym: last_sent_ts}
NOTIFY_BIG_MOVE_COOLDOWN_SEC: int = 3600  # не чаще 1 раза в час


def _cache_load_levels():
    global _levels_cache
    with db_connect() as conn:
        rows = conn.execute("SELECT symbol, alert_price, direction FROM alert_levels").fetchall()
    _levels_cache = {r["symbol"]: {"alert_price": r["alert_price"], "direction": r["direction"]} for r in rows}
    log.info("Загружено уровней в кэш: %d", len(_levels_cache))


def _cache_get_level(symbol):
    return _levels_cache.get(symbol)


def _cache_set_level(symbol, alert_price, direction):
    _levels_cache[symbol] = {"alert_price": alert_price, "direction": direction}
    db_set_alert_level(symbol, alert_price, direction)


def _cache_clear_level(symbol):
    _levels_cache.pop(symbol, None)
    db_clear_alert_level(symbol)


def _cache_clear_all():
    _levels_cache.clear()
    db_clear_alert_levels()


def normalize_symbol(sym: str) -> str:
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")


# ================================================================
#  BYBIT KLINE CACHE  (цена + объём)
# ================================================================

_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL  = 60
KLINE_LIMIT_1M   = 120   # 120 свечей по 1 мин = 2 часа
KLINE_LIMIT_5M   = 100   # 100 свечей по 5 мин = 8+ часов
KLINE_LIMIT_15M  = 60    # 60 свечей по 15 мин = 15 часов
KLINE_LIMIT = KLINE_LIMIT_1M

# Bybit: интервалы в минутах строкой
_BYBIT_INTERVAL = {"Min1": "1", "Min5": "5", "Min15": "15"}


async def _fetch_bybit_klines(symbol: str, interval: str = "Min1", limit: int = KLINE_LIMIT_1M) -> dict:
    """Возвращает dict с ключами: opens, closes, highs, lows, volumes (все list[float]).
    Bybit V5 kline: GET /v5/market/kline
    interval: "1" | "5" | "15" | "60" | "D" и т.д.
    Ответ: list[[ startTime, open, high, low, close, volume, turnover ]]
    Данные идут от новых к старым — разворачиваем.
    """
    empty = {"opens": [], "closes": [], "highs": [], "lows": [], "volumes": []}
    bybit_iv = _BYBIT_INTERVAL.get(interval, "1")
    try:
        async with _session.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category": "linear", "symbol": symbol,
                    "interval": bybit_iv, "limit": limit},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = await r.json()
        if data.get("retCode") == 0:
            rows = data["result"]["list"]
            rows = list(reversed(rows))   # от старых к новым
            return {
                "opens":   [float(c[1]) for c in rows],
                "highs":   [float(c[2]) for c in rows],
                "lows":    [float(c[3]) for c in rows],
                "closes":  [float(c[4]) for c in rows],
                "volumes": [float(c[5]) for c in rows],
            }
    except Exception as e:
        log.debug("Bybit kline %s %s: %s", symbol, interval, e)
    return empty


async def get_bybit_klines(symbol: str, interval: str = "Min1", limit: int = KLINE_LIMIT_1M) -> dict:
    cache_key = f"{symbol}:{interval}:{limit}"
    now       = time.time()
    cached    = _kline_cache.get(cache_key)
    if cached and now - cached["ts"] < KLINE_CACHE_TTL:
        return cached["data"]
    data = await _fetch_bybit_klines(symbol, interval, limit)
    if data["closes"]:
        _kline_cache[cache_key] = {"ts": now, "data": data}
    if len(_kline_cache) > 3000:
        stale = [k for k, v in _kline_cache.items() if now - v["ts"] > KLINE_CACHE_TTL * 5]
        for k in stale:
            _kline_cache.pop(k, None)
    return data



# ================================================================
#  INDICATORS
# ================================================================

MIN_SAMPLES = 10


def calculate_rsi(prices: list[float], window: int = 14) -> Optional[float]:
    try:
        if len(prices) < window + 1:
            return None
        s   = pd.Series(prices)
        val = RSIIndicator(close=s, window=window).rsi().iloc[-1]
        return None if pd.isna(val) else round(float(val), 2)
    except Exception as e:
        log.error("RSI error: %s", e)
        return None


def calculate_stoch_rsi(prices: list[float], window: int = 14) -> Optional[float]:
    """StochRSI: позиция RSI в его собственном диапазоне. >0.8 = перекупленность."""
    try:
        if len(prices) < window * 2 + 1:
            return None
        s          = pd.Series(prices)
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < window:
            return None
        rsi_min = rsi_series.rolling(window).min().iloc[-1]
        rsi_max = rsi_series.rolling(window).max().iloc[-1]
        if pd.isna(rsi_min) or pd.isna(rsi_max) or (rsi_max - rsi_min) == 0:
            return None
        return round(float((rsi_series.iloc[-1] - rsi_min) / (rsi_max - rsi_min)), 4)
    except Exception as e:
        log.error("StochRSI error: %s", e)
        return None


def calculate_bollinger_pct(prices: list[float], window: int = 20) -> Optional[float]:
    """%B Боллинджера. >1.0 = выше верхней полосы."""
    try:
        if len(prices) < window:
            return None
        s     = pd.Series(prices)
        bb    = BollingerBands(close=s, window=window, window_dev=2)
        upper = bb.bollinger_hband().iloc[-1]
        lower = bb.bollinger_lband().iloc[-1]
        if pd.isna(upper) or pd.isna(lower) or (upper - lower) == 0:
            return None
        return round(float((prices[-1] - lower) / (upper - lower)), 4)
    except Exception as e:
        log.error("BB error: %s", e)
        return None


def calculate_macd_full(prices: list[float]) -> dict:
    result = {"histogram": None, "histogram_prev": None, "slope": None, "cross_down": False}
    try:
        if len(prices) < 30:
            return result
        s    = pd.Series(prices)
        hist = MACD(close=s).macd_diff().dropna()
        if len(hist) < 4:
            return result
        result["histogram"]      = round(float(hist.iloc[-1]), 6)
        result["histogram_prev"] = round(float(hist.iloc[-2]), 6)
        result["slope"]          = round(float(hist.iloc[-1] - hist.iloc[-3]), 6)
        result["cross_down"]     = bool(hist.iloc[-2] > 0 and hist.iloc[-1] < 0)
    except Exception as e:
        log.error("MACD full error: %s", e)
    return result


def calculate_ema_cross(prices: list[float], fast: int = 9, slow: int = 21) -> dict:
    """
    EMA крест: если EMA9 только что пересекла EMA21 вниз — медвежий сигнал.
    Возвращает: cross_down (bool), ema_fast, ema_slow, gap_pct (разрыв в %).
    """
    result = {"cross_down": False, "ema_fast": None, "ema_slow": None, "gap_pct": None}
    try:
        if len(prices) < slow + 2:
            return result
        s        = pd.Series(prices)
        ema_fast = EMAIndicator(close=s, window=fast).ema_indicator()
        ema_slow = EMAIndicator(close=s, window=slow).ema_indicator()
        ef_now   = float(ema_fast.iloc[-1])
        ef_prev  = float(ema_fast.iloc[-2])
        es_now   = float(ema_slow.iloc[-1])
        es_prev  = float(ema_slow.iloc[-2])
        # Крест вниз: до — fast > slow, после — fast < slow
        result["cross_down"] = bool(ef_prev >= es_prev and ef_now < es_now)
        result["ema_fast"]   = round(ef_now, 6)
        result["ema_slow"]   = round(es_now, 6)
        result["gap_pct"]    = round((ef_now - es_now) / es_now * 100, 3)
    except Exception as e:
        log.error("EMA cross error: %s", e)
    return result


def calculate_atr(highs: list[float], lows: list[float],
                  closes: list[float], window: int = 14) -> Optional[float]:
    """ATR — средний истинный диапазон. Мера волатильности."""
    try:
        if len(closes) < window + 1:
            return None
        s_h = pd.Series(highs)
        s_l = pd.Series(lows)
        s_c = pd.Series(closes)
        atr = AverageTrueRange(high=s_h, low=s_l, close=s_c, window=window).average_true_range()
        val = atr.iloc[-1]
        return None if pd.isna(val) else round(float(val), 8)
    except Exception as e:
        log.error("ATR error: %s", e)
        return None


def detect_candle_pattern(closes: list[float], highs: list[float],
                           lows: list[float],
                           opens: list[float] | None = None) -> Optional[str]:
    """
    Медвежий свечной паттерн на последних 2 барах.
    opens — реальные цены открытия из Bybit kline.
    Если не переданы, аппроксимируем prev_close.
      - Shooting Star: длинная верхняя тень >2× тела, тело внизу, закрытие < открытие
      - Доджи: тело < 10% диапазона — нерешительность
      - Медвежье поглощение: красная свеча полностью поглощает предыдущую зелёную
      - Вечерняя звезда (3 бара): бычья → доджи/малое тело → красная
    """
    try:
        if len(closes) < 3 or len(highs) < 3 or len(lows) < 3:
            return None

        # Реальные opens из Bybit или аппроксимация
        if opens and len(opens) >= 3:
            o_prev2 = opens[-3]
            o_prev  = opens[-2]
            o_cur   = opens[-1]
        else:
            o_prev2 = closes[-4] if len(closes) >= 4 else closes[-3]
            o_prev  = closes[-3]
            o_cur   = closes[-2]

        c_prev2 = closes[-3]
        c_prev  = closes[-2]
        c_cur   = closes[-1]
        h_cur   = highs[-1]
        l_cur   = lows[-1]
        h_prev  = highs[-2]

        body_cur  = abs(c_cur  - o_cur)
        body_prev = abs(c_prev - o_prev)
        rng_cur   = h_cur - l_cur
        if rng_cur == 0:
            return None

        upper_wick = h_cur - max(c_cur, o_cur)
        lower_wick = min(c_cur, o_cur) - l_cur

        # Shooting Star: длинная верхняя тень >2× тела, нижняя тень < тела, медвежья
        if (body_cur > 0
                and upper_wick > 2 * body_cur
                and lower_wick < body_cur * 0.5
                and c_cur < o_cur):
            return "Shooting Star 🌠"

        # Доджи: тело < 10% диапазона (нерешительность на пике)
        if body_cur < rng_cur * 0.1:
            return "Доджи ✝️"

        # Медвежье поглощение: пред. зелёная, текущая красная и больше
        if (c_prev > o_prev         # предыдущая — зелёная
                and c_cur < o_cur   # текущая — красная
                and o_cur >= c_prev # открытие ≥ закрытие пред.
                and c_cur <= o_prev # закрытие ≤ открытие пред.
                and body_cur > body_prev):
            return "Медвежье поглощение 🐻"

        # Вечерняя звезда (3 бара): бычья → малое тело/доджи → красная
        body_prev2 = abs(c_prev2 - o_prev2)
        if (c_prev2 > o_prev2              # 1й бар — зелёный бычий
                and body_prev < body_prev2 * 0.3  # 2й бар — малое тело
                and c_cur < o_cur          # 3й бар — красный медвежий
                and c_cur < (o_prev2 + c_prev2) / 2):  # закрытие ниже середины 1го бара
            return "Вечерняя звезда 🌆"

    except Exception as e:
        log.error("Candle pattern error: %s", e)
    return None


def calculate_fibonacci_levels(high: float, low: float) -> dict:
    """
    Уровни Фибоначчи от локального хая к лою (зоны коррекции/цели).
    При развороте вниз цели: 0.236, 0.382, 0.5, 0.618 от текущего хая.
    """
    diff = high - low
    return {
        "fib_236": round(high - diff * 0.236, 8),
        "fib_382": round(high - diff * 0.382, 8),
        "fib_500": round(high - diff * 0.500, 8),
        "fib_618": round(high - diff * 0.618, 8),
        "fib_786": round(high - diff * 0.786, 8),
    }


def calculate_rsi_divergence(prices: list[float], window: int = 14,
                              lookback: int = 10) -> bool:
    """Медвежья дивергенция: цена = новый хай, RSI = ниже предыдущего пика."""
    try:
        if len(prices) < window + lookback + 1:
            return False
        s          = pd.Series(prices)
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < lookback:
            return False
        price_now  = prices[-1]
        price_prev = max(prices[-(lookback + 1):-1])
        rsi_now    = float(rsi_series.iloc[-1])
        rsi_prev   = float(rsi_series.iloc[-(lookback + 1):].max())
        return (price_now > price_prev) and (rsi_now < rsi_prev - 2)
    except Exception:
        return False


def calculate_price_momentum(prices: list[float],
                              fast: int = 5, slow: int = 20) -> Optional[float]:
    """Разница быстрого и медленного моментума. < 0 = иссякание."""
    try:
        if len(prices) < slow + 1:
            return None
        mom_fast = (prices[-1] - prices[-fast]) / prices[-fast] * 100
        mom_slow = (prices[-1] - prices[-slow]) / prices[-slow] * 100
        return round(mom_fast - mom_slow, 4)
    except Exception:
        return None


def calculate_volume_weakness(volumes: list[float]) -> Optional[str]:
    """
    Реальный объём из свечей Bybit.
    Если последний бар < REVERSAL_VOL_RATIO от среднего — сигнал слабости роста.
    """
    try:
        if len(volumes) < 20:
            return None
        avg_vol  = sum(volumes[-20:-1]) / 19
        last_vol = volumes[-1]
        if avg_vol == 0:
            return None
        ratio = last_vol / avg_vol
        if ratio < REVERSAL_VOL_RATIO:
            return f"📊 Объём слабый ({ratio:.1%} от среднего)"
        return None
    except Exception:
        return None


def calculate_volume_signal(sym: str) -> Optional[str]:
    """Fallback: анализ иссякания по тикам цены (если нет данных свечей)."""
    hist = price_history.get(sym, [])
    now  = time.time()
    if len(hist) < 20:
        return None
    hour_prices = [p for t, p in hist if now - t <= 3600]
    if len(hour_prices) < 10:
        return None
    mid   = len(hour_prices) // 2
    move1 = abs(hour_prices[mid - 1] - hour_prices[0])   / hour_prices[0]   * 100
    move2 = abs(hour_prices[-1]      - hour_prices[mid])  / hour_prices[mid] * 100
    if move1 > 0.5 and move2 < move1 * 0.3:
        return "📊 Движение без импульса (иссякание)"
    return None



def calculate_rsi_trend(prices: list[float], window: int = 14) -> Optional[str]:
    try:
        if len(prices) < window + 3:
            return None
        s   = pd.Series(prices)
        rsi = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi) < 3:
            return None
        delta = rsi.iloc[-1] - rsi.iloc[-3]
        if delta > 1:
            return "▲"
        if delta < -1:
            return "▼"
        return "—"
    except Exception:
        return None


def calculate_acceleration(recent: list[tuple[float, float]]) -> Optional[float]:
    try:
        if len(recent) < 6:
            return None
        mid    = len(recent) // 2
        half1  = recent[:mid]
        half2  = recent[mid:]
        speed1 = (half1[-1][1] - half1[0][1]) / half1[0][1] * 100 / max(half1[-1][0] - half1[0][0], 1)
        speed2 = (half2[-1][1] - half2[0][1]) / half2[0][1] * 100 / max(half2[-1][0] - half2[0][0], 1)
        if speed1 == 0:
            return None
        return round(speed2 / abs(speed1), 2)
    except Exception:
        return None


def calculate_growth_duration(recent: list[tuple[float, float]]) -> int:
    if len(recent) < 2:
        return 0
    return int(recent[-1][0] - recent[0][0])


def check_24h_breakout(sym: str, price: float) -> Optional[str]:
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if len(day_prices) < 20:
        return None
    high24 = max(day_prices[:-1])
    low24  = min(day_prices[:-1])
    if price > high24:
        return f"🔺 пробой хая 24h ({high24:.4g})"
    if price < low24:
        return f"🔻 пробой лоя 24h ({low24:.4g})"
    return None


def get_24h_context(sym: str, price: float) -> Optional[str]:
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if len(day_prices) < 20:
        return None
    high24    = max(day_prices)
    low24     = min(day_prices)
    dist_high = (price - high24) / high24 * 100
    dist_low  = (price - low24)  / low24  * 100
    return f"📉 от хая: {dist_high:+.1f}%  📈 от лоя: {dist_low:+.1f}%"


def get_peak_growth(sym: str, price: float, window_sec: int = 3600) -> float:
    """
    Максимальный рост за window_sec от минимума к текущей цене.
    Используется для разворотного детектора: монета могла уже откатить,
    но пиковый рост сохраняется в истории.
    """
    hist = price_history.get(sym, [])
    now  = time.time()
    window_prices = [p for t, p in hist if now - t <= window_sec]
    if not window_prices:
        return 0.0
    low = min(window_prices)
    if low <= 0:
        return 0.0
    return (price - low) / low * 100


# Обратная совместимость
def get_peak_growth_24h(sym: str, price: float) -> float:
    return get_peak_growth(sym, price, window_sec=86400)


def calculate_obv_divergence(closes: list[float], volumes: list[float],
                              lookback: int = 15) -> bool:
    """
    OBV-дивергенция: цена растёт, On-Balance Volume падает.
    Надёжный признак ослабления покупательского давления.
    """
    try:
        if len(closes) < lookback + 1 or len(volumes) < lookback + 1:
            return False
        # Считаем OBV
        obv = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]:
                obv.append(obv[-1] + volumes[i])
            elif closes[i] < closes[i - 1]:
                obv.append(obv[-1] - volumes[i])
            else:
                obv.append(obv[-1])
        # Цена: новый хай за lookback
        price_rising = closes[-1] > max(closes[-(lookback + 1):-1])
        # OBV: падает (текущий < среднего за lookback)
        obv_now  = obv[-1]
        obv_prev = sum(obv[-(lookback + 1):-1]) / lookback
        obv_falling = obv_now < obv_prev * 0.97  # OBV упал на 3%+
        return price_rising and obv_falling
    except Exception:
        return False


def calculate_wick_rejection(highs: list[float], lows: list[float],
                              opens: list[float], closes: list[float],
                              lookback: int = 3) -> Optional[float]:
    """
    Wick Rejection Ratio: отношение верхней тени к полному диапазону.
    >0.6 = сильное отталкивание от верхней зоны (медвежий признак).
    Усредняется за последние lookback свечей.
    """
    try:
        n = min(lookback, len(closes))
        if n < 1:
            return None
        ratios = []
        for i in range(-n, 0):
            rng = highs[i] - lows[i]
            if rng == 0:
                continue
            upper_wick = highs[i] - max(opens[i], closes[i])
            ratios.append(upper_wick / rng)
        return round(sum(ratios) / len(ratios), 3) if ratios else None
    except Exception:
        return None


def calculate_lower_highs(highs: list[float], window: int = 5) -> bool:
    """
    Паттерн убывающих максимумов (Lower Highs) на последних window свечах.
    3+ последовательных снижения хая = структурный разворот.
    """
    try:
        if len(highs) < window + 1:
            return False
        recent = highs[-window:]
        # Считаем сколько раз хай ниже предыдущего
        lower_count = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i - 1])
        return lower_count >= window - 2  # минимум window-2 убывания из window-1 пар
    except Exception:
        return False


def calculate_rsi_higher_tf(prices_15m: list[float], window: int = 14) -> Optional[float]:
    """
    RSI на 15m свечах — аппроксимация «старшего таймфрейма».
    Перекупленность >75 на 15m при уже высоком 1m RSI — сильный сигнал.
    """
    return calculate_rsi(prices_15m, window=window)


# ================================================================
#  REVERSAL DETECTOR  — 16 факторов
# ================================================================

def _esc(s: str) -> str:
    """Экранирование HTML-символов для Telegram parse_mode=HTML."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def detect_short_reversal(
    sym:        str,
    price:      float,
    klines_1m:  dict,   # Min1  x120 — RSI, StochRSI, BB, паттерны, wick
    klines_5m:  dict,   # Min5  x100 — ATR, EMA, MACD, объём, моментум, lower highs
    klines_15m: dict,   # Min15 x60  — OBV-дивергенция, RSI старшего TF
    recent:     list[tuple[float, float]],
    growth:     float,
    rsi:        Optional[float],
    min_score:  int = 4,
) -> dict:
    """
    16-факторный детектор разворота на шорт.

    Min1  (краткосрочные): RSI, StochRSI, BB, паттерн, отбой от хая, Wick Rejection
    Min5  (среднесрочные): MACD, EMA, ATR, моментум, Lower Highs, объём
    Min15 (долгосрочные):  OBV-дивергенция, RSI-дивергенция, RSI старшего TF
    Тики (акселерация):    замедление импульса

    Факторы (+1 каждый):
      1.  RSI перекупленность (Min1 > RSI_OB)
      2.  StochRSI зона перекупленности (Min1)
      3.  Цена выше верхней полосы Боллинджера %B>BB_OB (Min1)
      4.  MACD медвежий крест или убывающий slope (Min5)
      5.  RSI-дивергенция: цена↑, RSI↓ (Min15)
      6.  Замедление импульса accel < ACCEL (тики)
      7.  Отбой от 24h High с подтверждением свечи (Min1)
      8.  Моментум иссякает fast-slow < MOMENTUM (Min5)
      9.  EMA9 пробила EMA21 вниз или ниже EMA21 (Min5)
      10. ATR-перегрев: движение > ATR_MULT × ATR (Min5, 1 час)
      11. Медвежий свечной паттерн (Min1, реальные opens)
      12. Слабый объём при росте vol < VOL_RATIO (Min5 → Min1 → тики)
      13. OBV-дивергенция: цена↑, OBV↓ (Min15)
      14. Wick Rejection Ratio > 0.55 (Min1, 3 последних свечи)
      15. RSI старшего TF > 75 (Min15)
      16. Lower Highs паттерн (Min5, 5 свечей)
    """
    score   = 0
    factors: list[str] = []

    # Распаковываем данные по таймфреймам
    closes_1m   = klines_1m.get("closes",  [])
    highs_1m    = klines_1m.get("highs",   [])
    lows_1m     = klines_1m.get("lows",    [])
    opens_1m    = klines_1m.get("opens",   [])
    volumes_1m  = klines_1m.get("volumes", [])

    closes_5m   = klines_5m.get("closes",  [])
    highs_5m    = klines_5m.get("highs",   [])
    lows_5m     = klines_5m.get("lows",    [])
    volumes_5m  = klines_5m.get("volumes", [])

    closes_15m  = klines_15m.get("closes",  [])
    volumes_15m = klines_15m.get("volumes", [])

    # Fallback на тики если нет свечей
    prices_1m  = closes_1m  if len(closes_1m)  >= 20 else [p for _, p in price_history.get(sym, [])[-120:]]
    prices_5m  = closes_5m  if len(closes_5m)  >= 20 else prices_1m
    prices_15m = closes_15m if len(closes_15m) >= 20 else prices_5m

    # ── 1. RSI перекупленность (Min1) ──────────────────────────────────────────
    stoch_rsi_val = None
    if rsi is not None and rsi > REVERSAL_RSI_OB:
        score += 1
        factors.append(f"RSI перекуплен ({rsi:.1f} &gt; {REVERSAL_RSI_OB}) Min1")

    # ── 2. StochRSI (Min1) ─────────────────────────────────────────────────────
    stoch_rsi_val = calculate_stoch_rsi(prices_1m)
    if stoch_rsi_val is not None:
        thr = REVERSAL_STOCH_OB if (rsi is not None and rsi > REVERSAL_RSI_OB) else REVERSAL_STOCH_EXT
        if stoch_rsi_val > thr:
            score += 1
            factors.append(f"StochRSI перекупленность ({stoch_rsi_val:.2f} &gt; {thr}) Min1")

    # ── 3. Боллинджер %B (Min1) ────────────────────────────────────────────────
    bb_pct = calculate_bollinger_pct(prices_1m)
    if bb_pct is not None and bb_pct > REVERSAL_BB_OB:
        score += 1
        factors.append(f"Цена выше верхней BB %B={bb_pct:.2f} Min1")

    # ── 4. MACD (Min5) ─────────────────────────────────────────────────────────
    macd_data = calculate_macd_full(prices_5m)
    macd_hist = macd_data["histogram"]
    if macd_data["cross_down"]:
        score += 1
        factors.append("MACD медвежий крест (hist 0→-) Min5")
    elif (macd_hist is not None
          and macd_data["slope"] is not None
          and macd_hist > 0
          and macd_data["slope"] < REVERSAL_MACD_SLOPE):
        score += 1
        factors.append(f"MACD убывает Min5 (slope={macd_data['slope']:+.6f})")

    # ── 5. RSI-дивергенция (Min15 — более надёжный сигнал) ────────────────────
    if len(prices_15m) >= 30 and calculate_rsi_divergence(prices_15m):
        score += 1
        factors.append("RSI-дивергенция Min15 (цена↑, RSI↓)")
    elif len(prices_5m) >= 30 and calculate_rsi_divergence(prices_5m):
        score += 1
        factors.append("RSI-дивергенция Min5 (цена↑, RSI↓)")

    # ── 6. Замедление импульса (тики) ─────────────────────────────────────────
    accel = calculate_acceleration(recent)
    if accel is not None and growth >= current_percent and accel < REVERSAL_ACCEL:
        score += 1
        factors.append(f"Замедление импульса (accel={accel:.2f}x)")

    # ── 7. Отбой от 24h High (Min1 с реальными opens) ────────────────────────
    hist_data  = price_history.get(sym, [])
    now_ts     = time.time()
    day_prices = [p for t, p in hist_data if now_ts - t <= 86400]
    high24 = max(day_prices) if day_prices else price
    low24  = min(day_prices) if day_prices else price
    if len(day_prices) >= 20:
        near_high = price >= high24 * REVERSAL_HIGH_MARGIN
        if near_high:
            bearish = False
            if len(closes_1m) >= 2 and len(opens_1m) >= 1:
                # Реальная красная свеча у хая (close < open из Bybit)
                bearish = (opens_1m[-1] > closes_1m[-1]  # медвежья свеча
                           and highs_1m[-1] >= high24 * REVERSAL_HIGH_MARGIN)
            if not bearish and len(recent) >= 4:
                ticks = [p for _, p in recent[-4:]]
                falling = sum(ticks[i] > ticks[i + 1] for i in range(len(ticks) - 1))
                bearish = falling >= 2
            if bearish:
                pct = (price - high24) / high24 * 100
                score += 1
                factors.append(f"Отбой от 24h High ({pct:+.2f}%) Min1")

    # ── 8. Моментум иссякает (Min5) ───────────────────────────────────────────
    momentum = calculate_price_momentum(prices_5m)
    if momentum is not None and momentum < REVERSAL_MOMENTUM:
        score += 1
        factors.append(f"Моментум иссякает Min5 (diff={momentum:+.2f}%)")

    # ── 9. EMA крест / ниже EMA (Min5) ────────────────────────────────────────
    ema_data = calculate_ema_cross(prices_5m)
    if ema_data["cross_down"]:
        score += 1
        factors.append(
            f"EMA9 пробила EMA21 вниз Min5 "
            f"({ema_data['ema_fast']:.4g} &lt; {ema_data['ema_slow']:.4g})"
        )
    elif ema_data.get("gap_pct") is not None and ema_data["gap_pct"] < -0.05:
        score += 1
        factors.append(f"EMA9 ниже EMA21 Min5 (gap={ema_data['gap_pct']:+.2f}%)")

    # ── 10. ATR-перегрев (Min5, последний час) ────────────────────────────────
    atr = None
    if len(highs_5m) >= 15 and len(lows_5m) >= 15 and len(closes_5m) >= 15:
        atr = calculate_atr(highs_5m, lows_5m, closes_5m)
        if atr is not None and atr > 0:
            lookback    = min(12, len(closes_5m) - 1)
            period_move = abs(price - closes_5m[-lookback])
            atr_ratio   = period_move / atr
            if atr_ratio > REVERSAL_ATR_MULT:
                score += 1
                factors.append(f"ATR-перегрев Min5 ({atr_ratio:.1f}× ATR={atr:.4g})")

    # ── 11. Свечной паттерн (Min1, реальные opens из Bybit) ───────────────────
    candle_pattern = None
    if len(closes_1m) >= 4 and len(highs_1m) >= 4 and len(lows_1m) >= 4:
        candle_pattern = detect_candle_pattern(
            closes_1m, highs_1m, lows_1m,
            opens_1m if len(opens_1m) >= 4 else None,
        )
        if candle_pattern:
            score += 1
            factors.append(f"Паттерн Min1: {candle_pattern}")

    # ── 12. Слабый объём (Min5 → Min1 → тики) ────────────────────────────────
    vol_signal: Optional[str] = None
    if len(volumes_5m) >= 20:
        vol_signal = calculate_volume_weakness(volumes_5m)
    elif len(volumes_1m) >= 20:
        vol_signal = calculate_volume_weakness(volumes_1m)
    else:
        vol_signal = calculate_volume_signal(sym)
    if vol_signal:
        score += 1
        factors.append(vol_signal)

    # ── 13. OBV-дивергенция (Min15) ───────────────────────────────────────────
    if calculate_obv_divergence(prices_15m, volumes_15m if len(volumes_15m) >= 16
                                else volumes_5m):
        score += 1
        factors.append("OBV-дивергенция Min15 (цена↑, OBV↓)")

    # ── 14. Wick Rejection Ratio > 0.55 (Min1) ───────────────────────────────
    wick_ratio = None
    if len(closes_1m) >= 3 and len(opens_1m) >= 3:
        wick_ratio = calculate_wick_rejection(highs_1m, lows_1m, opens_1m, closes_1m, lookback=3)
        if wick_ratio is not None and wick_ratio > 0.55:
            score += 1
            factors.append(f"Wick Rejection {wick_ratio:.2f} (верхняя тень доминирует) Min1")

    # ── 15. RSI старшего TF > 75 (Min15) ─────────────────────────────────────
    rsi_15m = calculate_rsi_higher_tf(prices_15m) if len(prices_15m) >= 16 else None
    if rsi_15m is not None and rsi_15m > 75.0:
        score += 1
        factors.append(f"RSI перекуплен на Min15 ({rsi_15m:.1f} &gt; 75)")

    # ── 16. Lower Highs паттерн (Min5) ────────────────────────────────────────
    if len(highs_5m) >= 6 and calculate_lower_highs(highs_5m, window=5):
        score += 1
        factors.append("Lower Highs паттерн Min5 (убывающие максимумы)")

    # ── Цели по шорту: уровни Фибоначчи от 24h High к Low ────────────────────
    fib     = calculate_fibonacci_levels(high24, low24)
    target1 = fib["fib_382"]
    target2 = fib["fib_618"]

    day_context = get_24h_context(sym, price)

    return {
        "score":          score,
        "max_score":      16,
        "factors":        factors,
        "triggered":      score >= min_score,
        "rsi":            rsi,
        "rsi_15m":        rsi_15m,
        "stoch_rsi":      stoch_rsi_val,
        "bb_pct":         bb_pct,
        "macd_hist":      macd_hist,
        "accel":          accel,
        "atr":            atr,
        "wick_ratio":     wick_ratio,
        "target1":        target1,
        "target2":        target2,
        "fib":            fib,
        "high24":         high24,
        "low24":          low24,
        "candle_pattern": candle_pattern,
        "ema_gap":        ema_data.get("gap_pct"),
        "vol_signal":     vol_signal,
        "day_context":    day_context,
    }


# ================================================================
#  TELEGRAM HELPERS
# ================================================================

async def _tg_post(method: str, payload: dict, retries: int = 3) -> Optional[dict]:
    for attempt in range(retries):
        try:
            async with _session.post(
                f"{TG}/{method}", json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    log.warning("[_tg_post] Telegram %s error: %s", method, data)
                return data
        except asyncio.TimeoutError:
            log.warning("[_tg_post] timeout %s (попытка %d/%d)", method, attempt + 1, retries)
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s
        except Exception as e:
            log.error("[_tg_post] %s: %s", method, e)
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return None


async def send_message(text: str, chat_id, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    await _tg_post("sendMessage", payload)


async def send_document(chat_id, filename: str, content: str, caption: str = ""):
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("document", content.encode(), filename=filename, content_type="text/csv")
        async with _session.post(f"{TG}/sendDocument", data=form,
                                  timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json()
    except Exception as e:
        log.error("sendDocument: %s", e)


async def broadcast(text: str, reply_markup=None):
    tasks = [send_message(text, cid, reply_markup) for cid in db_get_subscribers()]
    await asyncio.gather(*tasks, return_exceptions=True)


def reply_keyboard():
    return {
        "keyboard": [
            # Строка 1 — Управление мониторингом
            ["⏸ Пауза", "▶️ Продолжить", "📊 Статус"],
            # Строка 2 — Порог роста/падения для алертов (включая 15%)
            ["📈 0.2%", "📈 5%", "📈 10%", "📈 15%", "📈 20%"],
            # Строка 3 — Временное окно
            ["⏱ 5 мин", "⏱ 1 час", "⏱ 4 ч", "⏱ 1 д"],
            # Строка 4 — Сигналы и история
            ["📋 История", "🏆 Топ-5", "📤 Экспорт"],
            # Строка 5 — Разворот + кулдауны
            ["🔄 Развороты", "⚙️ Настройки разворота", "🗑 Кулдауны"],
            # Строка 6 — Порог разворота (быстрая настройка)
            ["🎚 Порог 3/12", "🎚 Порог 4/12", "🎚 Порог 5/12", "🎚 Порог 7/12"],
            # Строка 7 — Процент роста для разворотных сигналов в шорт
            ["📉 Разворот 1%", "📉 Разворот 5%", "📉 Разворот 10%", "📉 Разворот 15%", "📉 Разворот 25%"],
            # Строка 8 — Период окна для расчёта роста разворота
            ["🕐 Разворот 5м", "🕐 Разворот 30м", "🕐 Разворот 1ч", "🕐 Разворот 4ч", "🕐 Разворот 1д"],
        ],
        "resize_keyboard": True,
        "persistent":      True,
    }


# ================================================================
#  ALERT FORMATTING
# ================================================================

def _fmt_dur(sec: int) -> str:
    if sec >= 3600:
        return f"{sec // 3600}ч {(sec % 3600) // 60}м"
    if sec >= 60:
        return f"{sec // 60}м"
    return f"{sec}с"


def alert_emoji(growth: float) -> str:
    a = abs(growth)
    if a >= 20:
        return "💥"
    if a >= 10:
        return "🔥"
    return "🚀" if growth > 0 else "📉"


def format_growth_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    accel: Optional[float]   = None,
    duration_sec: int        = 0,
    breakout: Optional[str]  = None,
    day_context: Optional[str] = None,
) -> str:
    emoji = alert_emoji(growth)
    sign  = "+" if growth > 0 else ""
    label = "Рост" if growth > 0 else "Падение"

    rsi_s    = f"{rsi:.1f}" if rsi is not None else "—"
    rsi_hint = (" ⚠️ перекуплен" if rsi is not None and rsi >= 70
                else " ⚠️ перепродан" if rsi is not None and rsi <= 30
                else "")
    rsi_t    = f" {rsi_trend}" if rsi_trend else ""
    macd_s   = f"{macd:+.6f}" if macd is not None else "—"

    accel_s = ""
    if accel is not None:
        if accel >= 2.0:
            accel_s = f"\n⚡ Ускорение: <b>×{accel:.1f}</b> 🔥"
        elif accel >= 1.3:
            accel_s = f"\n⚡ Ускорение: ×{accel:.1f}"
        elif accel < 0.7:
            accel_s = f"\n🐢 Замедление: ×{accel:.1f}"

    return (
        f"{emoji} <b>СИГНАЛ — {label.upper()}</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 {label}: <b>{sign}{growth:.2f}%</b> за {_fmt_dur(duration_sec)}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_t}{rsi_hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{accel_s}"
        f"{chr(10) + breakout if breakout else ''}"
        f"{chr(10) + day_context if day_context else ''}"
        f"\n\n📋 <code>{sym}</code>"
    )


format_alert = format_growth_alert  # обратная совместимость


def format_drop_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    duration_sec: int        = 0,
    breakout: Optional[str]  = None,
    day_context: Optional[str] = None,
) -> str:
    a     = abs(growth)
    emoji = "💥" if a >= 20 else "🔥" if a >= 10 else "📉"
    rsi_s = f"{rsi:.1f}" if rsi is not None else "—"
    hint  = " ⚠️ перепродан" if (rsi is not None and rsi <= 30) else ""
    rsi_t = f" {rsi_trend}" if rsi_trend else ""
    macd_s = f"{macd:+.6f}" if macd is not None else "—"

    return (
        f"{emoji} <b>ПАДЕНИЕ</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📉 Падение: <b>{growth:.2f}%</b> за {_fmt_dur(duration_sec)}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_t}{hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{chr(10) + breakout if breakout else ''}"
        f"{chr(10) + day_context if day_context else ''}"
        f"\n\n📋 <code>{sym}</code> #падение"
    )


def format_reversal_alert(
    sym:          str,
    price:        float,
    growth:       float,
    source:       str,
    rev:          dict,
    duration_sec: int = 0,
    window_sec:   int = 3600,
) -> str:
    score      = rev["score"]
    max_score  = rev.get("max_score", 16)
    factors    = rev["factors"]
    rsi        = rev.get("rsi")
    rsi_15m    = rev.get("rsi_15m")
    stoch      = rev.get("stoch_rsi")
    bb_pct     = rev.get("bb_pct")
    macd_h     = rev.get("macd_hist")
    atr        = rev.get("atr")
    target1    = rev.get("target1")
    target2    = rev.get("target2")
    candle     = rev.get("candle_pattern")
    ema_gap    = rev.get("ema_gap")
    wick_ratio = rev.get("wick_ratio")
    vol_sig    = rev.get("vol_signal", "")
    day_ctx    = rev.get("day_context", "")

    if score >= 10:
        confidence, hdr = "🔴 ВЫСОКАЯ", "🚨"
    elif score >= 6:
        confidence, hdr = "🟠 СРЕДНЯЯ", "⚠️"
    else:
        confidence, hdr = "🟡 СЛАБАЯ", "🔄"

    rsi_s    = f"{rsi:.1f}"       if rsi        is not None else "—"
    rsi15_s  = f"{rsi_15m:.1f}"   if rsi_15m    is not None else "—"
    stoch_s  = f"{stoch:.2f}"     if stoch      is not None else "—"
    bb_s     = f"{bb_pct:.2f}"    if bb_pct     is not None else "—"
    macd_s   = f"{macd_h:+.6f}"   if macd_h     is not None else "—"
    atr_s    = f"{atr:.6f}"       if atr        is not None else "—"
    ema_s    = f"{ema_gap:+.2f}%" if ema_gap    is not None else "—"
    wick_s   = f"{wick_ratio:.2f}" if wick_ratio is not None else "—"

    factors_s = "\n".join(f"  {i+1}. {_esc(f)}" for i, f in enumerate(factors)) if factors else "  —"

    t1_s      = f"<code>{target1:.4g}</code>" if target1 else "—"
    t2_s      = f"<code>{target2:.4g}</code>" if target2 else "—"
    fib       = rev.get("fib", {})
    t3_s      = f"<code>{fib['fib_500']:.4g}</code>" if fib.get("fib_500") else "—"

    candle_s  = f"\n🕯 Паттерн: <b>{candle}</b>" if candle  else ""
    vol_s     = f"\n{_esc(vol_sig)}"              if vol_sig else ""
    day_ctx_s = f"\n{day_ctx}"                    if day_ctx else ""

    return (
        f"{hdr} <b>РАЗВОРОТ НА ШОРТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 Рост до разворота: <b>+{growth:.2f}%</b> за {_fmt_dur(window_sec)}\n\n"
        f"🎯 Уверенность: {confidence}  [{score}/{max_score}]\n\n"
        f"<b>Сработавшие факторы ({score}/{max_score}):</b>\n{factors_s}\n\n"
        f"<b>Индикаторы:</b>\n"
        f"  RSI 1m: <code>{rsi_s}</code>  │  RSI 15m: <code>{rsi15_s}</code>\n"
        f"  StochRSI: <code>{stoch_s}</code>  │  BB%: <code>{bb_s}</code>\n"
        f"  MACD: <code>{macd_s}</code>  │  ATR: <code>{atr_s}</code>\n"
        f"  EMA gap: <code>{ema_s}</code>  │  Wick: <code>{wick_s}</code>"
        f"{candle_s}"
        f"{vol_s}"
        f"{day_ctx_s}\n\n"
        f"<b>🎯 Цели шорта (Фибо):</b>\n"
        f"  Цель 1 (38.2%): {t1_s}\n"
        f"  Цель 2 (50.0%): {t3_s}\n"
        f"  Цель 3 (61.8%): {t2_s}"
    )


# ================================================================
#  SYMBOLS  — только Bybit Linear Perpetual USDT
# ================================================================

async def get_symbols() -> set[str]:
    """Загружает все бессрочные USDT-контракты с Bybit V5 API."""
    symbols: set[str] = set()
    try:
        async with _session.get(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "linear", "limit": 1000},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            data = await r.json()
        if data.get("retCode") == 0:
            for item in data["result"]["list"]:
                sym = item.get("symbol", "")
                # Только бессрочные USDT-контракты (не квартальные)
                if (sym.endswith("USDT")
                        and item.get("contractType") == "LinearPerpetual"
                        and item.get("status") == "Trading"):
                    symbols.add(sym)
        log.info("Bybit Linear Perpetual USDT: %d монет", len(symbols))
    except Exception as e:
        log.error("get_symbols Bybit: %s", e)
    return symbols


# ================================================================
#  BYBIT WEBSOCKET — цены и свечи в реальном времени
# ================================================================
#
#  Архитектура:
#    ws_ticker_loop  — подписка на tickers всех монет → price_history
#    ws_kline_loop   — подписка на kline.1 / kline.5 / kline.15 → _kline_store
#    monitor         — читает из памяти, не делает HTTP-запросов
#
#  При старте: REST-запрос для cold-start свечей (заполняем историю).
#  WS-соединение одно на все монеты через batch-подписки по 500 символов.
# ================================================================

BYBIT_WS_PUBLIC  = "wss://stream.bybit.com/v5/public/linear"
WS_RECONNECT_SEC = 5
WS_PING_INTERVAL = 20   # Bybit требует ping каждые 20 сек

# ── Хранилище свечей (заполняется WS) ────────────────────────────
# _kline_store[sym][interval] = {"opens":[], "closes":[], "highs":[], "lows":[], "volumes":[]}
_kline_store: dict[str, dict[str, dict]] = {}
_kline_store_lock: asyncio.Lock | None = None   # инициализируется в main()

# Символы, на которые уже подписаны WS
_ws_subscribed_syms: set[str] = set()

# Флаги готовности — инициализируются в main() внутри event loop
_ws_tickers_ready: asyncio.Event | None = None
_ws_klines_ready:  asyncio.Event | None = None


def _kline_empty() -> dict:
    return {"opens": [], "closes": [], "highs": [], "lows": [], "volumes": []}


def _kline_get(sym: str, interval: str) -> dict:
    return _kline_store.get(sym, {}).get(interval, _kline_empty())


def _kline_update(sym: str, interval: str, candle: list):
    """Обновляет или добавляет последнюю свечу в хранилище."""
    store = _kline_store.setdefault(sym, {})
    buf   = store.setdefault(interval, _kline_empty())
    o, h, l, c, v = (float(candle[i]) for i in (1, 2, 3, 4, 5))
    # Последняя свеча — обновляем; если новее — добавляем
    if buf["closes"] and candle[6] == "true":   # confirm=true → свеча закрыта
        # Заменяем последнюю (она была неподтверждённой)
        for key, val in zip(("opens","highs","lows","closes","volumes"), (o,h,l,c,v)):
            if buf[key]:
                buf[key][-1] = val
            else:
                buf[key].append(val)
        # Добавляем новый незакрытый слот (следующая свеча придёт потом)
    else:
        # Неподтверждённая — просто обновляем последний элемент
        if not buf["closes"]:
            for key, val in zip(("opens","highs","lows","closes","volumes"), (o,h,l,c,v)):
                buf[key].append(val)
        else:
            buf["opens"][-1]   = o
            buf["highs"][-1]   = h
            buf["lows"][-1]    = l
            buf["closes"][-1]  = c
            buf["volumes"][-1] = v


async def _ws_send_ping(ws):
    try:
        await ws.send_str(json.dumps({"op": "ping"}))
    except Exception:
        pass


async def _ws_subscribe(ws, topics: list[str]):
    await ws.send_str(json.dumps({"op": "subscribe", "args": topics}))


async def ws_ticker_loop(symbols: set[str]):
    """
    Подписывается на tickers всех монет через Bybit Public WS.
    Обновляет price_history напрямую.
    Использует прокси если задан (через _session.ws_connect).
    """
    global _ws_subscribed_syms
    syms = sorted(symbols)
    # Разбиваем на батчи по 500 (лимит Bybit)
    batches = [syms[i:i+500] for i in range(0, len(syms), 500)]

    while True:
        try:
            proxy = None
            proxy_url = os.getenv("PROXY_URL") or getattr(config, "PROXY_URL", None)
            async with _session.ws_connect(
                BYBIT_WS_PUBLIC,
                proxy=proxy_url,
                heartbeat=WS_PING_INTERVAL,
                timeout=30,
            ) as ws:
                # Подписываемся на tickers батчами
                for batch in batches:
                    topics = [f"tickers.{s}" for s in batch]
                    await _ws_subscribe(ws, topics)
                _ws_subscribed_syms = set(syms)
                log.info("WS tickers: подписано на %d монет", len(syms))

                ping_ts = time.time()
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        topic = data.get("topic", "")
                        if topic.startswith("tickers."):
                            ticker = data.get("data", {})
                            sym    = ticker.get("symbol", "")
                            last   = ticker.get("lastPrice", "")
                            if sym and last:
                                try:
                                    price = float(last)
                                    if price > 0:
                                        now = time.time()
                                        hist = price_history.setdefault(sym, [])
                                        hist.append((now, price))
                                        cutoff = max(current_window * 2, 86400)
                                        if len(hist) % 100 == 0:
                                            price_history[sym] = [
                                                (t, p) for t, p in hist if now - t <= cutoff
                                            ]
                                        last_check_time = now
                                        _ws_tickers_ready.set()
                                except (ValueError, TypeError):
                                    pass
                        # Ping каждые WS_PING_INTERVAL сек
                        if time.time() - ping_ts >= WS_PING_INTERVAL:
                            await _ws_send_ping(ws)
                            ping_ts = time.time()
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        log.warning("WS tickers: соединение закрыто, переподключение...")
                        break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("ws_ticker_loop: %s", e)
        await asyncio.sleep(WS_RECONNECT_SEC)


async def ws_kline_loop(symbols: set[str]):
    """
    Подписывается на kline.1 / kline.5 / kline.15 для всех монет.
    Обновляет _kline_store.
    """
    syms = sorted(symbols)
    intervals = ["1", "5", "15"]
    # Разбиваем на батчи (3 интервала × N монет, лимит 500 топиков за запрос)
    all_topics = [f"kline.{iv}.{s}" for iv in intervals for s in syms]
    batches    = [all_topics[i:i+500] for i in range(0, len(all_topics), 500)]

    while True:
        try:
            proxy_url = os.getenv("PROXY_URL") or getattr(config, "PROXY_URL", None)
            async with _session.ws_connect(
                BYBIT_WS_PUBLIC,
                proxy=proxy_url,
                heartbeat=WS_PING_INTERVAL,
                timeout=30,
            ) as ws:
                for batch in batches:
                    await _ws_subscribe(ws, batch)
                log.info("WS klines: подписано (%d топиков)", len(all_topics))

                ping_ts = time.time()
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data  = json.loads(msg.data)
                        topic = data.get("topic", "")
                        if topic.startswith("kline."):
                            parts = topic.split(".")   # kline.1.BTCUSDT
                            if len(parts) == 3:
                                _, iv_str, sym = parts
                                iv_map = {"1": "Min1", "5": "Min5", "15": "Min15"}
                                interval = iv_map.get(iv_str)
                                if interval:
                                    for candle in data.get("data", []):
                                        _kline_update(sym, interval, [
                                            candle.get("start"),
                                            candle.get("open"),
                                            candle.get("high"),
                                            candle.get("low"),
                                            candle.get("close"),
                                            candle.get("volume"),
                                            candle.get("confirm", "false"),
                                        ])
                        if time.time() - ping_ts >= WS_PING_INTERVAL:
                            await _ws_send_ping(ws)
                            ping_ts = time.time()
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        log.warning("WS klines: соединение закрыто, переподключение...")
                        break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("ws_kline_loop: %s", e)
        await asyncio.sleep(WS_RECONNECT_SEC)


async def coldstart_klines(symbols: set[str]):
    """
    Один раз при старте загружает историю свечей через REST
    чтобы индикаторы заработали сразу, не дожидаясь накопления WS-данных.
    """
    log.info("Cold-start: загружаем свечи для %d монет...", len(symbols))
    sem = asyncio.Semaphore(20)   # параллельно не более 20 запросов

    async def _load_one(sym: str):
        async with sem:
            results = await asyncio.gather(
                _fetch_bybit_klines(sym, "Min1",  KLINE_LIMIT_1M),
                _fetch_bybit_klines(sym, "Min5",  KLINE_LIMIT_5M),
                _fetch_bybit_klines(sym, "Min15", KLINE_LIMIT_15M),
                return_exceptions=True,
            )
            store = _kline_store.setdefault(sym, {})
            for interval, res in zip(("Min1", "Min5", "Min15"), results):
                if not isinstance(res, Exception) and res["closes"]:
                    store[interval] = res

    tasks = [_load_one(s) for s in symbols]
    # Батчами по 100 чтобы не перегружать прокси
    for i in range(0, len(tasks), 100):
        await asyncio.gather(*tasks[i:i+100], return_exceptions=True)
        await asyncio.sleep(0.5)

    log.info("Cold-start свечей завершён")
    _ws_klines_ready.set()


def get_klines_multi(sym: str) -> dict:
    """Читает свечи из памяти (заполнены WS + cold-start). Без HTTP."""
    return {
        "klines_1m":  _kline_get(sym, "Min1"),
        "klines_5m":  _kline_get(sym, "Min5"),
        "klines_15m": _kline_get(sym, "Min15"),
    }


# ── Оставляем REST-функции для cold-start (используются только там) ──────────

_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL  = 60
KLINE_LIMIT_1M   = 120
KLINE_LIMIT_5M   = 100
KLINE_LIMIT_15M  = 60
KLINE_LIMIT      = KLINE_LIMIT_1M
_BYBIT_INTERVAL  = {"Min1": "1", "Min5": "5", "Min15": "15"}


async def _fetch_bybit_klines(symbol: str, interval: str = "Min1", limit: int = KLINE_LIMIT_1M) -> dict:
    empty    = {"opens": [], "closes": [], "highs": [], "lows": [], "volumes": []}
    bybit_iv = _BYBIT_INTERVAL.get(interval, "1")
    try:
        async with _session.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category": "linear", "symbol": symbol,
                    "interval": bybit_iv, "limit": limit},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = await r.json()
        if data.get("retCode") == 0:
            rows = list(reversed(data["result"]["list"]))
            return {
                "opens":   [float(c[1]) for c in rows],
                "highs":   [float(c[2]) for c in rows],
                "lows":    [float(c[3]) for c in rows],
                "closes":  [float(c[4]) for c in rows],
                "volumes": [float(c[5]) for c in rows],
            }
    except Exception as e:
        log.debug("Bybit kline REST %s %s: %s", symbol, interval, e)
    return empty


# ================================================================
#  MONITOR  — читает данные из памяти (WS), без HTTP-запросов
# ================================================================

async def monitor():
    global current_percent, current_window, signals_count, reversal_count, checks_count, last_check_time
    global REVERSAL_GROWTH_MIN_PCT, NOTIFY_BIG_MOVE_PCT

    _cache_load_levels()
    await broadcast("✅ <b>Бот запущен</b> (v17 — Bybit WebSocket)")

    # Ждём первых данных от WS
    log.info("Ожидание первых тикеров от WebSocket...")
    await asyncio.wait_for(_ws_tickers_ready.wait(), timeout=60)
    log.info("WebSocket тикеры готовы, запускаем анализ")

    while True:
        if monitor_paused:
            await asyncio.sleep(2)
            continue

        try:
            now          = time.time()
            checks_count += 1
            last_check_time = now

            if PROM_AVAILABLE:
                PROM_CHECKS.inc()
                PROM_COINS.set(len(price_history))

            # Снимаем снэпшот текущих цен из price_history (заполняется WS)
            snapshot: dict[str, float] = {}
            for sym, hist in list(price_history.items()):
                if hist:
                    snapshot[sym] = hist[-1][1]

            for sym, price in snapshot.items():
                await asyncio.sleep(0)
                last_check_time = time.time()

                if price <= 0:
                    continue

                hist   = price_history.get(sym, [])
                cutoff = max(current_window * 2, 86400)
                price_history[sym] = [(t, p) for t, p in hist if now - t <= cutoff]

                recent = [(t, p) for t, p in price_history[sym] if now - t <= current_window]
                source = "Bybit"

                if len(recent) < MIN_SAMPLES:
                    continue

                old_price = recent[0][1]
                if old_price <= 0:
                    continue

                growth    = (price - old_price) / old_price * 100
                direction = 1 if growth > 0 else -1

                # ── Свечи из памяти (WS + cold-start) — нет HTTP ──────────────
                klines_all = get_klines_multi(sym)
                klines_1m  = klines_all["klines_1m"]
                klines_5m  = klines_all["klines_5m"]
                klines_15m = klines_all["klines_15m"]
                closes     = klines_1m["closes"]

                rsi = calculate_rsi(closes) if closes else None
                if rsi is None:
                    vals = [p for _, p in price_history[sym][-120:]]
                    rsi  = calculate_rsi(vals)

                # ════════════════════════════════════════════════════════════
                #  БЛОК 1: РАЗВОРОТ НА ШОРТ
                #  Условие: пиковый рост за REVERSAL_WINDOW_SEC >= REVERSAL_GROWTH_MIN_PCT
                #  Фильтр дублей: кулдаун + смена цены + рост скора
                # ════════════════════════════════════════════════════════════
                peak_growth = get_peak_growth(sym, price, REVERSAL_WINDOW_SEC)
                if peak_growth >= REVERSAL_GROWTH_MIN_PCT or growth >= REVERSAL_GROWTH_MIN_PCT:
                    last_rev_ts = _reversal_cooldown.get(sym, 0)
                    if now - last_rev_ts >= REVERSAL_COOLDOWN_SEC:
                        rev = detect_short_reversal(
                            sym, price, klines_1m, klines_5m, klines_15m,
                            recent, growth, rsi, REVERSAL_MIN_SCORE
                        )
                        if rev["triggered"]:
                            # ── Умный фильтр повторов ───────────────────────
                            last_info = _reversal_last.get(sym)
                            if last_info:
                                price_change = abs(price - last_info["price"]) / last_info["price"] * 100
                                score_delta  = rev["score"] - last_info["score"]
                                # Пропускаем если: цена почти не изменилась И скор не вырос значительно
                                if (price_change < REVERSAL_REPEAT_PRICE_PCT
                                        and score_delta < REVERSAL_REPEAT_SCORE_DELTA):
                                    log.debug(
                                        "REVERSAL skip %s: price_chg=%.2f%% score_delta=%d",
                                        sym, price_change, score_delta,
                                    )
                                    continue
                            # ── Отправляем сигнал ───────────────────────────
                            duration = calculate_growth_duration(recent)
                            rev_text = format_reversal_alert(
                                sym, price, max(growth, peak_growth), source, rev,
                                duration_sec=duration,
                                window_sec=REVERSAL_WINDOW_SEC,
                            )
                            _reversal_cooldown[sym] = now
                            _reversal_last[sym] = {"price": price, "score": rev["score"], "ts": now}
                            db_save_reversal(
                                sym, price, rev["score"], rev["factors"],
                                rev["rsi"], rev["macd_hist"],
                                rev["stoch_rsi"], rev["bb_pct"],
                                rev["atr"], rev["target1"], rev["target2"], source,
                            )
                            await broadcast(rev_text)
                            reversal_count += 1
                            if PROM_AVAILABLE:
                                PROM_REVERSALS.inc()
                            log.info("REVERSAL %s score=%d/16 peak=%.2f%% price_chg=%.2f%%",
                                     sym, rev["score"], peak_growth,
                                     price_change if last_info else 0.0)
                            continue  # не дублируем обычным алертом

                # ════════════════════════════════════════════════════════════
                #  БЛОК 1.5: УВЕДОМЛЕНИЕ О БОЛЬШОМ РОСТЕ / ПАДЕНИИ (15% и т.п.)
                # ════════════════════════════════════════════════════════════
                if NOTIFY_BIG_MOVE_PCT > 0 and abs(growth) >= NOTIFY_BIG_MOVE_PCT:
                    last_notif = _notify_cooldown.get(sym, 0)
                    if now - last_notif >= NOTIFY_BIG_MOVE_COOLDOWN_SEC:
                        direction_emoji = "🚀" if growth > 0 else "📉"
                        move_label = "РОСТ" if growth > 0 else "ПАДЕНИЕ"
                        rsi_str = f"{rsi:.1f}" if rsi is not None else "—"
                        dur_str = _fmt_dur(calculate_growth_duration(recent))
                        notif_text = (
                            f"🔔 <b>КРУПНОЕ ДВИЖЕНИЕ — {move_label}</b>\n\n"
                            f"🪙 <b>{sym}</b>  [{source}]\n"
                            f"💵 Цена: <code>{price}</code>\n"
                            f"{direction_emoji} {move_label}: <b>{growth:+.2f}%</b> "
                            f"за {dur_str}\n"
                            f"📊 RSI: <code>{rsi_str}</code>\n\n"
                            f"📋 <code>{sym}</code> #большое_движение"
                        )
                        _notify_cooldown[sym] = now
                        await broadcast(notif_text)
                        log.info("BigMove notify: %s %+.2f%%", sym, growth)

                # ════════════════════════════════════════════════════════════
                #  БЛОК 2: ОБЫЧНЫЕ АЛЕРТЫ (РОСТ / ПАДЕНИЕ)
                # ════════════════════════════════════════════════════════════
                if abs(growth) < current_percent:
                    level = _cache_get_level(sym)
                    if level and level["direction"] != direction:
                        _cache_clear_level(sym)
                    continue

                # RSI-фильтр направления
                if rsi is not None:
                    if direction == 1 and rsi < 50:
                        continue
                    if direction == -1 and rsi > 50:
                        continue

                last_sent = _alert_cooldown.get(sym, 0)
                if now - last_sent < ALERT_COOLDOWN_SEC:
                    continue

                level = _cache_get_level(sym)
                if level:
                    prev_price = level["alert_price"]
                    prev_dir   = level["direction"]
                    if prev_dir == direction:
                        if abs(price - prev_price) / prev_price * 100 < current_percent:
                            continue

                vals      = [p for _, p in price_history[sym][-120:]]
                macd_data = calculate_macd_full(vals)
                macd      = macd_data["histogram"]
                # RSI-тренд из уже загруженных свечей — не делаем лишний HTTP запрос
                rsi_trend = calculate_rsi_trend(closes) if len(closes) >= 17 else None
                accel     = calculate_acceleration(recent)
                duration  = calculate_growth_duration(recent)
                breakout  = check_24h_breakout(sym, price)
                day_ctx   = get_24h_context(sym, price)

                if direction == 1:
                    text = format_growth_alert(
                        sym, price, growth, rsi, macd, source,
                        rsi_trend=rsi_trend, accel=accel,
                        duration_sec=duration, breakout=breakout, day_context=day_ctx,
                    )
                else:
                    text = format_drop_alert(
                        sym, price, growth, rsi, macd, source,
                        rsi_trend=rsi_trend, duration_sec=duration,
                        breakout=breakout, day_context=day_ctx,
                    )

                _cache_set_level(sym, price, direction)
                _alert_cooldown[sym] = now
                db_save_alert(sym, price, growth, rsi, macd, source)
                await broadcast(text)
                signals_count += 1

                if PROM_AVAILABLE:
                    PROM_SIGNALS.inc()

                log.info("Signal: %s %+.2f%% [%s]", sym, growth, source)

            # WS пушит данные непрерывно — делаем паузу config.INTERVAL
            # чтобы не жечь CPU, но не ждать REST-ответа
            await asyncio.sleep(config.INTERVAL)

            # ── Периодическая очистка памяти ──────────────────────────────────
            checks_count += 1
            if checks_count % 200 == 0:
                # Удаляем устаревшие кулдауны
                stale_ts = now - max(ALERT_COOLDOWN_SEC, REVERSAL_COOLDOWN_SEC) * 3
                for d in (_alert_cooldown, _reversal_cooldown, _notify_cooldown):
                    stale = [k for k, v in d.items() if v < stale_ts]
                    for k in stale:
                        d.pop(k, None)
            if checks_count % 1000 == 0:
                # Удаляем монеты без свежих данных
                cutoff2 = now - max(current_window * 4, 86400 * 2)
                dead = [s for s, h in price_history.items() if not h or h[-1][0] < cutoff2]
                for s in dead:
                    price_history.pop(s, None)
                if dead:
                    log.info("price_history: удалено %d мёртвых монет", len(dead))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("monitor error: %s", e)
            await asyncio.sleep(5)


# ================================================================
#  COMMAND HANDLERS
# ================================================================

async def handle_message(msg: dict):
    global current_percent, current_window, monitor_paused
    global REVERSAL_MIN_SCORE, REVERSAL_RSI_OB, REVERSAL_STOCH_OB, REVERSAL_STOCH_EXT
    global REVERSAL_BB_OB, REVERSAL_MACD_SLOPE, REVERSAL_ACCEL, REVERSAL_HIGH_MARGIN
    global REVERSAL_MOMENTUM, REVERSAL_COOLDOWN_SEC, REVERSAL_ATR_MULT, REVERSAL_VOL_RATIO
    global REVERSAL_GROWTH_MIN_PCT, NOTIFY_BIG_MOVE_PCT, REVERSAL_WINDOW_SEC
    global REVERSAL_REPEAT_PRICE_PCT, REVERSAL_REPEAT_SCORE_DELTA

    text    = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]

    if chat_id != int(config.CHAT_ID):
        if text == "/subscribe":
            db_add_subscriber(chat_id)
            await send_message("✅ Вы подписались на сигналы", chat_id)
        elif text == "/unsubscribe":
            db_remove_subscriber(chat_id)
            await send_message("❌ Вы отписались от сигналов", chat_id)
        else:
            await send_message("⛔ Нет доступа. /subscribe — подписаться на алерты.", chat_id)
        return

    if text in ("/start", "/menu"):
        await send_message(
            f"🚀 <b>Crypto Alert Bot v16</b>\n\n"
            f"📈 Порог роста: <b>{current_percent}%</b>\n"
            f"⏱ Период алертов: <b>{current_window // 60} мин</b>\n"
            f"🔄 Порог разворота: <b>{REVERSAL_MIN_SCORE}/16 факторов</b>\n"
            f"📉 Рост для разворота: <b>{REVERSAL_GROWTH_MIN_PCT}%</b> за <b>{_fmt_dur(REVERSAL_WINDOW_SEC)}</b>\n"
            f"🔔 Уведомление движения: <b>{NOTIFY_BIG_MOVE_PCT}%</b>\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг активен'}\n\n"
            f"<b>Кнопки управления:</b>\n"
            f"  📈 0.2%/5%/10%/15%/20% — порог роста/падения алертов\n"
            f"  📉 Разворот 1-25% — минимальный рост для шорт-разворота\n"
            f"  🕐 Разворот 5м-1д — период расчёта роста разворота\n"
            f"  ⏱ 5 мин/1 час/4 ч/1 д — период окна алертов\n"
            f"  🎚 Порог 3-7/12 — чувствительность (кол-во факторов)\n"
            f"  ⚙️ Настройки разворота — все параметры\n\n"
            f"<b>Команды:</b>\n"
            f"  /set_percent 2.5 — порог алертов (%)\n"
            f"  /set_window 60 — период алертов (мин)\n"
            f"  /rev_score 4 — порог факторов разворота\n"
            f"  /rev_growth 5 — % роста для разворота\n"
            f"  /rev_window 60 — период роста разворота (мин)\n"
            f"  /notify_pct 15 — порог уведомлений движения (%)\n"
            f"  /rev_cooldown 5 — кулдаун разворота (мин)",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    _pct_map = {
        "📈 0.2%": 0.2, "📈 5%": 5.0, "📈 10%": 10.0, "📈 15%": 15.0, "📈 20%": 20.0,
    }
    if text in _pct_map:
        current_percent = _pct_map[text]
        await send_message(
            f"✅ Порог роста/падения: <b>{current_percent}%</b>",
            chat_id, reply_markup=reply_keyboard(),
        )
        return

    _win_map = {
        "⏱ 5 мин": (300,   "5 мин"),
        "⏱ 1 час": (3600,  "1 час"),
        "⏱ 4 ч":   (14400, "4 часа"),
        "⏱ 1 д":   (86400, "1 день"),
    }
    if text in _win_map:
        current_window, label = _win_map[text]
        await send_message(
            f"✅ Период: <b>{label}</b>",
            chat_id, reply_markup=reply_keyboard(),
        )
        return

    if text == "⏸ Пауза":
        monitor_paused = True
        await send_message("⏸ Мониторинг приостановлен", chat_id, reply_markup=reply_keyboard())
        return

    if text == "▶️ Продолжить":
        monitor_paused = False
        await send_message("▶️ Мониторинг возобновлён", chat_id, reply_markup=reply_keyboard())
        return

    if text in ("📊 Статус", "/status"):
        uptime = int(time.time() - start_time)
        d, r   = divmod(uptime, 86400)
        h, r   = divmod(r, 3600)
        m      = r // 60
        await send_message(
            f"📊 <b>СТАТУС</b>\n\n"
            f"🟢 Аптайм: {d}д {h}ч {m}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"🔔 Сигналов: {signals_count}\n"
            f"🔄 Разворотов: {reversal_count}\n"
            f"🔄 Порог разворота: {REVERSAL_MIN_SCORE}/16\n"
            f"📉 Рост для разворота: {REVERSAL_GROWTH_MIN_PCT}% за {_fmt_dur(REVERSAL_WINDOW_SEC)}\n"
            f"🔔 Уведомление движения: {NOTIFY_BIG_MOVE_PCT}%\n"
            f"🔄 Кулдаун разворота: {REVERSAL_COOLDOWN_SEC // 60} мин\n"
            f"📈 Порог роста: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"⚡ Интервал: {config.INTERVAL} сек\n"
            f"👥 Подписчиков: {len(db_get_subscribers())}\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Активен'}",
            chat_id,
        )
        return

    if text in ("📋 История", "/history"):
        asyncio.create_task(_cmd_history(chat_id))
        return

    if text in ("🏆 Топ-5", "/top5"):
        asyncio.create_task(_cmd_top5(chat_id))
        return

    if text in ("📤 Экспорт", "/export"):
        asyncio.create_task(_cmd_export(chat_id))
        return

    if text in ("🗑 Кулдауны", "/clear_cooldowns"):
        _cache_clear_all()
        _reversal_cooldown.clear()
        _reversal_last.clear()
        _notify_cooldown.clear()
        await send_message("🗑 Кулдауны, уровни и история разворотов сброшены", chat_id)
        return

    if text in ("🔄 Развороты", "/reversals"):
        asyncio.create_task(_cmd_reversals(chat_id))
        return

    if text in ("⚙️ Настройки разворота", "⚙️ Разворот", "/reversal_settings"):
        await _cmd_reversal_settings(chat_id)
        return

    if text in ("🗄 БД Статистика", "/db_stats"):
        s = db_stats()
        await send_message(
            f"🗄 <b>База данных</b>\n\n"
            f"📋 Алертов: {s['alerts']}\n"
            f"📅 Старейший: {s['oldest_alert']}\n"
            f"🪙 Уровней: {s['levels']}\n"
            f"🔄 Разворотов: {s['reversals']}\n"
            f"💾 Размер: {s['size_mb']:.2f} МБ",
            chat_id,
        )
        return

    if text in ("🧹 БД Очистка", "/db_cleanup"):
        deleted = db_cleanup()
        total   = sum(deleted.values())
        await send_message(
            f"🧹 <b>Очистка завершена</b>\n\n"
            f"📋 Алертов: -{deleted.get('alerts', 0)}\n"
            f"📍 Уровней: -{deleted.get('alert_levels', 0)}\n"
            f"📊 price_stats: -{deleted.get('price_stats', 0)}\n"
            f"🔄 Разворотов: -{deleted.get('reversal_signals', 0)}\n"
            f"────────────────\n"
            f"🗑 Итого: {total} строк\n"
            f"💾 Размер: {db_size_mb():.2f} МБ",
            chat_id,
        )
        return

    # Кнопки % роста для разворотных сигналов в шорт
    _rev_growth_map = {
        "📉 Разворот 1%":  1.0,
        "📉 Разворот 5%":  5.0,
        "📉 Разворот 10%": 10.0,
        "📉 Разворот 15%": 15.0,
        "📉 Разворот 25%": 25.0,
    }
    if text in _rev_growth_map:
        REVERSAL_GROWTH_MIN_PCT = _rev_growth_map[text]
        await send_message(
            f"✅ Порог роста для разворота в шорт: <b>{REVERSAL_GROWTH_MIN_PCT}%</b>\n"
            f"ℹ️ Разворотный детектор сработает если рост ≥ {REVERSAL_GROWTH_MIN_PCT}%",
            chat_id,
        )
        return

    if text.startswith("/notify_pct"):
        try:
            val = float(text.split()[1])
            assert 1.0 <= val <= 100.0
            NOTIFY_BIG_MOVE_PCT = val
            _notify_cooldown.clear()
            await send_message(f"✅ Уведомление о движении: <b>≥ {val}%</b>", chat_id)
        except Exception:
            await send_message(
                f"❌ /notify_pct 15   (текущее: {NOTIFY_BIG_MOVE_PCT}%)\n"
                f"Диапазон: 1–100", chat_id
            )
        return

    if text.startswith("/rev_growth"):
        try:
            val = float(text.split()[1])
            assert 0.1 <= val <= 100.0
            REVERSAL_GROWTH_MIN_PCT = val
            await send_message(
                f"✅ Порог роста для разворота: <b>{val}%</b>", chat_id
            )
        except Exception:
            await send_message(
                f"❌ /rev_growth 5   (текущее: {REVERSAL_GROWTH_MIN_PCT}%)\n"
                f"Диапазон: 0.1–100", chat_id
            )
        return

    # Кнопки периода окна для расчёта роста разворота
    _rev_window_map = {
        "🕐 Разворот 5м":  (300,   "5 минут"),
        "🕐 Разворот 30м": (1800,  "30 минут"),
        "🕐 Разворот 1ч":  (3600,  "1 час"),
        "🕐 Разворот 4ч":  (14400, "4 часа"),
        "🕐 Разворот 1д":  (86400, "1 день"),
    }
    if text in _rev_window_map:
        REVERSAL_WINDOW_SEC, label = _rev_window_map[text]
        await send_message(
            f"✅ Окно роста для разворота: <b>{label}</b>\n"
            f"ℹ️ Разворотный детектор смотрит рост монеты за последние <b>{label}</b>.\n"
            f"Порог: <b>{REVERSAL_GROWTH_MIN_PCT}%</b> за этот период.",
            chat_id,
        )
        return

    if text.startswith("/rev_window"):
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 10080
            REVERSAL_WINDOW_SEC = val * 60
            await send_message(
                f"✅ Окно разворота: <b>{val} мин</b> ({_fmt_dur(REVERSAL_WINDOW_SEC)})", chat_id
            )
        except Exception:
            cur = _fmt_dur(REVERSAL_WINDOW_SEC)
            await send_message(
                f"❌ /rev_window 60   (в минутах, 1–10080)\nТекущее: {cur}", chat_id
            )
        return

    if text.startswith("/rev_price_pct"):
        try:
            val = float(text.split()[1])
            assert 0.1 <= val <= 20.0
            REVERSAL_REPEAT_PRICE_PCT = val
            await send_message(
                f"✅ Мин. смена цены между разворотами: <b>{val}%</b>\n"
                f"ℹ️ Повторный сигнал по монете только если цена изменилась на ≥{val}%",
                chat_id,
            )
        except Exception:
            await send_message(
                f"❌ /rev_price_pct 3   (текущее: {REVERSAL_REPEAT_PRICE_PCT}%)\nДиапазон: 0.1–20",
                chat_id,
            )
        return

    if text.startswith("/rev_score_delta"):
        try:
            val = int(text.split()[1])
            assert 0 <= val <= 12
            REVERSAL_REPEAT_SCORE_DELTA = val
            await send_message(
                f"✅ Мин. прирост скора между разворотами: <b>+{val}</b>\n"
                f"ℹ️ Повторный сигнал только если скор вырос на ≥{val} факторов",
                chat_id,
            )
        except Exception:
            await send_message(
                f"❌ /rev_score_delta 2   (текущее: {REVERSAL_REPEAT_SCORE_DELTA})\nДиапазон: 0–12",
                chat_id,
            )
        return

    # Быстрая настройка порога разворота через кнопки
    _rev_score_map = {
        "🎚 Порог 3/12": 3,
        "🎚 Порог 4/12": 4,
        "🎚 Порог 5/12": 5,
        "🎚 Порог 7/12": 7,
    }
    if text in _rev_score_map:
        REVERSAL_MIN_SCORE = _rev_score_map[text]
        await send_message(
            f"✅ Порог разворота: <b>{REVERSAL_MIN_SCORE}/16 факторов</b>\n"
            f"ℹ️ Агрессивный: 3, Стандарт: 4-5, Строгий: 7",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    # ── Команды настройки разворота ───────────────────────────────────────────
    _rev_cmds = {
        "/rev_score":    ("REVERSAL_MIN_SCORE",   int,   1,    16,    "Порог факторов",           "/16"),
        "/rev_rsi":      ("REVERSAL_RSI_OB",      float, 50,   90,    "RSI перекупленность",      ""),
        "/rev_stoch":    ("REVERSAL_STOCH_OB",    float, 0.5,  1.0,   "StochRSI порог",           ""),
        "/rev_bb":       ("REVERSAL_BB_OB",       float, 0.8,  1.5,   "Боллинджер %B порог",      ""),
        "/rev_accel":    ("REVERSAL_ACCEL",       float, 0.1,  1.0,   "Порог замедления",         ""),
        "/rev_momentum": ("REVERSAL_MOMENTUM",    float, -5.0, 0,     "Порог моментума",          "%"),
        "/rev_cooldown": ("REVERSAL_COOLDOWN_SEC",int,   1,    1440,  "Кулдаун разворота (мин)",  " мин"),
        "/rev_atr":      ("REVERSAL_ATR_MULT",    float, 1.0,  10.0,  "ATR-перегрев множитель",   "x"),
        "/rev_vol":      ("REVERSAL_VOL_RATIO",   float, 0.1,  1.0,   "Порог слабого объёма",     ""),
    }
    for cmd, (var, typ, vmin, vmax, label, sfx) in _rev_cmds.items():
        if text.startswith(cmd):
            try:
                raw = text.split()[1]
                val = typ(raw)
                assert vmin <= val <= vmax
                # Для кулдауна храним в секундах
                stored = val * 60 if var == "REVERSAL_COOLDOWN_SEC" else val
                globals()[var] = stored
                display = val if var != "REVERSAL_COOLDOWN_SEC" else val
                await send_message(f"✅ {label}: <b>{display}{sfx}</b>", chat_id)
            except Exception:
                await send_message(
                    f"❌ {cmd} {vmin}…{vmax}  (текущее: {globals()[var]})", chat_id
                )
            return

    if text.startswith("/rev_high"):
        try:
            val = float(text.split()[1])
            assert 0.0 <= val <= 5.0
            REVERSAL_HIGH_MARGIN = 1.0 - val / 100  # global объявлен выше — OK
            await send_message(f"✅ Зона отбоя от хая: <b>{val}%</b> (margin={REVERSAL_HIGH_MARGIN:.4f})", chat_id)
        except Exception:
            cur_pct = round((1.0 - REVERSAL_HIGH_MARGIN) * 100, 2)
            await send_message(f"❌ /rev_high 0.2  (отступ от хая в %, 0–5)\nТекущее: {cur_pct}%", chat_id)
        return

    if text.startswith("/set_percent"):
        try:
            val = float(text.split()[1])
            assert 0.01 <= val <= 100
            current_percent = val
            await send_message(f"✅ Новый порог: <b>{val}%</b>", chat_id)
        except Exception:
            await send_message("❌ /set_percent 2.5", chat_id)
        return

    if text.startswith("/set_window"):
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 10080
            current_window = val * 60
            await send_message(f"✅ Новый период: <b>{val} мин</b>", chat_id)
        except Exception:
            await send_message("❌ /set_window 60  (в минутах)", chat_id)
        return

    if text == "/subscribe":
        db_add_subscriber(chat_id)
        await send_message("✅ Вы в списке получателей", chat_id)
        return

    await send_message(
        "❓ Неизвестная команда.\n/menu — открыть панель управления",
        chat_id,
        reply_markup=reply_keyboard(),
    )


async def _cmd_history(chat_id):
    rows = db_recent_alerts(10)
    if not rows:
        await send_message("📋 История пуста", chat_id)
        return
    lines = ["📋 <b>Последние 10 сигналов:</b>\n"]
    for r in rows:
        ts    = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
        sign  = "🚀" if r["growth"] > 0 else "📉"
        rsi_s = f"RSI {r['rsi']:.1f}" if r["rsi"] is not None else "RSI —"
        lines.append(f"{sign} <b>{r['symbol']}</b> {r['growth']:+.2f}% | {rsi_s} | {ts}")
    await send_message("\n".join(lines), chat_id)


async def _cmd_top5(chat_id):
    rows = db_top_signals(5)
    if not rows:
        await send_message("🏆 Нет данных за 24ч", chat_id)
        return
    lines = ["🏆 <b>Топ-5 сигналов за 24ч:</b>\n"]
    for i, r in enumerate(rows, 1):
        ts   = time.strftime("%H:%M", time.localtime(r["ts"]))
        sign = "🚀" if r["growth"] > 0 else "📉"
        lines.append(f"{i}. {sign} <b>{r['symbol']}</b> {r['growth']:+.2f}% [{r['source']}] {ts}")
    await send_message("\n".join(lines), chat_id)


async def _cmd_reversals(chat_id):
    rows = db_recent_reversals(8)
    if not rows:
        await send_message("🔄 Разворотных сигналов пока нет", chat_id)
        return
    lines = ["🔄 <b>Последние 8 разворотов:</b>\n"]
    for r in rows:
        ts      = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
        score   = r["score"]
        lvl     = "🔴" if score >= 8 else "🟠" if score >= 5 else "🟡"
        factors = r["factors"]
        # Экранируем HTML-символы в тексте фактора
        f_brief = factors[0].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") if factors else "—"
        t1_s    = f" → цель {r['target1']:.4g}" if r.get("target1") else ""
        lines.append(
            f"{lvl} <b>{r['symbol']}</b> [{score}/12] {ts}{t1_s}\n"
            f"   <i>{f_brief}</i>"
        )
    await send_message("\n".join(lines), chat_id)


async def _cmd_reversal_settings(chat_id):
    high_pct = round((1.0 - REVERSAL_HIGH_MARGIN) * 100, 2)
    await send_message(
        f"⚙️ <b>Настройки детектора разворота (16 факторов)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Скоринг:</b>\n"
        f"  Порог:         <code>{REVERSAL_MIN_SCORE}/16</code>  → /rev_score 4\n\n"
        f"<b>Пороги факторов:</b>\n"
        f"  RSI OB (1m):   <code>&gt; {REVERSAL_RSI_OB}</code>     → /rev_rsi 70\n"
        f"  RSI OB (15m):  <code>&gt; 75</code>          (фиксировано)\n"
        f"  StochRSI:      <code>&gt; {REVERSAL_STOCH_OB}</code>   → /rev_stoch 0.80\n"
        f"  Боллинджер:    <code>&gt; {REVERSAL_BB_OB}</code>      → /rev_bb 1.0\n"
        f"  Замедление:    <code>&lt; {REVERSAL_ACCEL}</code>      → /rev_accel 0.5\n"
        f"  Моментум:      <code>&lt; {REVERSAL_MOMENTUM}%</code>  → /rev_momentum -0.5\n"
        f"  ATR-перегрев:  <code>&gt; {REVERSAL_ATR_MULT}×</code> → /rev_atr 3.0\n"
        f"  Объём слабый:  <code>&lt; {REVERSAL_VOL_RATIO:.0%}</code>  → /rev_vol 0.7\n"
        f"  Зона хая 24h:  <code>{high_pct}%</code>        → /rev_high 0.2\n"
        f"  Wick Rejection:<code>&gt; 0.55</code>         (фиксировано)\n\n"
        f"<b>Новые факторы (13-16):</b>\n"
        f"  13. OBV-дивергенция (Min15, цена↑ OBV↓)\n"
        f"  14. Wick Rejection Ratio &gt;0.55 (Min1)\n"
        f"  15. RSI 15m &gt;75 (старший таймфрейм)\n"
        f"  16. Lower Highs (Min5, 3+ убывающих хая)\n\n"
        f"<b>Кулдаун и фильтр дублей:</b>\n"
        f"  Кулдаун:         <code>{REVERSAL_COOLDOWN_SEC // 60} мин</code>  → /rev_cooldown 30\n"
        f"  Мин. смена цены: <code>{REVERSAL_REPEAT_PRICE_PCT}%</code>  → /rev_price_pct 3\n"
        f"  Мин. рост скора: <code>+{REVERSAL_REPEAT_SCORE_DELTA}</code>      → /rev_score_delta 2\n\n"
        f"<b>Пресеты:</b>\n"
        f"  Агрессивный: /rev_score 3  /rev_cooldown 10  /rev_price_pct 1\n"
        f"  Стандарт:    /rev_score 5  /rev_cooldown 30  /rev_price_pct 3\n"
        f"  Строгий:     /rev_score 9  /rev_cooldown 60  /rev_price_pct 5",
        chat_id,
    )


async def _cmd_export(chat_id):
    await send_message("📤 Генерирую CSV...", chat_id)
    csv_data = db_export_csv(500)
    fname    = f"alerts_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    await send_document(chat_id, fname, csv_data, caption="📤 Последние 500 алертов")


# ================================================================
#  TELEGRAM LOOP
# ================================================================

async def get_updates() -> list:
    global offset
    try:
        async with _session.get(
            f"{TG}/getUpdates",
            params={
                "timeout":         20,       # long-polling — прокси медленный
                "offset":          offset,
                "allowed_updates": json.dumps(["message", "edited_message"]),
            },
            timeout=aiohttp.ClientTimeout(total=35),  # запас на прокси
        ) as resp:
            data = await resp.json()
        if data.get("ok"):
            return data["result"]
    except Exception as e:
        log.error("get_updates: %s", e)
    return []


async def telegram_loop():
    global offset
    log.info("Telegram polling запущен")
    while True:
        try:
            updates = await get_updates()
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message") or update.get("edited_message")
                if msg:
                    asyncio.create_task(handle_message(msg))
            # Если обновлений нет — пауза 0.5с, если есть — сразу следующий запрос
            if not updates:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("telegram_loop: %s", e)
            await asyncio.sleep(3)


# ================================================================
#  BACKGROUND TASKS
# ================================================================

async def heartbeat():
    while True:
        log.info("♥ alive | signals=%d | reversals=%d | coins=%d",
                 signals_count, reversal_count, len(price_history))
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        try:
            save_state()
        except Exception:
            pass
        await asyncio.sleep(30)


async def db_cleanup_loop():
    last_vacuum = time.time()
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            deleted = db_cleanup()
            total   = sum(deleted.values())
            stats   = db_stats()
            log.info("DB cleanup: удалено %d | %.2f МБ | алертов %d",
                     total, stats["size_mb"], stats["alerts"])
            now = time.time()
            if total > 0 and now - last_vacuum >= DB_VACUUM_INTERVAL_H * 3600:
                log.info("DB VACUUM...")
                await asyncio.get_event_loop().run_in_executor(None, db_vacuum)
                last_vacuum = now
                log.info("VACUUM done | %.2f МБ", db_size_mb())
        except Exception as e:
            log.exception("db_cleanup_loop: %s", e)


# ================================================================
#  WATCHDOG
# ================================================================

WATCHDOG_TIMEOUT = 300  # 5 минут — прокси медленный, монет много


async def watchdog():
    global monitor_task, last_check_time
    await asyncio.sleep(60)
    while True:
        try:
            if not monitor_paused:
                stall = (time.time() - last_check_time) if last_check_time else 0
                if stall > WATCHDOG_TIMEOUT:
                    log.warning("Watchdog: завис %.0f с — перезапуск", stall)
                    await broadcast(f"⚠️ Watchdog: завис {stall:.0f}с. Перезапуск...")
                    if monitor_task and not monitor_task.done():
                        monitor_task.cancel()
                        try:
                            await monitor_task
                        except asyncio.CancelledError:
                            pass
                    monitor_task    = asyncio.create_task(monitor())
                    last_check_time = time.time()
                    await broadcast("✅ Monitor перезапущен")
        except Exception as e:
            log.exception("watchdog: %s", e)
        await asyncio.sleep(30)


# ================================================================
#  SHUTDOWN
# ================================================================

_shutdown_event: asyncio.Event | None = None


def _handle_signal(sig):
    log.info("Signal %s — завершение", sig)
    save_state()
    if _shutdown_event:
        _shutdown_event.set()


def handle_async_exception(loop, context):
    exc = context.get("exception")
    if exc:
        log.exception("Unhandled async exception", exc_info=exc)
    else:
        log.error("Async error: %s", context["message"])


# ================================================================
#  MAIN
# ================================================================

async def main():
    global monitor_task, _session, _shutdown_event
    global _ws_tickers_ready, _ws_klines_ready, _kline_store_lock

    _shutdown_event    = asyncio.Event()
    _ws_tickers_ready  = asyncio.Event()
    _ws_klines_ready   = asyncio.Event()
    _kline_store_lock  = asyncio.Lock()
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    if PROM_AVAILABLE:
        prom_port = getattr(config, "PROM_PORT", 8000)
        try:
            start_http_server(prom_port)
            log.info("Prometheus on :%d", prom_port)
        except Exception as e:
            log.warning("Prometheus failed: %s", e)

    proxy_url = os.getenv("PROXY_URL") or getattr(config, "PROXY_URL", None)
    if proxy_url and PROXY_AVAILABLE:
        connector = ProxyConnector.from_url(proxy_url, limit=100, ttl_dns_cache=300)
        log.info("🌐 Прокси активен: %s", proxy_url.split("@")[-1])
    elif proxy_url and not PROXY_AVAILABLE:
        log.warning("⚠️ PROXY_URL задан, но aiohttp-socks не установлен — работаем без прокси")
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    else:
        log.info("Прокси не задан — прямое подключение")
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        _session = session

        # 1. Получаем список символов через REST
        log.info("Загружаем список монет...")
        symbols = await get_symbols()
        if not symbols:
            log.error("Не удалось получить список символов — выход")
            return

        # 2. Cold-start: загружаем историю свечей через REST (параллельно с WS)
        coldstart_task = asyncio.create_task(coldstart_klines(symbols))

        # 3. WebSocket: тикеры (цены) — запускаем немедленно
        ws_ticker_task = asyncio.create_task(ws_ticker_loop(symbols))

        # 4. WebSocket: свечи — запускаем немедленно
        ws_kline_task  = asyncio.create_task(ws_kline_loop(symbols))

        # 5. Monitor — запускается, ждёт _ws_tickers_ready внутри
        monitor_task   = asyncio.create_task(monitor())

        tasks = [
            monitor_task,
            ws_ticker_task,
            ws_kline_task,
            coldstart_task,
            asyncio.create_task(telegram_loop()),
            asyncio.create_task(heartbeat()),
            asyncio.create_task(save_state_loop()),
            asyncio.create_task(db_cleanup_loop()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(_shutdown_event.wait()),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        log.info("Завершение...")
        for t in pending:
            if not t.done():
                t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    log.info("Бот остановлен")


while True:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        break
    except Exception as e:
        log.exception("Критическая ошибка: %s", e)
        time.sleep(10)
