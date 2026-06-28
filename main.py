"""
Crypto Alert Bot — v13
Улучшения vs v12:
  • Все команды вынесены в кнопки Reply Keyboard:
      - Управление: ⏸ Пауза / ▶️ Продолжить / 📊 Статус
      - Порог роста: 📈 0.2% / 5% / 10% / 20%
      - Период:      ⏱ 5 мин / 1 час / 4 ч / 1 д
      - Данные:      📋 История / 🏆 Топ-5 / 📤 Экспорт
      - Разворот:    🔄 Развороты / ⚙️ Настройки разворота / 🗑 Кулдауны
      - БД:          🗄 БД Статистика / 🧹 БД Очистка
      - Быстрый порог: 🎚 Порог 3/12 / 4/12 / 5/12 / 7/12
  • Исправлен баг кнопки разворота: текст "⚙️ Разворот" → "⚙️ Настройки разворота"
    (несовпадение текста кнопки и обработчика в v12)
  • Дубли db_stats / db_cleanup убраны (объединены с кнопками)
  • /menu теперь показывает полную справку по кнопкам и командам
  • Неизвестная команда повторно показывает клавиатуру
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import signal
import sqlite3
import time
from contextlib import contextmanager
from typing import Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
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
    import os
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
REVERSAL_COOLDOWN_SEC: int = getattr(config, "REVERSAL_COOLDOWN_SEC", 300)

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
#  MEXC KLINE CACHE  (цена + объём)
# ================================================================

_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL = 60
KLINE_LIMIT     = 120   # увеличили для ATR и EMA


async def _fetch_mexc_klines(symbol: str, interval: str = "Min1") -> dict:
    """Возвращает dict с ключами: closes, highs, lows, volumes (все list[float])."""
    empty = {"closes": [], "highs": [], "lows": [], "volumes": []}
    try:
        async with _session.get(
            "https://contract.mexc.com/api/v1/contract/kline",
            params={"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            data = await r.json()
        if data.get("success") and data.get("data"):
            d = data["data"]
            return {
                "closes":  [float(c) for c in d.get("close",  [])],
                "highs":   [float(c) for c in d.get("high",   [])],
                "lows":    [float(c) for c in d.get("low",    [])],
                "volumes": [float(c) for c in d.get("vol",    [])],
            }
    except Exception as e:
        log.debug("MEXC kline %s: %s", symbol, e)
    return empty


async def get_mexc_klines(symbol: str, interval: str = "Min1") -> dict:
    now    = time.time()
    cached = _kline_cache.get(symbol)
    if cached and now - cached["ts"] < KLINE_CACHE_TTL:
        return cached["data"]
    data = await _fetch_mexc_klines(symbol, interval)
    if data["closes"]:
        _kline_cache[symbol] = {"ts": now, "data": data}
    return data


async def get_mexc_closes(symbol: str, interval: str = "Min1") -> list[float]:
    return (await get_mexc_klines(symbol, interval))["closes"]


def _to_mexc_symbol(sym: str) -> str:
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    if sym.endswith("PERP"):
        base = sym[:-4]
        if base.endswith("USDT"):
            return base[:-4] + "_USDT"
    return sym


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
                           lows: list[float]) -> Optional[str]:
    """
    Определяет медвежий свечной паттерн на последних 2 барах:
      - Shooting Star: тело внизу, длинная верхняя тень (>2× тела)
      - Доджи: открытие ≈ закрытие (тело < 10% диапазона)
      - Медвежье поглощение: красная свеча полностью поглощает предыдущую зелёную
    """
    try:
        if len(closes) < 3 or len(highs) < 3 or len(lows) < 3:
            return None

        # Последняя свеча
        o1, c1, h1, l1 = closes[-2], closes[-1], highs[-1], lows[-1]
        body1  = abs(c1 - o1)
        range1 = h1 - l1
        if range1 == 0:
            return None
        upper_wick = h1 - max(c1, o1)
        lower_wick = min(c1, o1) - l1

        # Shooting Star: верхняя тень > 2× тела, нижняя тень маленькая, закрытие ниже открытия
        if (body1 > 0 and upper_wick > 2 * body1
                and lower_wick < body1
                and c1 < o1):
            return "Shooting Star 🌠"

        # Доджи: тело < 10% диапазона
        if body1 < range1 * 0.1:
            return "Доджи ✝️"

        # Медвежье поглощение: предыдущая зелёная, текущая красная и больше
        o0, c0 = closes[-3], closes[-2]
        if c0 > o0 and c1 < o1 and o1 >= c0 and c1 <= o0:
            return "Медвежье поглощение 🐻"

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
    Реальный объём из свечей MEXC.
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


async def get_rsi_trend_from_mexc(symbol: str, window: int = 14) -> Optional[str]:
    mexc_sym = _to_mexc_symbol(symbol)
    closes   = await get_mexc_closes(mexc_sym, interval="Min1")
    if len(closes) < window + 3:
        return None
    return calculate_rsi_trend(closes, window=window)


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


def get_peak_growth_24h(sym: str, price: float) -> float:
    """
    Максимальный рост за 24ч от минимума к текущей цене.
    Используется для разворотного детектора: монета могла уже откатить,
    но пиковый рост сохраняется в истории.
    """
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if not day_prices:
        return 0.0
    low24 = min(day_prices)
    if low24 <= 0:
        return 0.0
    return (price - low24) / low24 * 100


# ================================================================
#  REVERSAL DETECTOR  — 12 факторов
# ================================================================

def detect_short_reversal(
    sym:       str,
    price:     float,
    klines:    dict,              # {"closes": [], "highs": [], "lows": [], "volumes": []}
    recent:    list[tuple[float, float]],
    growth:    float,
    rsi:       Optional[float],
    min_score: int = 4,
) -> dict:
    """
    12-факторный детектор разворота на шорт.

    Факторы (+1 каждый):
      1.  RSI перекупленность (> REVERSAL_RSI_OB)
      2.  StochRSI в зоне перекупленности (> REVERSAL_STOCH_OB)
      3.  Цена выше верхней полосы Боллинджера (%B > REVERSAL_BB_OB)
      4.  MACD медвежий крест (histogram 0 → -) ИЛИ убывающий slope
      5.  Медвежья RSI-дивергенция (цена ↑, RSI ↓)
      6.  Замедление роста (accel < REVERSAL_ACCEL)
      7.  Отбой от 24h High (цена у хая + последние тики вниз)
      8.  Моментум иссякает (fast-slow < REVERSAL_MOMENTUM)
      9.  EMA крест вниз (EMA9 пробила EMA21 вниз)
      10. ATR-перегрев (цена выросла > REVERSAL_ATR_MULT × ATR за период)
      11. Медвежий свечной паттерн (Shooting Star / Доджи / Поглощение)
      12. Слабый объём при росте (объём последнего бара < средн.)

    Возвращает dict с score, factors, triggered, целями по шорту, ATR, Фибо.
    """
    score   = 0
    factors: list[str] = []

    closes  = klines.get("closes", [])
    highs   = klines.get("highs",  [])
    lows    = klines.get("lows",   [])
    volumes = klines.get("volumes", [])

    # Fallback на тики если свечей нет
    prices = closes if len(closes) >= 30 else [p for _, p in price_history.get(sym, [])[-120:]]

    # ── 1. RSI перекупленность ────────────────────────────────────────────────
    stoch_rsi_val = None
    if rsi is not None and rsi > REVERSAL_RSI_OB:
        score += 1
        factors.append(f"RSI перекуплен ({rsi:.1f} &gt; {REVERSAL_RSI_OB})")

    # ── 2. StochRSI ───────────────────────────────────────────────────────────
    stoch_rsi_val = calculate_stoch_rsi(prices)
    if stoch_rsi_val is not None:
        threshold = REVERSAL_STOCH_OB if (rsi is not None and rsi > REVERSAL_RSI_OB) else REVERSAL_STOCH_EXT
        if stoch_rsi_val > threshold:
            score += 1
            factors.append(f"StochRSI перекупленность ({stoch_rsi_val:.2f} &gt; {threshold})")

    # ── 3. Боллинджер %B ─────────────────────────────────────────────────────
    bb_pct = calculate_bollinger_pct(prices)
    if bb_pct is not None and bb_pct > REVERSAL_BB_OB:
        score += 1
        factors.append(f"Цена выше BB ({bb_pct:.2f})")

    # ── 4. MACD крест / slope ─────────────────────────────────────────────────
    macd_data = calculate_macd_full(prices)
    macd_hist = macd_data["histogram"]
    if macd_data["cross_down"]:
        score += 1
        factors.append("MACD медвежий крест (hist 0→-)")
    elif (macd_hist is not None
          and macd_data["slope"] is not None
          and macd_hist > 0
          and macd_data["slope"] < REVERSAL_MACD_SLOPE):
        score += 1
        factors.append(f"MACD убывает (slope={macd_data['slope']:+.6f})")

    # ── 5. RSI-дивергенция ────────────────────────────────────────────────────
    if len(prices) >= 30 and calculate_rsi_divergence(prices):
        score += 1
        factors.append("⚡ Медвежья RSI-дивергенция (цена ↑, RSI ↓)")

    # ── 6. Замедление accel ───────────────────────────────────────────────────
    accel = calculate_acceleration(recent)
    if accel is not None and growth >= current_percent and accel < REVERSAL_ACCEL:
        score += 1
        factors.append(f"Замедление импульса (accel={accel:.2f}x)")

    # ── 7. Отбой от 24h High ─────────────────────────────────────────────────
    hist_data  = price_history.get(sym, [])
    now_ts     = time.time()
    day_prices = [p for t, p in hist_data if now_ts - t <= 86400]
    high24 = max(day_prices) if day_prices else price
    low24  = min(day_prices) if day_prices else price
    if len(day_prices) >= 20:
        near_high = price >= high24 * REVERSAL_HIGH_MARGIN
        if near_high and len(recent) >= 4:
            last_ticks = [p for _, p in recent[-4:]]
            if all(last_ticks[i] >= last_ticks[i + 1] for i in range(len(last_ticks) - 1)):
                score += 1
                pct_from_high = (price - high24) / high24 * 100
                factors.append(f"Отбой от 24h High ({pct_from_high:+.2f}%)")

    # ── 8. Моментум иссякает ─────────────────────────────────────────────────
    momentum = calculate_price_momentum(prices)
    if momentum is not None and momentum < REVERSAL_MOMENTUM:
        score += 1
        factors.append(f"Моментум иссякает (diff={momentum:+.2f}%)")

    # ── 9. EMA крест вниз ────────────────────────────────────────────────────
    ema_data = calculate_ema_cross(prices)
    if ema_data["cross_down"]:
        score += 1
        factors.append(f"EMA9 пробила EMA21 вниз ({ema_data['ema_fast']:.4g} &lt; {ema_data['ema_slow']:.4g})")
    elif (ema_data["gap_pct"] is not None
          and ema_data["gap_pct"] < -0.05):
        # EMA9 уже ниже EMA21 — медвежья зона
        score += 1
        factors.append(f"EMA9 ниже EMA21 (gap={ema_data['gap_pct']:+.2f}%)")

    # ── 10. ATR-перегрев ─────────────────────────────────────────────────────
    atr = None
    if len(highs) >= 15 and len(lows) >= 15:
        atr = calculate_atr(highs, lows, closes)
        if atr is not None and atr > 0:
            # Движение за период в единицах ATR
            period_move = abs(price - prices[max(0, len(prices) - 30)])
            atr_ratio   = period_move / atr
            if atr_ratio > REVERSAL_ATR_MULT:
                score += 1
                factors.append(f"ATR-перегрев ({atr_ratio:.1f}× ATR за период)")

    # ── 11. Свечной паттерн ───────────────────────────────────────────────────
    candle_pattern = None
    if len(closes) >= 3 and len(highs) >= 3 and len(lows) >= 3:
        candle_pattern = detect_candle_pattern(closes, highs, lows)
        if candle_pattern:
            score += 1
            factors.append(f"Свечной паттерн: {candle_pattern}")

    # ── 12. Слабый объём ─────────────────────────────────────────────────────
    vol_signal = calculate_volume_weakness(volumes) if volumes else calculate_volume_signal(sym)
    if vol_signal:
        score += 1
        factors.append(vol_signal)

    # ── Цели по шорту (уровни Фибоначчи от хая к лою 24h) ───────────────────
    fib = calculate_fibonacci_levels(high24, low24)
    target1 = fib["fib_382"]   # первая цель
    target2 = fib["fib_618"]   # вторая цель (более глубокая)

    # ── Контекст для уведомления ─────────────────────────────────────────────
    day_context = get_24h_context(sym, price)

    return {
        "score":          score,
        "factors":        factors,
        "triggered":      score >= min_score,
        "rsi":            rsi,
        "stoch_rsi":      stoch_rsi_val,
        "bb_pct":         bb_pct,
        "macd_hist":      macd_hist,
        "accel":          accel,
        "atr":            atr,
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

async def _tg_post(method: str, payload: dict) -> Optional[dict]:
    try:
        async with _session.post(
            f"{TG}/{method}", json=payload, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            data = await resp.json()
            if not data.get("ok"):
                log.warning("Telegram %s error: %s", method, data)
            return data
    except Exception as e:
        log.error("Telegram %s: %s", method, e)
        return None


async def send_message(text: str, chat_id, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
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
            # Строка 2 — Порог роста
            ["📈 0.2%", "📈 5%", "📈 10%", "📈 20%"],
            # Строка 3 — Временное окно
            ["⏱ 5 мин", "⏱ 1 час", "⏱ 4 ч", "⏱ 1 д"],
            # Строка 4 — Сигналы и история
            ["📋 История", "🏆 Топ-5", "📤 Экспорт"],
            # Строка 5 — Разворот
            ["🔄 Развороты", "⚙️ Настройки разворота", "🗑 Кулдауны"],
            # Строка 6 — База данных
            ["🗄 БД Статистика", "🧹 БД Очистка"],
            # Строка 7 — Порог разворота (быстрая настройка)
            ["🎚 Порог 3/12", "🎚 Порог 4/12", "🎚 Порог 5/12", "🎚 Порог 7/12"],
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
) -> str:
    """
    Уведомление о развороте на шорт.
    Включает: скор, факторы, индикаторы, ATR, свечной паттерн, цели по Фибо.
    """
    score   = rev["score"]
    factors = rev["factors"]
    rsi     = rev.get("rsi")
    stoch   = rev.get("stoch_rsi")
    bb_pct  = rev.get("bb_pct")
    macd_h  = rev.get("macd_hist")
    atr     = rev.get("atr")
    target1 = rev.get("target1")
    target2 = rev.get("target2")
    candle  = rev.get("candle_pattern")
    ema_gap = rev.get("ema_gap")
    vol_sig = rev.get("vol_signal", "")
    day_ctx = rev.get("day_context", "")

    # Уровень уверенности
    if score >= 8:
        confidence = "🔴 ВЫСОКАЯ"
        hdr        = "🚨"
    elif score >= 5:
        confidence = "🟠 СРЕДНЯЯ"
        hdr        = "⚠️"
    else:
        confidence = "🟡 СЛАБАЯ"
        hdr        = "🔄"

    rsi_s    = f"{rsi:.1f}"     if rsi    is not None else "—"
    stoch_s  = f"{stoch:.2f}"   if stoch  is not None else "—"
    bb_s     = f"{bb_pct:.2f}"  if bb_pct is not None else "—"
    macd_s   = f"{macd_h:+.6f}" if macd_h is not None else "—"
    atr_s    = f"{atr:.6f}"     if atr    is not None else "—"
    ema_s    = f"{ema_gap:+.2f}%" if ema_gap is not None else "—"

    factors_s = "\n".join(f"  {i+1}. {f}" for i, f in enumerate(factors)) if factors else "  —"

    t1_s = f"<code>{target1:.4g}</code>" if target1 else "—"
    t2_s = f"<code>{target2:.4g}</code>" if target2 else "—"

    candle_s  = f"\n🕯 Паттерн: <b>{candle}</b>"   if candle  else ""
    vol_s     = f"\n{vol_sig}"                       if vol_sig else ""
    day_ctx_s = f"\n{day_ctx}"                       if day_ctx else ""

    return (
        f"{hdr} <b>РАЗВОРОТ НА ШОРТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 Рост до разворота: <b>+{growth:.2f}%</b> за {_fmt_dur(duration_sec)}\n\n"
        f"🎯 Уверенность: {confidence}  [{score}/12]\n\n"
        f"<b>Сработавшие факторы:</b>\n{factors_s}\n\n"
        f"<b>Индикаторы:</b>\n"
        f"  RSI: <code>{rsi_s}</code>  │  StochRSI: <code>{stoch_s}</code>\n"
        f"  BB%: <code>{bb_s}</code>   │  MACD hist: <code>{macd_s}</code>\n"
        f"  ATR: <code>{atr_s}</code>  │  EMA gap: <code>{ema_s}</code>"
        f"{candle_s}"
        f"{vol_s}"
        f"{day_ctx_s}\n\n"
        f"<b>🎯 Цели шорта (Фибо):</b>\n"
        f"  Цель 1 (38.2%): {t1_s}\n"
        f"  Цель 2 (61.8%): {t2_s}\n\n"
        f"📋 <code>{sym}</code> #шорт"
    )


# ================================================================
#  SYMBOLS
# ================================================================

async def get_symbols() -> set[str]:
    symbols: set[str] = set()
    for label, url, suffixes in [
        ("Trading", "https://public.bybit.com/trading/", ("USDT", "PERP")),
        ("Spot",    "https://public.bybit.com/spot/",    ("USDT",)),
    ]:
        try:
            async with _session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                html = await r.text()
            soup  = BeautifulSoup(html, "html.parser")
            count = 0
            for a in soup.find_all("a"):
                sym = a.text.strip("/")
                if sym.endswith(suffixes):
                    symbols.add(sym.replace("/", ""))
                    count += 1
            log.info("%s: %d символов", label, count)
        except Exception as e:
            log.error("get_symbols %s: %s", label, e)
    log.info("Всего монет: %d", len(symbols))
    return symbols


# ================================================================
#  PRICES
# ================================================================

async def _fetch_okx(norm: dict) -> tuple[dict, dict]:
    prices, sources = {}, {}
    try:
        async with _session.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            data = await r.json()
        for item in data.get("data", []):
            sym   = normalize_symbol(item["instId"])
            price = float(item["last"])
            if price > 0 and sym in norm:
                real = norm[sym]
                prices[real]  = price
                sources[real] = "OKX"
    except Exception as e:
        log.error("OKX: %s", e)
    return prices, sources


async def _fetch_mexc(norm: dict) -> tuple[dict, dict]:
    prices, sources = {}, {}
    try:
        async with _session.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            data = await r.json()
        if data.get("success"):
            for item in data["data"]:
                sym   = normalize_symbol(item["symbol"])
                price = float(item["lastPrice"])
                if price > 0 and sym in norm:
                    real = norm[sym]
                    prices[real]  = price
                    sources[real] = "MEXC"
    except Exception as e:
        log.error("MEXC: %s", e)
    return prices, sources


_norm_cache: dict[str, str] = {}
_norm_symbols_key: frozenset = frozenset()


async def get_prices(symbols: set[str]) -> tuple[dict, dict]:
    global _norm_cache, _norm_symbols_key
    sym_key = frozenset(symbols)
    if sym_key != _norm_symbols_key:
        _norm_cache       = {normalize_symbol(s): s for s in symbols}
        _norm_symbols_key = sym_key
    norm = _norm_cache

    merged: dict = {}
    msrc:   dict = {}
    results = await asyncio.gather(_fetch_mexc(norm), _fetch_okx(norm), return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            log.error("Fetch error: %s", res)
            continue
        p, s = res
        for sym, price in p.items():
            if sym not in merged:
                merged[sym] = price
                msrc[sym]   = s[sym]
    return merged, msrc


# ================================================================
#  MONITOR
# ================================================================

async def monitor():
    global current_percent, current_window, signals_count, reversal_count, checks_count, last_check_time

    symbols          = await get_symbols()
    last_symbols_upd = time.time()
    _cache_load_levels()

    await broadcast("✅ <b>Бот запущен</b> (v12 — 12-факторный разворот)")

    while True:
        if monitor_paused:
            await asyncio.sleep(2)
            continue

        try:
            now             = time.time()
            checks_count   += 1
            last_check_time = now

            if PROM_AVAILABLE:
                PROM_CHECKS.inc()
                PROM_COINS.set(len(price_history))

            if now - last_symbols_upd >= 1800:
                new_sym = await get_symbols()
                if new_sym:
                    symbols = new_sym
                    log.info("Монеты обновлены: %d", len(symbols))
                last_symbols_upd = now

            prices, sources = await get_prices(symbols)

            for sym, price in prices.items():
                await asyncio.sleep(0)

                if price <= 0:
                    continue

                hist = price_history.setdefault(sym, [])
                hist.append((now, price))
                cutoff = max(current_window * 2, 86400)
                price_history[sym] = [(t, p) for t, p in hist if now - t <= cutoff]

                recent = [(t, p) for t, p in price_history[sym] if now - t <= current_window]
                source = sources.get(sym, "UNKNOWN")

                if len(recent) < MIN_SAMPLES:
                    continue

                old_price = recent[0][1]
                if old_price <= 0:
                    continue

                growth    = (price - old_price) / old_price * 100
                direction = 1 if growth > 0 else -1

                # ── Свечи MEXC (closes + highs + lows + volumes) ──────────────
                mexc_sym = _to_mexc_symbol(sym)
                klines   = await get_mexc_klines(mexc_sym, interval="Min1")
                closes   = klines["closes"]

                rsi = calculate_rsi(closes) if closes else None
                if rsi is None:
                    vals = [p for _, p in price_history[sym][-120:]]
                    rsi  = calculate_rsi(vals)

                # ════════════════════════════════════════════════════════════
                #  БЛОК 1: РАЗВОРОТ НА ШОРТ
                #  Условие: пиковый рост за 24ч >= 50% от порога
                #  (монета могла уже откатить, но разворот ещё актуален)
                # ════════════════════════════════════════════════════════════
                peak_growth = get_peak_growth_24h(sym, price)
                if peak_growth >= current_percent * 0.5 or growth >= current_percent * 0.5:
                    last_rev = _reversal_cooldown.get(sym, 0)
                    if now - last_rev >= REVERSAL_COOLDOWN_SEC:
                        rev = detect_short_reversal(
                            sym, price, klines, recent, growth, rsi, REVERSAL_MIN_SCORE
                        )
                        if rev["triggered"]:
                            duration = calculate_growth_duration(recent)
                            rev_text = format_reversal_alert(
                                sym, price, max(growth, peak_growth), source, rev,
                                duration_sec=duration,
                            )
                            _reversal_cooldown[sym] = now
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
                            log.info("REVERSAL %s score=%d/%d", sym, rev["score"], 12)
                            continue  # не дублируем обычным алертом

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
                rsi_trend = await get_rsi_trend_from_mexc(sym)
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

            await asyncio.sleep(config.INTERVAL)

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
            f"🚀 <b>Crypto Alert Bot v12</b>\n\n"
            f"📈 Порог роста: <b>{current_percent}%</b>\n"
            f"⏱ Период: <b>{current_window // 60} мин</b>\n"
            f"🔄 Порог разворота: <b>{REVERSAL_MIN_SCORE}/12 факторов</b>\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг активен'}\n\n"
            f"<b>Кнопки управления:</b>\n"
            f"  📈 0.2% / 5% / 10% / 20% — порог роста\n"
            f"  ⏱ 5 мин / 1 час / 4 ч / 1 д — период окна\n"
            f"  🎚 Порог 3-7/12 — чувствительность разворота\n"
            f"  ⚙️ Настройки разворота — все параметры\n"
            f"  🗄 БД Статистика / 🧹 БД Очистка — база\n\n"
            f"<b>Команды:</b>\n"
            f"  /set_percent 2.5 — произвольный порог (%)\n"
            f"  /set_window 60 — произвольный период (мин)\n"
            f"  /rev_score 4 — порог факторов разворота\n"
            f"  /rev_cooldown 5 — кулдаун разворота (мин)",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    _pct_map = {
        "📈 0.2%": 0.2, "📈 5%": 5.0, "📈 10%": 10.0, "📈 20%": 20.0,
    }
    if text in _pct_map:
        current_percent = _pct_map[text]
        await send_message(f"✅ Порог: <b>{current_percent}%</b>", chat_id)
        return

    _win_map = {
        "⏱ 5 мин": (300,   "5 мин"),
        "⏱ 1 час": (3600,  "1 час"),
        "⏱ 4 ч":   (14400, "4 часа"),
        "⏱ 1 д":   (86400, "1 день"),
    }
    if text in _win_map:
        current_window, label = _win_map[text]
        await send_message(f"✅ Период: <b>{label}</b>", chat_id)
        return

    if text == "⏸ Пауза":
        monitor_paused = True
        await send_message("⏸ Мониторинг приостановлен", chat_id)
        return

    if text == "▶️ Продолжить":
        monitor_paused = False
        await send_message("▶️ Мониторинг возобновлён", chat_id)
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
            f"🔄 Порог разворота: {REVERSAL_MIN_SCORE}/12\n"
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
        await send_message("🗑 Кулдауны и уровни сброшены", chat_id)
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
        await send_message(
            f"🧹 Удалено: {sum(deleted.values())} строк\n{deleted}\n"
            f"💾 Размер: {db_size_mb():.2f} МБ",
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
        await send_message(f"✅ Порог разворота: <b>{REVERSAL_MIN_SCORE}/12 факторов</b>", chat_id)
        return

    # ── Команды настройки разворота ───────────────────────────────────────────
    _rev_cmds = {
        "/rev_score":    ("REVERSAL_MIN_SCORE",   int,   1,    12,    "Порог факторов",           "/12"),
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
            REVERSAL_HIGH_MARGIN = 1.0 - val / 100
            await send_message(f"✅ Зона отбоя от хая: <b>{val}%</b>", chat_id)
        except Exception:
            await send_message("❌ /rev_high 0.2  (отступ от хая в %, 0–5)", chat_id)
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
        f_brief = factors[0] if factors else "—"
        t1_s    = f" → цель {r['target1']:.4g}" if r.get("target1") else ""
        lines.append(
            f"{lvl} <b>{r['symbol']}</b> [{score}/12] {ts}{t1_s}\n"
            f"   <i>{f_brief}</i>"
        )
    await send_message("\n".join(lines), chat_id)


async def _cmd_reversal_settings(chat_id):
    high_pct = round((1.0 - REVERSAL_HIGH_MARGIN) * 100, 2)
    await send_message(
        f"⚙️ <b>Настройки детектора разворота (12 факторов)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Скоринг:</b>\n"
        f"  Порог:         <code>{REVERSAL_MIN_SCORE}/12</code>    → /rev_score 4\n\n"
        f"<b>Пороги факторов:</b>\n"
        f"  RSI OB:        <code>&gt; {REVERSAL_RSI_OB}</code>       → /rev_rsi 70\n"
        f"  StochRSI:      <code>&gt; {REVERSAL_STOCH_OB}</code>    → /rev_stoch 0.80\n"
        f"  Боллинджер:    <code>&gt; {REVERSAL_BB_OB}</code>       → /rev_bb 1.0\n"
        f"  Замедление:    <code>&lt; {REVERSAL_ACCEL}</code>       → /rev_accel 0.5\n"
        f"  Моментум:      <code>&lt; {REVERSAL_MOMENTUM}%</code>  → /rev_momentum -0.5\n"
        f"  ATR-перегрев:  <code>&gt; {REVERSAL_ATR_MULT}×</code>  → /rev_atr 3.0\n"
        f"  Объём слабый:  <code>&lt; {REVERSAL_VOL_RATIO:.0%}</code>   → /rev_vol 0.7\n"
        f"  Зона хая 24h:  <code>{high_pct}%</code>             → /rev_high 0.2\n\n"
        f"<b>Кулдаун:</b>\n"
        f"  Между сигналами: <code>{REVERSAL_COOLDOWN_SEC // 60} мин</code>  → /rev_cooldown 5\n\n"
        f"<b>Пресеты:</b>\n"
        f"  Агрессивный: /rev_score 3  /rev_rsi 65  /rev_cooldown 2\n"
        f"  Строгий:     /rev_score 7  /rev_rsi 75  /rev_cooldown 15",
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
            params={"timeout": 30, "offset": offset},
            timeout=aiohttp.ClientTimeout(total=35),
        ) as resp:
            data = await resp.json()
        if data.get("ok"):
            return data["result"]
    except Exception as e:
        log.error("get_updates: %s", e)
    return []


async def telegram_loop():
    global offset
    while True:
        try:
            for update in await get_updates():
                offset = update["update_id"] + 1
                if "message" in update:
                    asyncio.create_task(handle_message(update["message"]))
        except Exception as e:
            log.exception("telegram_loop: %s", e)
        await asyncio.sleep(0.2)


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

WATCHDOG_TIMEOUT = 90


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

    _shutdown_event = asyncio.Event()
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

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        _session     = session
        monitor_task = asyncio.create_task(monitor())

        tasks = [
            monitor_task,
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
