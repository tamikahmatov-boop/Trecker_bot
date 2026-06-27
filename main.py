"""
Crypto Alert Bot — v11
Улучшения vs v10:
  • Детектор разворота на шорт с многофакторным скорингом:
      - RSI перекупленность (>70) + дивергенция (цена растёт, RSI падает)
      - MACD: крест вниз (histogram был + стал -) + slope гистограммы
      - Замедление роста (accel < 0.5) после сильного движения
      - Отбой от 24h High (цена ≥ хай и начала падать)
      - Свечной паттерн: последние тики показывают разворот
      - Объём: рост без объёма = слабый сигнал (по 24h vol)
      - Стохастик RSI — дополнительный осциллятор зоны перекупленности
      - Боллинджер: цена выше верхней полосы = зона перегрева
  • Три отдельных типа уведомлений:
      🔄 РАЗВОРОТ НА ШОРТ — многофакторный скоринг, минимум 3 из 7 факторов
      🚀 РОСТ — отдельное уведомление при росте выше порога
      📉 ПАДЕНИЕ — отдельное уведомление при падении выше порога
  • Кулдаун разворота независим от кулдауна обычных алертов
  • /reversal_stats — статистика точности разворотных сигналов
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
from contextlib import asynccontextmanager, contextmanager
from typing import Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD
from ta.volatility import BollingerBands

import config

# ── Prometheus (опционально) ──────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    PROM_SIGNALS   = Counter("bot_signals_total",      "Всего сигналов")
    PROM_CHECKS    = Counter("bot_checks_total",        "Всего циклов")
    PROM_COINS     = Gauge("bot_tracked_coins",         "Монет в истории")
    PROM_REVERSALS = Counter("bot_reversals_total",     "Разворотных сигналов")
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
_db_lock = asyncio.Lock()


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
                source      TEXT,
                ts          REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rev_symbol ON reversal_signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_rev_ts     ON reversal_signals(ts);
        """)


db_init()

DB_KEEP_ALERTS_DAYS    = getattr(config, "DB_KEEP_ALERTS_DAYS",    30)
DB_KEEP_LEVELS_DAYS    = getattr(config, "DB_KEEP_LEVELS_DAYS",     7)
DB_VACUUM_INTERVAL_H   = getattr(config, "DB_VACUUM_INTERVAL_H",   24)


def db_save_alert(symbol: str, price: float, growth: float,
                  rsi: Optional[float], macd: Optional[float], source: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, macd, source, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, price, growth, rsi, macd, source, time.time()),
        )


def db_save_reversal(symbol: str, price: float, score: int, factors: list[str],
                     rsi: Optional[float], macd: Optional[float],
                     stoch_rsi: Optional[float], bb_pct: Optional[float], source: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO reversal_signals "
            "(symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, source, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (symbol, price, score, json.dumps(factors, ensure_ascii=False),
             rsi, macd, stoch_rsi, bb_pct, source, time.time()),
        )


def db_recent_reversals(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, source, ts "
            "FROM reversal_signals ORDER BY ts DESC LIMIT ?",
            (limit,),
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


def db_get_alert_level(symbol: str) -> Optional[dict]:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT alert_price, direction, ts FROM alert_levels WHERE symbol=?", (symbol,)
        ).fetchone()
    return dict(row) if row else None


def db_set_alert_level(symbol: str, alert_price: float, direction: int):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO alert_levels (symbol, alert_price, direction, ts) "
            "VALUES (?, ?, ?, ?)",
            (symbol, alert_price, direction, time.time()),
        )


def db_clear_alert_levels():
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels")


def db_clear_alert_level(symbol: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels WHERE symbol=?", (symbol,))


def db_cleanup() -> dict:
    now     = time.time()
    deleted = {}
    with db_connect() as conn:
        cutoff_alerts = now - DB_KEEP_ALERTS_DAYS * 86400
        cur = conn.execute("DELETE FROM alerts WHERE ts < ?", (cutoff_alerts,))
        deleted["alerts"] = cur.rowcount

        cutoff_levels = now - DB_KEEP_LEVELS_DAYS * 86400
        cur = conn.execute("DELETE FROM alert_levels WHERE ts < ?", (cutoff_levels,))
        deleted["alert_levels"] = cur.rowcount

        cutoff_stats = now - 86400
        cur = conn.execute("DELETE FROM price_stats WHERE updated < ?", (cutoff_stats,))
        deleted["price_stats"] = cur.rowcount

        # Разворотные сигналы храним 14 дней
        cutoff_rev = now - 14 * 86400
        cur = conn.execute("DELETE FROM reversal_signals WHERE ts < ?", (cutoff_rev,))
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
        alerts_count   = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        oldest         = conn.execute("SELECT MIN(ts) FROM alerts").fetchone()[0]
        levels_count   = conn.execute("SELECT COUNT(*) FROM alert_levels").fetchone()[0]
        rev_count      = conn.execute("SELECT COUNT(*) FROM reversal_signals").fetchone()[0]
    oldest_s = time.strftime("%d.%m.%Y", time.localtime(oldest)) if oldest else "—"
    return {
        "alerts":       alerts_count,
        "oldest_alert": oldest_s,
        "levels":       levels_count,
        "reversals":    rev_count,
        "size_mb":      db_size_mb(),
    }


def db_recent_alerts(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, growth, rsi, macd, source, ts "
            "FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def db_top_signals(limit: int = 5) -> list[dict]:
    cutoff = time.time() - 86400
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, growth, price, source, ts "
            "FROM alerts WHERE ts >= ? ORDER BY ABS(growth) DESC LIMIT ?",
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
            "INSERT OR IGNORE INTO subscribers (chat_id, added_ts) VALUES (?, ?)",
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

monitor_paused  = False
monitor_task:   asyncio.Task | None = None

_session: aiohttp.ClientSession | None = None

_alert_cooldown:    dict[str, float] = {}
_reversal_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SEC:    int = getattr(config, "ALERT_COOLDOWN_SEC", 60)
REVERSAL_COOLDOWN_SEC: int = getattr(config, "REVERSAL_COOLDOWN_SEC", 300)  # 5 мин

_levels_cache: dict[str, dict] = {}

# Минимальный скоринг для отправки разворотного сигнала (из 8 возможных факторов)
REVERSAL_MIN_SCORE: int = getattr(config, "REVERSAL_MIN_SCORE", 3)


def _cache_load_levels():
    global _levels_cache
    with db_connect() as conn:
        rows = conn.execute("SELECT symbol, alert_price, direction FROM alert_levels").fetchall()
    _levels_cache = {r["symbol"]: {"alert_price": r["alert_price"], "direction": r["direction"]} for r in rows}
    log.info("Загружено уровней в кэш: %d", len(_levels_cache))


def _cache_get_level(symbol: str) -> dict | None:
    return _levels_cache.get(symbol)


def _cache_set_level(symbol: str, alert_price: float, direction: int):
    _levels_cache[symbol] = {"alert_price": alert_price, "direction": direction}
    db_set_alert_level(symbol, alert_price, direction)


def _cache_clear_level(symbol: str):
    _levels_cache.pop(symbol, None)
    db_clear_alert_level(symbol)


def _cache_clear_all():
    _levels_cache.clear()
    db_clear_alert_levels()


def normalize_symbol(sym: str) -> str:
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")


# ================================================================
#  MEXC KLINE CACHE
# ================================================================

_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL = 60
KLINE_LIMIT     = 100


async def _fetch_mexc_klines(symbol: str, interval: str = "Min1") -> list[float]:
    try:
        async with _session.get(
            "https://contract.mexc.com/api/v1/contract/kline",
            params={"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            data = await r.json()
        if data.get("success") and data.get("data"):
            closes = [float(c) for c in data["data"]["close"]]
            return closes
    except Exception as e:
        log.debug("MEXC kline %s: %s", symbol, e)
    return []


async def get_mexc_closes(symbol: str, interval: str = "Min1") -> list[float]:
    now    = time.time()
    cached = _kline_cache.get(symbol)
    if cached and now - cached["ts"] < KLINE_CACHE_TTL:
        return cached["closes"]
    closes = await _fetch_mexc_klines(symbol, interval)
    if closes:
        _kline_cache[symbol] = {"ts": now, "closes": closes}
    return closes


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
    """
    Стохастик RSI — показывает позицию RSI в его собственном диапазоне.
    Значения > 0.8 — зона перекупленности (сильный сигнал разворота).
    """
    try:
        if len(prices) < window * 2 + 1:
            return None
        s = pd.Series(prices)
        # Считаем RSI
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < window:
            return None
        # Стохастик поверх RSI
        rsi_min = rsi_series.rolling(window).min().iloc[-1]
        rsi_max = rsi_series.rolling(window).max().iloc[-1]
        if pd.isna(rsi_min) or pd.isna(rsi_max) or (rsi_max - rsi_min) == 0:
            return None
        stoch = (rsi_series.iloc[-1] - rsi_min) / (rsi_max - rsi_min)
        return round(float(stoch), 4)
    except Exception as e:
        log.error("StochRSI error: %s", e)
        return None


def calculate_bollinger_pct(prices: list[float], window: int = 20) -> Optional[float]:
    """
    Позиция цены в полосах Боллинджера (%B).
    >1.0 = выше верхней полосы (перегрев),  0.5 = середина,  <0 = ниже нижней.
    """
    try:
        if len(prices) < window:
            return None
        s  = pd.Series(prices)
        bb = BollingerBands(close=s, window=window, window_dev=2)
        upper = bb.bollinger_hband().iloc[-1]
        lower = bb.bollinger_lband().iloc[-1]
        if pd.isna(upper) or pd.isna(lower) or (upper - lower) == 0:
            return None
        pct = (prices[-1] - lower) / (upper - lower)
        return round(float(pct), 4)
    except Exception as e:
        log.error("BB error: %s", e)
        return None


def calculate_macd_full(prices: list[float]) -> dict:
    """
    Возвращает словарь с полными данными MACD:
      histogram     — текущее значение гистограммы
      histogram_prev — предыдущее значение (для определения пересечения)
      slope         — наклон гистограммы за последние 3 бара (убывает = медвежий)
      cross_down    — True если только что произошёл медвежий крест (+ → -)
    """
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
        # Наклон: разница между последним и 3 бара назад
        result["slope"]          = round(float(hist.iloc[-1] - hist.iloc[-3]), 6)
        # Медвежий крест: предыдущий бар был положительным, текущий отрицательный
        result["cross_down"]     = (hist.iloc[-2] > 0 and hist.iloc[-1] < 0)
    except Exception as e:
        log.error("MACD full error: %s", e)
    return result


def calculate_rsi_divergence(prices: list[float], window: int = 14, lookback: int = 10) -> bool:
    """
    Медвежья дивергенция: цена делает новый максимум, а RSI — нет.
    Это один из самых сильных сигналов разворота.
    """
    try:
        if len(prices) < window + lookback + 1:
            return False
        s          = pd.Series(prices)
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < lookback:
            return False

        # Цена: текущий максимум vs максимум lookback назад
        price_now  = prices[-1]
        price_prev = max(prices[-(lookback + 1):-1])

        # RSI: текущий vs максимум RSI за lookback баров
        rsi_now    = float(rsi_series.iloc[-1])
        rsi_prev   = float(rsi_series.iloc[-(lookback + 1):].max())

        # Дивергенция: цена выше, RSI ниже
        divergence = (price_now > price_prev) and (rsi_now < rsi_prev - 2)
        return divergence
    except Exception:
        return False


def calculate_price_momentum(prices: list[float], fast: int = 5, slow: int = 20) -> Optional[float]:
    """
    Моментум: скорость изменения цены. Отрицательный и убывающий = медвежий.
    Возвращает разницу между быстрым и медленным моментумом.
    """
    try:
        if len(prices) < slow + 1:
            return None
        mom_fast = (prices[-1] - prices[-fast]) / prices[-fast] * 100
        mom_slow = (prices[-1] - prices[-slow]) / prices[-slow] * 100
        return round(mom_fast - mom_slow, 4)
    except Exception:
        return None


def calculate_volume_signal(sym: str) -> Optional[str]:
    """
    Анализ 24h объёма: рост цены без роста объёма — слабый, ложный пробой.
    Возвращает строку-описание или None.
    """
    hist = price_history.get(sym, [])
    now  = time.time()
    if len(hist) < 20:
        return None
    # Сравниваем скорость роста цены в первой и второй половине последнего часа
    hour_prices = [p for t, p in hist if now - t <= 3600]
    if len(hour_prices) < 10:
        return None
    mid    = len(hour_prices) // 2
    move1  = abs(hour_prices[mid - 1] - hour_prices[0])   / hour_prices[0]   * 100
    move2  = abs(hour_prices[-1]       - hour_prices[mid]) / hour_prices[mid]  * 100
    # Во второй половине движение резко замедлилось
    if move1 > 0.5 and move2 < move1 * 0.3:
        return "📊 Движение без импульса (иссякание)"
    return None


async def get_rsi_from_mexc(symbol: str, window: int = 14) -> Optional[float]:
    mexc_sym = _to_mexc_symbol(symbol)
    closes   = await get_mexc_closes(mexc_sym, interval="Min1")
    if not closes:
        return None
    return calculate_rsi(closes, window=window)


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
        s    = pd.Series(prices)
        rsi  = RSIIndicator(close=s, window=window).rsi().dropna()
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
        mid   = len(recent) // 2
        half1 = recent[:mid]
        half2 = recent[mid:]
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


# ================================================================
#  REVERSAL DETECTOR  (многофакторный анализ разворота на шорт)
# ================================================================

def detect_short_reversal(
    sym:       str,
    price:     float,
    closes:    list[float],   # свечи MEXC (до 100 баров)
    recent:    list[tuple[float, float]],  # тики за текущее окно
    growth:    float,         # текущий рост в % за окно
    rsi:       Optional[float],
    min_score: int = 3,
) -> dict:
    """
    Многофакторный детектор разворота на шорт.

    Анализирует 8 независимых факторов, каждый даёт +1 к скору.
    Порог срабатывания: REVERSAL_MIN_SCORE (по умолчанию 3 из 8).

    Факторы:
      1. RSI перекупленность (> 70)
      2. Стохастик RSI в зоне перекупленности (> 0.80)
      3. Цена выше верхней полосы Боллинджера (%B > 1.0)
      4. MACD медвежий крест (гистограмма пересекла 0 вниз) ИЛИ slope < 0 при положительной гистограмме
      5. Медвежья RSI-дивергенция (цена выше, RSI ниже предыдущего пика)
      6. Замедление роста (accel < 0.5 после движения > порога)
      7. Отбой от 24h High (цена ≥ хай дня, и последние 3 тика вниз)
      8. Моментум иссякает (fast momentum < slow momentum, разница < -0.5)

    Возвращает dict:
      score     — количество сработавших факторов
      factors   — список описаний сработавших факторов
      triggered — True если score >= REVERSAL_MIN_SCORE
      rsi, stoch_rsi, bb_pct, macd_hist — значения индикаторов для отчёта
    """
    score   = 0
    factors = []

    # Используем closes (реальные свечи) если доступны, иначе тики
    prices = closes if len(closes) >= 30 else [p for _, p in price_history.get(sym, [])[-100:]]

    # ── 1. RSI перекупленность ────────────────────────────────────────────────
    stoch_rsi_val = None
    if rsi is not None and rsi > 70:
        score += 1
        factors.append(f"RSI перекуплен ({rsi:.1f} > 70)")

        # ── 2. Стохастик RSI ──────────────────────────────────────────────────
        stoch_rsi_val = calculate_stoch_rsi(prices)
        if stoch_rsi_val is not None and stoch_rsi_val > 0.80:
            score += 1
            factors.append(f"StochRSI в перекупленности ({stoch_rsi_val:.2f} > 0.80)")
    elif rsi is not None:
        # Стохастик RSI считаем в любом случае
        stoch_rsi_val = calculate_stoch_rsi(prices)
        if stoch_rsi_val is not None and stoch_rsi_val > 0.85:
            # Экстремальная зона стохастика даже без RSI > 70
            score += 1
            factors.append(f"StochRSI экстремум ({stoch_rsi_val:.2f} > 0.85)")

    # ── 3. Боллинджер %B > 1.0 (цена выше верхней полосы) ───────────────────
    bb_pct = calculate_bollinger_pct(prices)
    if bb_pct is not None and bb_pct > 1.0:
        score += 1
        factors.append(f"Цена выше BB ({bb_pct:.2f}x верхней полосы)")
    elif bb_pct is not None and bb_pct > 0.95:
        # Близко к верхней полосе — слабый сигнал, не считаем
        pass

    # ── 4. MACD медвежий крест или нарастающий нисходящий slope ─────────────
    macd_data = calculate_macd_full(prices)
    macd_hist = macd_data["histogram"]
    if macd_data["cross_down"]:
        score += 1
        factors.append("MACD медвежий крест (гистограмма пробила 0 вниз)")
    elif (macd_data["histogram"] is not None
          and macd_data["slope"] is not None
          and macd_data["histogram"] > 0
          and macd_data["slope"] < -0.000005):
        score += 1
        factors.append(f"MACD гистограмма убывает (slope={macd_data['slope']:+.6f})")

    # ── 5. Медвежья RSI-дивергенция ──────────────────────────────────────────
    if len(prices) >= 30:
        divergence = calculate_rsi_divergence(prices)
        if divergence:
            score += 1
            factors.append("⚡ Медвежья RSI-дивергенция (цена ↑, RSI ↓)")

    # ── 6. Замедление роста (accel) ───────────────────────────────────────────
    accel = calculate_acceleration(recent)
    if accel is not None and growth >= current_percent and accel < 0.5:
        score += 1
        factors.append(f"Замедление импульса (accel={accel:.2f}x)")

    # ── 7. Отбой от 24h High ─────────────────────────────────────────────────
    hist      = price_history.get(sym, [])
    now_ts    = time.time()
    day_prices = [p for t, p in hist if now_ts - t <= 86400]
    if len(day_prices) >= 20:
        high24 = max(day_prices)
        near_high = price >= high24 * 0.998  # в пределах 0.2% от хая
        if near_high and len(recent) >= 4:
            last_ticks = [p for _, p in recent[-4:]]
            turning_down = all(last_ticks[i] >= last_ticks[i + 1] for i in range(len(last_ticks) - 1))
            if turning_down:
                score += 1
                pct_from_high = (price - high24) / high24 * 100
                factors.append(f"Отбой от 24h High ({pct_from_high:+.2f}% от хая)")

    # ── 8. Моментум иссякает ─────────────────────────────────────────────────
    momentum = calculate_price_momentum(prices)
    if momentum is not None and momentum < -0.5:
        score += 1
        factors.append(f"Моментум иссякает (fast-slow={momentum:+.2f}%)")

    # ── Дополнительный контекст иссякания объёма (не влияет на скор) ─────────
    vol_signal = calculate_volume_signal(sym)

    return {
        "score":       score,
        "factors":     factors,
        "triggered":   score >= min_score,
        "rsi":         rsi,
        "stoch_rsi":   stoch_rsi_val,
        "bb_pct":      bb_pct,
        "macd_hist":   macd_hist,
        "accel":       accel,
        "vol_signal":  vol_signal,
    }


# ================================================================
#  TELEGRAM HELPERS
# ================================================================

async def _tg_post(method: str, payload: dict) -> Optional[dict]:
    global _session
    try:
        async with _session.post(f"{TG}/{method}", json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
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
        async with _session.post(f"{TG}/sendDocument", data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json()
    except Exception as e:
        log.error("sendDocument: %s", e)


async def broadcast(text: str, reply_markup=None):
    tasks = [send_message(text, cid, reply_markup) for cid in db_get_subscribers()]
    await asyncio.gather(*tasks, return_exceptions=True)


def reply_keyboard():
    return {
        "keyboard": [
            ["📈 0.2%",    "📈 5%",       "📈 10%"          ],
            ["📈 15%",     "📈 20%"                          ],
            ["⏱ 5 мин",   "⏱ 1 час",    "⏱ 4 ч", "⏱ 1 д" ],
            ["📊 Статус",  "📋 История",  "🏆 Топ-5"         ],
            ["⏸ Пауза",   "▶️ Продолжить"                   ],
            ["📤 Экспорт", "🗑 Кулдауны", "🔄 Развороты"     ],
        ],
        "resize_keyboard": True,
        "persistent":       True,
    }


async def send_main_menu(chat_id):
    await send_message("⚙️ <b>Панель управления</b>", chat_id, reply_markup=reply_keyboard())


# ================================================================
#  ALERT FORMATTING
# ================================================================

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
    accel: Optional[float] = None,
    duration_sec: int = 0,
    breakout: Optional[str] = None,
    day_context: Optional[str] = None,
) -> str:
    """Уведомление о росте цены."""
    emoji = alert_emoji(growth)
    label = "Рост" if growth > 0 else "Падение"
    sign  = "+" if growth > 0 else ""

    rsi_s    = f"{rsi:.1f}" if rsi is not None else "—"
    rsi_hint = ""
    if rsi is not None:
        if rsi >= 70:
            rsi_hint = " ⚠️ перекуплен"
        elif rsi <= 30:
            rsi_hint = " ⚠️ перепродан"
    rsi_trend_s = f" {rsi_trend}" if rsi_trend else ""

    macd_s = f"{macd:+.6f}" if macd is not None else "—"

    accel_s = ""
    if accel is not None:
        if accel >= 2.0:
            accel_s = f"\n⚡ Ускорение: <b>×{accel:.1f}</b> 🔥"
        elif accel >= 1.3:
            accel_s = f"\n⚡ Ускорение: ×{accel:.1f}"
        elif accel < 0.7:
            accel_s = f"\n🐢 Замедление: ×{accel:.1f}"

    if duration_sec >= 3600:
        dur_s = f"{duration_sec // 3600}ч {(duration_sec % 3600) // 60}м"
    elif duration_sec >= 60:
        dur_s = f"{duration_sec // 60}м"
    else:
        dur_s = f"{duration_sec}с"

    breakout_s    = f"\n{breakout}"    if breakout    else ""
    day_context_s = f"\n{day_context}" if day_context else ""

    return (
        f"{emoji} <b>СИГНАЛ</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 {label}: <b>{sign}{growth:.2f}%</b> за {dur_s}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_trend_s}{rsi_hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{accel_s}"
        f"{breakout_s}"
        f"{day_context_s}"
        f"\n\n📋 <code>{sym}</code>"
    )


# Псевдоним для обратной совместимости
format_alert = format_growth_alert


def format_reversal_alert(
    sym:       str,
    price:     float,
    growth:    float,
    source:    str,
    rev:       dict,           # результат detect_short_reversal
    duration_sec: int = 0,
    day_context: Optional[str] = None,
) -> str:
    """
    Уведомление о развороте на шорт — отдельный тип, отдельное форматирование.
    Показывает скор, все сработавшие факторы и значения индикаторов.
    """
    score   = rev["score"]
    factors = rev["factors"]
    rsi     = rev.get("rsi")
    stoch   = rev.get("stoch_rsi")
    bb_pct  = rev.get("bb_pct")
    macd_h  = rev.get("macd_hist")
    vol_sig = rev.get("vol_signal", "")

    # Уровень уверенности по скору
    if score >= 6:
        confidence = "🔴 ВЫСОКАЯ"
        hdr_emoji  = "🚨"
    elif score >= 4:
        confidence = "🟠 СРЕДНЯЯ"
        hdr_emoji  = "⚠️"
    else:
        confidence = "🟡 СЛАБАЯ"
        hdr_emoji  = "🔄"

    if duration_sec >= 3600:
        dur_s = f"{duration_sec // 3600}ч {(duration_sec % 3600) // 60}м"
    elif duration_sec >= 60:
        dur_s = f"{duration_sec // 60}м"
    else:
        dur_s = f"{duration_sec}с"

    rsi_s   = f"{rsi:.1f}"   if rsi    is not None else "—"
    stoch_s = f"{stoch:.2f}" if stoch  is not None else "—"
    bb_s    = f"{bb_pct:.2f}" if bb_pct is not None else "—"
    macd_s  = f"{macd_h:+.6f}" if macd_h is not None else "—"

    factors_s = "\n".join(f"  • {f}" for f in factors) if factors else "  —"

    vol_s       = f"\n{vol_sig}" if vol_sig else ""
    day_ctx_s   = f"\n{day_context}" if day_context else ""

    return (
        f"{hdr_emoji} <b>РАЗВОРОТ НА ШОРТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 Рост до разворота: <b>+{growth:.2f}%</b> за {dur_s}\n\n"
        f"🎯 Уверенность: {confidence}  [{score}/8]\n\n"
        f"<b>Сработавшие факторы:</b>\n"
        f"{factors_s}\n\n"
        f"<b>Индикаторы:</b>\n"
        f"  RSI: <code>{rsi_s}</code>  │  StochRSI: <code>{stoch_s}</code>\n"
        f"  BB%: <code>{bb_s}</code>  │  MACD hist: <code>{macd_s}</code>"
        f"{vol_s}"
        f"{day_ctx_s}"
        f"\n\n📋 <code>{sym}</code> #шорт"
    )


def format_drop_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    duration_sec: int = 0,
    breakout: Optional[str] = None,
    day_context: Optional[str] = None,
) -> str:
    """Отдельное уведомление о падении цены."""
    a = abs(growth)
    if a >= 20:
        emoji = "💥"
    elif a >= 10:
        emoji = "🔥"
    else:
        emoji = "📉"

    rsi_s    = f"{rsi:.1f}" if rsi is not None else "—"
    rsi_hint = " ⚠️ перепродан" if (rsi is not None and rsi <= 30) else ""
    rsi_trend_s = f" {rsi_trend}" if rsi_trend else ""
    macd_s   = f"{macd:+.6f}" if macd is not None else "—"

    if duration_sec >= 3600:
        dur_s = f"{duration_sec // 3600}ч {(duration_sec % 3600) // 60}м"
    elif duration_sec >= 60:
        dur_s = f"{duration_sec // 60}м"
    else:
        dur_s = f"{duration_sec}с"

    breakout_s    = f"\n{breakout}"    if breakout    else ""
    day_context_s = f"\n{day_context}" if day_context else ""

    return (
        f"{emoji} <b>ПАДЕНИЕ</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📉 Падение: <b>{growth:.2f}%</b> за {dur_s}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_trend_s}{rsi_hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{breakout_s}"
        f"{day_context_s}"
        f"\n\n📋 <code>{sym}</code> #падение"
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
                html  = await r.text()
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
    norm   = _norm_cache
    merged: dict = {}
    msrc:   dict = {}

    results = await asyncio.gather(
        _fetch_mexc(norm),
        _fetch_okx(norm),
        return_exceptions=True,
    )

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

    await broadcast("✅ <b>Бот запущен</b> (v11 — разворот на шорт активен)")

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

                recent    = [(t, p) for t, p in price_history[sym] if now - t <= current_window]
                source    = sources.get(sym, "UNKNOWN")

                if len(recent) < MIN_SAMPLES:
                    continue

                old_price = recent[0][1]
                if old_price <= 0:
                    continue

                growth    = (price - old_price) / old_price * 100
                direction = 1 if growth > 0 else -1

                # ── RSI по реальным свечам MEXC ──────────────────────────────
                mexc_sym = _to_mexc_symbol(sym)
                closes   = await get_mexc_closes(mexc_sym, interval="Min1")
                rsi = calculate_rsi(closes) if closes else None
                if rsi is None:
                    vals = [p for _, p in price_history[sym][-100:]]
                    rsi  = calculate_rsi(vals)

                # ════════════════════════════════════════════════════════════
                #  БЛОК 1: РАЗВОРОТ НА ШОРТ
                #  Проверяем независимо от порога роста — монета могла уже
                #  вырасти ранее и сейчас даёт сигнал разворота.
                # ════════════════════════════════════════════════════════════
                if growth >= current_percent * 0.5:  # был хоть какой-то рост
                    last_rev = _reversal_cooldown.get(sym, 0)
                    if now - last_rev >= REVERSAL_COOLDOWN_SEC:
                        rev = detect_short_reversal(sym, price, closes, recent, growth, rsi, REVERSAL_MIN_SCORE)
                        if rev["triggered"]:
                            duration  = calculate_growth_duration(recent)
                            day_ctx   = get_24h_context(sym, price)
                            rev_text  = format_reversal_alert(
                                sym, price, growth, source, rev,
                                duration_sec=duration,
                                day_context=day_ctx,
                            )
                            _reversal_cooldown[sym] = now
                            db_save_reversal(
                                sym, price, rev["score"], rev["factors"],
                                rev["rsi"], rev["macd_hist"],
                                rev["stoch_rsi"], rev["bb_pct"], source,
                            )
                            await broadcast(rev_text)
                            reversal_count += 1
                            if PROM_AVAILABLE:
                                PROM_REVERSALS.inc()
                            log.info("REVERSAL %s score=%d factors=%s", sym, rev["score"], rev["factors"])
                            # После разворотного сигнала пропускаем обычный алерт роста
                            continue

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
                        log.debug("Skip %s: growth but RSI=%.1f < 50", sym, rsi)
                        continue
                    if direction == -1 and rsi > 50:
                        log.debug("Skip %s: drop but RSI=%.1f > 50", sym, rsi)
                        continue

                # Кулдаун обычных алертов
                last_sent = _alert_cooldown.get(sym, 0)
                if now - last_sent < ALERT_COOLDOWN_SEC:
                    log.debug("Cooldown skip %s (%.0f с назад)", sym, now - last_sent)
                    continue

                # Проверка уровня (повторный алерт)
                level = _cache_get_level(sym)
                if level:
                    prev_price = level["alert_price"]
                    prev_dir   = level["direction"]
                    if prev_dir == direction:
                        step = abs(price - prev_price) / prev_price * 100
                        if step < current_percent:
                            continue

                # Вычисляем контекст
                vals      = [p for _, p in price_history[sym][-100:]]
                macd_data = calculate_macd_full(vals)
                macd      = macd_data["histogram"]
                rsi_trend = await get_rsi_trend_from_mexc(sym)
                accel     = calculate_acceleration(recent)
                duration  = calculate_growth_duration(recent)
                breakout  = check_24h_breakout(sym, price)
                day_ctx   = get_24h_context(sym, price)

                # Отдельное форматирование для роста и падения
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
#  COMMAND / CALLBACK HANDLERS
# ================================================================

async def handle_message(msg: dict):
    global current_percent, current_window, monitor_paused, REVERSAL_MIN_SCORE

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
            f"🚀 <b>Бот запущен</b>\n\n"
            f"📈 Порог: <b>{current_percent}%</b>\n"
            f"⏱ Период: <b>{current_window // 60} мин</b>\n"
            f"🔄 Разворот: минимум {REVERSAL_MIN_SCORE}/8 факторов\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг идёт'}",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    _pct_map = {
        "📈 0.2%": 0.2, "📈 5%": 5.0, "📈 10%": 10.0,
        "📈 15%": 15.0, "📈 20%": 20.0,
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
        if monitor_paused:
            await send_message("⏸ Мониторинг уже на паузе", chat_id)
        else:
            monitor_paused = True
            await send_message("⏸ Мониторинг приостановлен", chat_id)
        return

    if text == "▶️ Продолжить":
        if not monitor_paused:
            await send_message("▶️ Мониторинг уже активен", chat_id)
        else:
            monitor_paused = False
            await send_message("▶️ Мониторинг возобновлён", chat_id)
        return

    if text in ("📊 Статус", "/status"):
        uptime = int(time.time() - start_time)
        d, rem = divmod(uptime, 86400)
        h, rem = divmod(rem, 3600)
        m      = rem // 60
        await send_message(
            f"📊 <b>СТАТУС</b>\n\n"
            f"🟢 Аптайм: {d}д {h}ч {m}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"🔔 Сигналов роста/падения: {signals_count}\n"
            f"🔄 Разворотных сигналов: {reversal_count}\n"
            f"🔄 Порог разворота: {REVERSAL_MIN_SCORE}/8 факторов\n"
            f"🔄 Кулдаун разворота: {REVERSAL_COOLDOWN_SEC // 60} мин\n"
            f"🔄 Циклов: {checks_count}\n"
            f"📈 Порог: <b>{current_percent}%</b>\n"
            f"⏱ Период: <b>{current_window // 60} мин</b>\n"
            f"⚡ Интервал: {config.INTERVAL} сек\n"
            f"🕒 Кулдаун алертов: {ALERT_COOLDOWN_SEC // 60} мин\n"
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
        await send_message("📤 Генерирую CSV...", chat_id)
        asyncio.create_task(_cmd_export(chat_id))
        return

    if text in ("🗑 Кулдауны", "/clear_cooldowns"):
        _cache_clear_all()
        _reversal_cooldown.clear()
        await send_message("🗑 Уровни алертов и кулдауны разворота сброшены", chat_id)
        return

    # ── Последние разворотные сигналы ────────────────────────────────────────
    if text in ("🔄 Развороты", "/reversals"):
        asyncio.create_task(_cmd_reversals(chat_id))
        return

    # ── Настройка порога разворота: /set_reversal_score 3 ────────────────────
    if text.startswith("/set_reversal_score"):
        global REVERSAL_MIN_SCORE
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 8
            REVERSAL_MIN_SCORE = val
            await send_message(f"✅ Порог разворота: <b>{val}/8 факторов</b>", chat_id)
        except Exception:
            await send_message("❌ Использование: /set_reversal_score 3  (от 1 до 8)", chat_id)
        return

    if text == "/db_stats":
        s = db_stats()
        await send_message(
            f"🗄 <b>База данных</b>\n\n"
            f"📋 Алертов: <b>{s['alerts']}</b>\n"
            f"📅 Старейший: <b>{s['oldest_alert']}</b>\n"
            f"🪙 Уровней монет: <b>{s['levels']}</b>\n"
            f"🔄 Разворотных сигналов: <b>{s['reversals']}</b>\n"
            f"💾 Размер файла: <b>{s['size_mb']:.2f} МБ</b>\n\n"
            f"⚙️ Хранение алертов: <b>{DB_KEEP_ALERTS_DAYS} дн.</b>\n"
            f"⚙️ Хранение уровней: <b>{DB_KEEP_LEVELS_DAYS} дн.</b>",
            chat_id,
        )
        return

    if text == "/db_cleanup":
        deleted = db_cleanup()
        total   = sum(deleted.values())
        await send_message(
            f"🧹 <b>Очистка выполнена</b>\n\n"
            f"Удалено строк: <b>{total}</b>\n"
            f"  alerts: {deleted['alerts']}\n"
            f"  levels: {deleted['alert_levels']}\n"
            f"  stats:  {deleted['price_stats']}\n"
            f"  reversals: {deleted['reversal_signals']}\n\n"
            f"💾 Размер БД: <b>{db_size_mb():.2f} МБ</b>",
            chat_id,
        )
        return

    if text.startswith("/set_percent"):
        try:
            val = float(text.split()[1])
            assert 0.01 <= val <= 100
            current_percent = val
            await send_message(f"✅ Новый порог: <b>{val}%</b>", chat_id)
        except Exception:
            await send_message("❌ Использование: /set_percent 2.5", chat_id)
        return

    if text.startswith("/set_window"):
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 10080
            current_window = val * 60
            await send_message(f"✅ Новый период: <b>{val} мин</b>", chat_id)
        except Exception:
            await send_message("❌ Использование: /set_window 60  (в минутах)", chat_id)
        return

    if text == "/subscribe":
        db_add_subscriber(chat_id)
        await send_message("✅ Вы уже в списке получателей", chat_id)
        return

    await send_message("❓ Неизвестная команда. /menu — открыть панель", chat_id)


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
        await send_message("🏆 Нет данных за последние 24 ч", chat_id)
        return
    lines = ["🏆 <b>Топ-5 сигналов за 24 ч:</b>\n"]
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
    lines = ["🔄 <b>Последние 8 разворотов на шорт:</b>\n"]
    for r in rows:
        ts      = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
        score   = r["score"]
        factors = r["factors"]
        # Уровень по скору
        if score >= 6:
            lvl = "🔴"
        elif score >= 4:
            lvl = "🟠"
        else:
            lvl = "🟡"
        factor_brief = factors[0] if factors else "—"
        lines.append(
            f"{lvl} <b>{r['symbol']}</b>  [{score}/8]  {ts}\n"
            f"   <i>{factor_brief}</i>"
        )
    await send_message("\n".join(lines), chat_id)


async def _cmd_export(chat_id):
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
        status = "PAUSED" if monitor_paused else "running"
        log.info("♥ alive | %s | signals=%d | reversals=%d | coins=%d",
                 status, signals_count, reversal_count, len(price_history))
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
            log.info(
                "DB cleanup: удалено %d строк %s | размер %.2f МБ | алертов всего %d",
                total, deleted, stats["size_mb"], stats["alerts"],
            )
            now = time.time()
            if total > 0 and now - last_vacuum >= DB_VACUUM_INTERVAL_H * 3600:
                log.info("DB VACUUM начат...")
                await asyncio.get_event_loop().run_in_executor(None, db_vacuum)
                last_vacuum = now
                log.info("DB VACUUM завершён | размер %.2f МБ", db_size_mb())
        except Exception as e:
            log.exception("db_cleanup_loop error: %s", e)


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
                    log.warning("Watchdog: monitor завис (%.0f с) — перезапуск", stall)
                    await broadcast(f"⚠️ Watchdog: monitor завис ({stall:.0f} с). Перезапуск...")
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
#  GRACEFUL SHUTDOWN
# ================================================================

_shutdown_event: asyncio.Event | None = None


def _handle_signal(sig):
    log.info("Signal %s получен — завершение...", sig)
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
            log.info("Prometheus metrics on :%d", prom_port)
        except Exception as e:
            log.warning("Prometheus start failed: %s", e)

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

        log.info("Завершение задач...")
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
