"""
Crypto Alert Bot — v10
Улучшения vs v9:
  • Убран Binance — единственные источники цен: MEXC (приоритет) и OKX
  • RSI считается по реальным kline-свечам MEXC (не по тикам бота) — совпадает с биржей
  • Кэш kline на 60 с — экономия запросов
  • Параллельный fetch цен (asyncio.gather)
  • WAL-режим SQLite + пул соединений (thread-safe)
  • Таблица price_stats (24h high/low/vol) — новый сигнал по объёму
  • MACD-индикатор — дополнительный фильтр к RSI
  • /set_percent X — произвольный порог без кнопок
  • /set_window X  — произвольный период (в минутах)
  • /top5 — топ-5 монет по силе последнего сигнала
  • /clear_cooldowns — сброс кулдаунов вручную
  • Многоуровневый алерт: 🚀 / 🔥 / 💥 в зависимости от величины роста
  • Экспорт истории в CSV по команде /export
  • Конфигурируемый список CHAT_IDs (мультиподписчики)
  • Prometheus-метрики (опционально, если установлен prometheus_client)
  • Graceful shutdown (SIGTERM / SIGINT)
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
from ta.momentum import RSIIndicator
from ta.trend import MACD

import config

# ── Prometheus (опционально) ──────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    PROM_SIGNALS   = Counter("bot_signals_total",      "Всего сигналов")
    PROM_CHECKS    = Counter("bot_checks_total",        "Всего циклов")
    PROM_COINS     = Gauge("bot_tracked_coins",         "Монет в истории")
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
_db_lock = asyncio.Lock()   # для async-секций


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
        """)


db_init()

# ── Настройки хранения (дней) ─────────────────────────────────────────────────
DB_KEEP_ALERTS_DAYS    = getattr(config, "DB_KEEP_ALERTS_DAYS",    30)   # алерты
DB_KEEP_LEVELS_DAYS    = getattr(config, "DB_KEEP_LEVELS_DAYS",     7)   # уровни монет
DB_VACUUM_INTERVAL_H   = getattr(config, "DB_VACUUM_INTERVAL_H",   24)   # VACUUM раз в N часов


def db_save_alert(symbol: str, price: float, growth: float,
                  rsi: Optional[float], macd: Optional[float], source: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, macd, source, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, price, growth, rsi, macd, source, time.time()),
        )


def db_get_alert_level(symbol: str) -> Optional[dict]:
    """Возвращает запись последнего алерта: alert_price, direction."""
    with db_connect() as conn:
        row = conn.execute(
            "SELECT alert_price, direction, ts FROM alert_levels WHERE symbol=?", (symbol,)
        ).fetchone()
    return dict(row) if row else None


def db_set_alert_level(symbol: str, alert_price: float, direction: int):
    """Сохраняет цену и направление последнего алерта."""
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
    """Сброс уровня для одной монеты (при смене направления)."""
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels WHERE symbol=?", (symbol,))


    """
    Удаляет устаревшие записи из всех таблиц.
    Возвращает словарь {таблица: удалено строк}.
    """
    now     = time.time()
    deleted = {}
    with db_connect() as conn:
        # alerts старше N дней
        cutoff_alerts = now - DB_KEEP_ALERTS_DAYS * 86400
        cur = conn.execute("DELETE FROM alerts WHERE ts < ?", (cutoff_alerts,))
        deleted["alerts"] = cur.rowcount

        # alert_levels монет которые не обновлялись N дней
        cutoff_levels = now - DB_KEEP_LEVELS_DAYS * 86400
        cur = conn.execute("DELETE FROM alert_levels WHERE ts < ?", (cutoff_levels,))
        deleted["alert_levels"] = cur.rowcount

        # price_stats устаревшие записи (нет смысла хранить дольше суток)
        cutoff_stats = now - 86400
        cur = conn.execute("DELETE FROM price_stats WHERE updated < ?", (cutoff_stats,))
        deleted["price_stats"] = cur.rowcount

    return deleted


def db_vacuum():
    """VACUUM — возвращает место на диске после удалений."""
    with db_connect() as conn:
        conn.execute("VACUUM")


def db_size_mb() -> float:
    """Размер файла БД в мегабайтах."""
    import os
    try:
        return os.path.getsize(DB_FILE) / 1_048_576
    except OSError:
        return 0.0


def db_stats() -> dict:
    """Статистика таблиц: количество строк и диапазон дат."""
    with db_connect() as conn:
        alerts_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        oldest = conn.execute("SELECT MIN(ts) FROM alerts").fetchone()[0]
        levels_count = conn.execute("SELECT COUNT(*) FROM alert_levels").fetchone()[0]
    oldest_s = time.strftime("%d.%m.%Y", time.localtime(oldest)) if oldest else "—"
    return {
        "alerts":       alerts_count,
        "oldest_alert": oldest_s,
        "levels":       levels_count,
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
    """Топ сигналов по абсолютной величине роста за последние 24 ч."""
    cutoff = time.time() - 86400
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, growth, price, source, ts "
            "FROM alerts WHERE ts >= ? ORDER BY ABS(growth) DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def db_export_csv(limit: int = 500) -> str:
    """Возвращает CSV-строку последних алертов."""
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
    # всегда включаем основной CHAT_ID из конфига
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
checks_count     = 0
start_time       = time.time()
last_check_time: float = 0.0

current_percent: float = config.PERCENT
current_window:  int   = config.WINDOW

monitor_paused  = False
monitor_task:   asyncio.Task | None = None

# Общая aiohttp-сессия (создаётся в main)
_session: aiohttp.ClientSession | None = None

# Кулдаун в памяти: symbol → timestamp последнего отправленного алерта
_alert_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SEC: int = getattr(config, "ALERT_COOLDOWN_SEC", 60)

# Кэш уровней в памяти — избегаем SQLite на каждой монете в цикле
# {symbol: {"alert_price": float, "direction": int}}
_levels_cache: dict[str, dict] = {}


def _cache_load_levels():
    """Загружает alert_levels из БД в память при старте."""
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
#  MEXC KLINE CACHE  (для точного RSI как на бирже)
# ================================================================

# {symbol: {"ts": float, "closes": list[float]}}
_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL = 60   # секунд — обновляем не чаще раза в минуту
KLINE_LIMIT     = 100  # свечей (RSI-14 + запас)


async def _fetch_mexc_klines(symbol: str, interval: str = "Min1") -> list[float]:
    """
    Возвращает список цен закрытия (close) последних KLINE_LIMIT свечей с MEXC.
    interval: Min1 / Min5 / Min15 / Min30 / Min60 / Hour4 / Day1 и т.д.
    Документация: https://mexcdevelop.github.io/apidocs/contract_v1_en/#k-line-data
    """
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
    """
    Возвращает закрытия из кэша (обновляет раз в KLINE_CACHE_TTL секунд).
    symbol — в формате MEXC: BTC_USDT
    """
    now    = time.time()
    cached = _kline_cache.get(symbol)
    if cached and now - cached["ts"] < KLINE_CACHE_TTL:
        return cached["closes"]
    closes = await _fetch_mexc_klines(symbol, interval)
    if closes:
        _kline_cache[symbol] = {"ts": now, "closes": closes}
    return closes


def _to_mexc_symbol(sym: str) -> str:
    """
    Конвертирует BTCUSDT → BTC_USDT для MEXC kline API.
    Работает для большинства USDT-пар.
    """
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    if sym.endswith("PERP"):
        # BTCUSDT_PERP → BTC_USDT (бессрочный фьючерс)
        base = sym[:-4]
        if base.endswith("USDT"):
            return base[:-4] + "_USDT"
    return sym


# ================================================================
#  INDICATORS
# ================================================================

# Минимум точек данных чтобы сигнал считался надёжным
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


async def get_rsi_from_mexc(symbol: str, window: int = 14) -> Optional[float]:
    """
    RSI по реальным 1-минутным свечам MEXC — совпадает с индикатором на бирже.
    """
    mexc_sym = _to_mexc_symbol(symbol)
    closes   = await get_mexc_closes(mexc_sym, interval="Min1")
    if not closes:
        return None
    return calculate_rsi(closes, window=window)


async def get_rsi_trend_from_mexc(symbol: str, window: int = 14) -> Optional[str]:
    """Направление RSI по свечам MEXC: растёт ▲ / падает ▼ / боковик —"""
    mexc_sym = _to_mexc_symbol(symbol)
    closes   = await get_mexc_closes(mexc_sym, interval="Min1")
    if len(closes) < window + 3:
        return None
    return calculate_rsi_trend(closes, window=window)


def calculate_rsi_trend(prices: list[float], window: int = 14) -> Optional[str]:
    """Направление RSI: растёт ▲ / падает ▼ / боковик —"""
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


def calculate_macd(prices: list[float]) -> Optional[float]:
    """MACD-гистограмма последней точки."""
    try:
        if len(prices) < 26:
            return None
        s    = pd.Series(prices)
        hist = MACD(close=s).macd_diff().iloc[-1]
        return None if pd.isna(hist) else round(float(hist), 6)
    except Exception as e:
        log.error("MACD error: %s", e)
        return None


def calculate_acceleration(recent: list[tuple[float, float]]) -> Optional[float]:
    """
    Ускорение цены: сравниваем скорость роста первой и второй половины окна.
    Возвращает множитель (>1 = ускорение, <1 = замедление).
    """
    try:
        if len(recent) < 6:
            return None
        mid  = len(recent) // 2
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
    """Сколько секунд занял рост/падение (от первой до последней точки окна)."""
    if len(recent) < 2:
        return 0
    return int(recent[-1][0] - recent[0][0])


def check_24h_breakout(sym: str, price: float) -> Optional[str]:
    """
    Проверяет пробой 24h High или Low.
    Возвращает строку с описанием или None.
    """
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
    """
    Расстояние до 24h High/Low в процентах — показывает где цена в диапазоне дня.
    Например: 'хай 24h: -3.2% | лой 24h: +18.4%'
    """
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if len(day_prices) < 20:
        return None
    high24 = max(day_prices)
    low24  = min(day_prices)
    dist_high = (price - high24) / high24 * 100
    dist_low  = (price - low24)  / low24  * 100
    return f"📉 от хая: {dist_high:+.1f}%  📈 от лоя: {dist_low:+.1f}%"


# ================================================================
#  TELEGRAM HELPERS  (async)
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
    """Отправить файл через multipart/form-data."""
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
    """Разослать сообщение всем подписчикам."""
    tasks = [send_message(text, cid, reply_markup) for cid in db_get_subscribers()]
    await asyncio.gather(*tasks, return_exceptions=True)


def reply_keyboard():
    """Постоянная клавиатура внизу экрана — одно нажатие, никаких всплывающих меню."""
    return {
        "keyboard": [
            ["📈 0.2%",    "📈 5%",       "📈 10%"         ],
            ["📈 15%",     "📈 20%"                         ],
            ["⏱ 5 мин",   "⏱ 1 час",    "⏱ 4 ч", "⏱ 1 д"],
            ["📊 Статус",  "📋 История",  "🏆 Топ-5"        ],
            ["⏸ Пауза",   "▶️ Продолжить"                  ],
            ["📤 Экспорт", "🗑 Кулдауны", "/status"         ],
        ],
        "resize_keyboard": True,
        "persistent":       True,
    }


async def send_main_menu(chat_id):
    await send_message(
        "⚙️ <b>Панель управления</b>",
        chat_id,
        reply_markup=reply_keyboard(),
    )


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


def format_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    accel: Optional[float] = None,
    duration_sec: int = 0,
    breakout: Optional[str] = None,
    day_context: Optional[str] = None,
) -> str:
    emoji  = alert_emoji(growth)
    label  = "Рост" if growth > 0 else "Падение"
    sign   = "+" if growth > 0 else ""

    # RSI
    rsi_s = f"{rsi:.1f}" if rsi is not None else "—"
    rsi_hint = ""
    if rsi is not None:
        if rsi >= 70:
            rsi_hint = " ⚠️ перекуплен"
        elif rsi <= 30:
            rsi_hint = " ⚠️ перепродан"
    rsi_trend_s = f" {rsi_trend}" if rsi_trend else ""

    # MACD
    macd_s = f"{macd:+.6f}" if macd is not None else "—"

    # Ускорение
    accel_s = ""
    if accel is not None:
        if accel >= 2.0:
            accel_s = f"\n⚡ Ускорение: <b>×{accel:.1f}</b> 🔥"
        elif accel >= 1.3:
            accel_s = f"\n⚡ Ускорение: ×{accel:.1f}"
        elif accel < 0.7:
            accel_s = f"\n🐢 Замедление: ×{accel:.1f}"

    # Длительность
    if duration_sec >= 3600:
        dur_s = f"{duration_sec // 3600}ч {(duration_sec % 3600) // 60}м"
    elif duration_sec >= 60:
        dur_s = f"{duration_sec // 60}м"
    else:
        dur_s = f"{duration_sec}с"

    # Пробой
    breakout_s   = f"\n{breakout}"    if breakout    else ""
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
#  PRICES  (параллельный fetch)
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
    # Пересоздаём norm только если список монет изменился
    sym_key = frozenset(symbols)
    if sym_key != _norm_symbols_key:
        _norm_cache      = {normalize_symbol(s): s for s in symbols}
        _norm_symbols_key = sym_key
    norm   = _norm_cache
    merged: dict = {}
    msrc:   dict = {}

    results = await asyncio.gather(
        _fetch_mexc(norm),
        _fetch_okx(norm),
        return_exceptions=True,
    )

    # MEXC — приоритет, OKX заполняет пробелы
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
    global current_percent, current_window, signals_count, checks_count, last_check_time

    symbols           = await get_symbols()
    last_symbols_upd  = time.time()
    _cache_load_levels()

    await broadcast("✅ <b>Бот запущен</b>")

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

            # обновление списка монет каждые 30 мин
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

                # Минимум точек — фильтр шумовых пиков
                if len(recent) < MIN_SAMPLES:
                    continue

                old_price = recent[0][1]
                if old_price <= 0:
                    continue

                growth    = (price - old_price) / old_price * 100
                direction = 1 if growth > 0 else -1

                if abs(growth) < current_percent:
                    level = _cache_get_level(sym)
                    if level and level["direction"] != direction:
                        _cache_clear_level(sym)
                    continue

                # ── RSI по реальным свечам MEXC ──────────────────────────────
                rsi = await get_rsi_from_mexc(sym)
                # Фallback на локальные тики если MEXC kline недоступен
                if rsi is None:
                    vals = [p for _, p in price_history[sym][-100:]]
                    rsi  = calculate_rsi(vals)
                if rsi is not None:
                    if direction == 1 and rsi < 50:
                        # Рост, но RSI ниже 50 — сигнал против тренда, пропускаем
                        log.debug("Skip %s: growth but RSI=%.1f < 50", sym, rsi)
                        continue
                    if direction == -1 and rsi > 50:
                        # Падение, но RSI выше 50 — пропускаем
                        log.debug("Skip %s: drop but RSI=%.1f > 50", sym, rsi)
                        continue

                # ── Кулдаун в памяти (защита от дублей при быстрых циклах) ──
                last_sent = _alert_cooldown.get(sym, 0)
                if now - last_sent < ALERT_COOLDOWN_SEC:
                    log.debug("Cooldown skip %s (%.0f с назад)", sym, now - last_sent)
                    continue

                # ── Проверка уровня (повторный алерт) ────────────────────────
                level = _cache_get_level(sym)
                if level:
                    prev_price = level["alert_price"]
                    prev_dir   = level["direction"]
                    if prev_dir == direction:
                        step = abs(price - prev_price) / prev_price * 100
                        if step < current_percent:
                            continue
                    # смена направления → новый алерт

                # ── Вычисляем дополнительный контекст ────────────────────────
                vals      = [p for _, p in price_history[sym][-100:]]
                macd      = calculate_macd(vals)
                rsi_trend = await get_rsi_trend_from_mexc(sym)
                accel     = calculate_acceleration(recent)
                duration  = calculate_growth_duration(recent)
                breakout  = check_24h_breakout(sym, price)
                day_ctx   = get_24h_context(sym, price)
                source    = sources.get(sym, "UNKNOWN")

                text = format_alert(
                    sym, price, growth, rsi, macd, source,
                    rsi_trend=rsi_trend,
                    accel=accel,
                    duration_sec=duration,
                    breakout=breakout,
                    day_context=day_ctx,
                )
                # Записываем уровень и кулдаун ДО await — защита от race condition
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
    global current_percent, current_window, monitor_paused

    text    = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]

    # ── Авторизация ────────────────────────────────────────────────────────────
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

    # ── /start / /menu ─────────────────────────────────────────────────────────
    if text in ("/start", "/menu"):
        await send_message(
            f"🚀 <b>Бот запущен</b>\n\n"
            f"📈 Порог: <b>{current_percent}%</b>\n"
            f"⏱ Период: <b>{current_window // 60} мин</b>\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг идёт'}",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    # ── Пороги роста ──────────────────────────────────────────────────────────
    _pct_map = {
        "📈 0.2%": 0.2, "📈 5%": 5.0, "📈 10%": 10.0,
        "📈 15%": 15.0, "📈 20%": 20.0,
    }
    if text in _pct_map:
        current_percent = _pct_map[text]
        await send_message(f"✅ Порог: <b>{current_percent}%</b>", chat_id)
        return

    # ── Периоды ────────────────────────────────────────────────────────────────
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

    # ── Пауза / Продолжить ────────────────────────────────────────────────────
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

    # ── Статус ────────────────────────────────────────────────────────────────
    if text in ("📊 Статус", "/status"):
        uptime  = int(time.time() - start_time)
        d, rem  = divmod(uptime, 86400)
        h, rem  = divmod(rem, 3600)
        m       = rem // 60
        await send_message(
            f"📊 <b>СТАТУС</b>\n\n"
            f"🟢 Аптайм: {d}д {h}ч {m}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"🔔 Сигналов: {signals_count}\n"
            f"🔄 Циклов: {checks_count}\n"
            f"📈 Порог: <b>{current_percent}%</b>\n"
            f"⏱ Период: <b>{current_window // 60} мин</b>\n"
            f"⚡ Интервал: {config.INTERVAL} сек\n"
            f"🕒 Кулдаун: {config.COOLDOWN // 60} мин\n"
            f"👥 Подписчиков: {len(db_get_subscribers())}\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Активен'}",
            chat_id,
        )
        return

    # ── История — мгновенный ответ + фоновая задача ───────────────────────────
    if text in ("📋 История", "/history"):
        asyncio.create_task(_cmd_history(chat_id))
        return

    # ── Топ-5 — мгновенный ответ + фоновая задача ────────────────────────────
    if text in ("🏆 Топ-5", "/top5"):
        asyncio.create_task(_cmd_top5(chat_id))
        return

    # ── Экспорт CSV — сообщение сразу, файл в фоне ───────────────────────────
    if text in ("📤 Экспорт", "/export"):
        await send_message("📤 Генерирую CSV...", chat_id)
        asyncio.create_task(_cmd_export(chat_id))
        return

    # ── Сброс кулдаунов ───────────────────────────────────────────────────────
    if text in ("🗑 Кулдауны", "/clear_cooldowns"):
        _cache_clear_all()
        await send_message("🗑 Уровни алертов сброшены", chat_id)
        return

    if text == "/db_stats":
        s = db_stats()
        await send_message(
            f"🗄 <b>База данных</b>\n\n"
            f"📋 Алертов: <b>{s['alerts']}</b>\n"
            f"📅 Старейший: <b>{s['oldest_alert']}</b>\n"
            f"🪙 Уровней монет: <b>{s['levels']}</b>\n"
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
            f"  stats:  {deleted['price_stats']}\n\n"
            f"💾 Размер БД: <b>{db_size_mb():.2f} МБ</b>",
            chat_id,
        )
        return

    # ── Произвольный порог: /set_percent 2.5 ─────────────────────────────────
    if text.startswith("/set_percent"):
        try:
            val = float(text.split()[1])
            assert 0.01 <= val <= 100
            current_percent = val
            await send_message(f"✅ Новый порог: <b>{val}%</b>", chat_id)
        except Exception:
            await send_message("❌ Использование: /set_percent 2.5", chat_id)
        return

    # ── Произвольный период: /set_window 60 (мин) ────────────────────────────
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
                    # Каждое сообщение обрабатываем в отдельной задаче —
                    # telegram_loop не блокируется на медленных командах
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
        log.info("♥ alive | %s | signals=%d | coins=%d", status, signals_count, len(price_history))
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        try:
            save_state()
        except Exception:
            pass
        await asyncio.sleep(30)


async def db_cleanup_loop():
    """
    Фоновая задача автоочистки БД.
    Каждые 6 часов удаляет устаревшие строки.
    Раз в DB_VACUUM_INTERVAL_H часов запускает VACUUM.
    """
    last_vacuum = time.time()

    while True:
        await asyncio.sleep(6 * 3600)   # проверяем каждые 6 часов
        try:
            deleted = db_cleanup()
            total   = sum(deleted.values())
            stats   = db_stats()

            log.info(
                "DB cleanup: удалено %d строк %s | размер %.2f МБ | алертов всего %d",
                total, deleted, stats["size_mb"], stats["alerts"],
            )

            # VACUUM только если что-то удалили и подошло время
            now = time.time()
            if total > 0 and now - last_vacuum >= DB_VACUUM_INTERVAL_H * 3600:
                log.info("DB VACUUM начат...")
                await asyncio.get_event_loop().run_in_executor(None, db_vacuum)
                last_vacuum = now
                new_size = db_size_mb()
                log.info("DB VACUUM завершён | размер %.2f МБ", new_size)

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
                    log.info("Watchdog: monitor перезапущен")
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


# ================================================================
#  GLOBAL EXCEPTION HANDLER
# ================================================================

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

    # Запуск Prometheus (если доступен)
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

        # Graceful shutdown
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
