
import json
import logging
import asyncio
import sqlite3
import requests
import time
import pandas as pd
from ta.momentum import RSIIndicator
from bs4 import BeautifulSoup
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ================================================================
#  DATABASE  (SQLite — алерты, кулдауны, история)
# ================================================================

DB_FILE = "alerts.db"


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                price     REAL    NOT NULL,
                growth    REAL    NOT NULL,
                rsi       REAL,
                source    TEXT,
                ts        REAL    NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts     ON alerts(ts)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cooldowns (
                symbol    TEXT PRIMARY KEY,
                ts        REAL NOT NULL
            )
        """)


db_init()


def db_save_alert(symbol: str, price: float, growth: float,
                  rsi: float | None, source: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, source, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (symbol, price, growth, rsi, source, time.time())
        )


def db_get_cooldown(symbol: str) -> float:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT ts FROM cooldowns WHERE symbol = ?", (symbol,)
        ).fetchone()
    return row["ts"] if row else 0.0


def db_set_cooldown(symbol: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO cooldowns (symbol, ts) VALUES (?, ?)",
            (symbol, time.time())
        )


def db_recent_alerts(limit: int = 10) -> list:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, growth, rsi, source, ts "
            "FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ================================================================
#  STATE  (json — лёгкие глобальные настройки)
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
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            last_alert_growth = data.get("last_alert_growth", {})
    except Exception:
        pass


load_state()

# ================================================================
#  GLOBALS
# ================================================================

TOKEN = config.BOT_TOKEN
URL   = f"https://api.telegram.org/bot{TOKEN}"

offset        = 0
price_history: dict = {}
signals_count = 0
checks_count  = 0
start_time    = time.time()
last_check_time: float = 0.0

current_percent = config.PERCENT
current_window  = config.WINDOW

# Пауза мониторинга
monitor_paused = False

# Задача monitor — нужна watchdog'у для перезапуска
monitor_task: asyncio.Task | None = None


def normalize_symbol(sym: str) -> str:
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")


# ================================================================
#  TELEGRAM HELPERS
# ================================================================

def send_message(text: str, chat_id):
    try:
        resp = requests.post(
            f"{URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
        if not resp.ok:
            logging.warning("Telegram error: %s", resp.text)
    except Exception as e:
        logging.error("Telegram send error: %s", e)


def send_keyboard(chat_id):
    keyboard = {
        "keyboard": [
            ["📈 0.2%", "📈 5%", "📈 10%"],
            ["📈 15%", "📈 20%"],
            ["⏱ 5 мин", "⏱ 1 час"],
            ["⏱ 4 часа", "⏱ 1 день"],
            ["📊 Статистика", "📋 История"],
            ["⏸ Пауза", "▶️ Продолжить"],
            ["/status"],
        ],
        "resize_keyboard": True,
    }
    requests.post(
        f"{URL}/sendMessage",
        json={"chat_id": chat_id, "text": "Выберите настройки:", "reply_markup": keyboard},
    )


# ================================================================
#  SYMBOLS
# ================================================================

def get_symbols() -> set:
    symbols: set = set()

    for label, url, suffix in [
        ("Trading", "https://public.bybit.com/trading/", ("USDT", "PERP")),
        ("Spot",    "https://public.bybit.com/spot/",    ("USDT",)),
    ]:
        try:
            r = requests.get(url, timeout=20)
            soup = BeautifulSoup(r.text, "html.parser")
            count = 0
            for a in soup.find_all("a"):
                sym = a.text.strip("/")
                if sym.endswith(suffix):
                    symbols.add(sym.replace("/", ""))
                    count += 1
            logging.info("%s: %d", label, count)
        except Exception as e:
            logging.error("Ошибка %s: %s", label, e)

    logging.info("Всего монет: %d", len(symbols))
    return symbols


# ================================================================
#  PRICES
# ================================================================

def get_prices(symbols: set) -> tuple[dict, dict]:
    prices: dict  = {}
    sources: dict = {}
    norm = {normalize_symbol(s): s for s in symbols}

    # OKX
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=20
        )
        for item in r.json().get("data", []):
            sym = normalize_symbol(item["instId"])
            price = float(item["last"])
            if price > 0 and sym in norm:
                real = norm[sym]
                prices[real]  = price
                sources[real] = "OKX"
    except Exception as e:
        logging.error("Ошибка OKX: %s", e)

    # MEXC
    try:
        r = requests.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=20
        )
        data = r.json()
        if data.get("success"):
            for item in data["data"]:
                sym = normalize_symbol(item["symbol"])
                price = float(item["lastPrice"])
                if price > 0 and sym in norm:
                    real = norm[sym]
                    if real not in prices:
                        prices[real]  = price
                        sources[real] = "MEXC"
    except Exception as e:
        logging.error("Ошибка MEXC: %s", e)

    return prices, sources


# ================================================================
#  RSI
# ================================================================

def calculate_rsi(prices: list, window: int = 5) -> float | None:
    try:
        if len(prices) < window + 1:
            return None
        series = pd.Series(prices)
        rsi = RSIIndicator(close=series, window=window).rsi().iloc[-1]
        return None if pd.isna(rsi) else round(float(rsi), 2)
    except Exception as e:
        logging.error("Ошибка RSI: %s", e)
        return None


# ================================================================
#  RETRY WRAPPER
# ================================================================

async def safe_request(func, retries: int = 5, delay: float = 2):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            logging.warning("Retry %d/%d: %s", attempt + 1, retries, e)
            await asyncio.sleep(delay)
    return None


# ================================================================
#  MONITOR
# ================================================================

async def monitor():
    global current_percent, current_window, signals_count, checks_count, last_check_time

    symbols = get_symbols()
    last_symbols_update = time.time()

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:
        # ── пауза ──────────────────────────────────────────────
        if monitor_paused:
            await asyncio.sleep(2)
            continue

        try:
            now = time.time()
            checks_count   += 1
            last_check_time = now

            # обновление списка монет каждые 30 мин
            if now - last_symbols_update >= 1800:
                new_symbols = get_symbols()
                if new_symbols:
                    symbols = new_symbols
                    logging.info("Монеты обновлены: %d", len(symbols))
                else:
                    logging.warning("Список монет не обновлён, используется старый")
                last_symbols_update = now

            prices, sources = get_prices(symbols)

            for sym, price in prices.items():
                await asyncio.sleep(0)   # передать управление event loop

                if price <= 0:
                    continue

                hist = price_history.setdefault(sym, [])
                hist.append((now, price))

                # чистим старую историю
                cutoff = max(current_window * 2, 86400)
                price_history[sym] = [x for x in hist if now - x[0] <= cutoff]

                recent = [x for x in price_history[sym] if now - x[0] <= current_window]
                if len(recent) < 2:
                    continue

                old_price = recent[0][1]
                if old_price <= 0:
                    continue

                growth = ((price - old_price) / old_price) * 100

                if abs(growth) < current_percent:
                    continue

                # ── антиспам через SQLite ──────────────────────
                last_ts = db_get_cooldown(sym)
                if now - last_ts < config.COOLDOWN:
                    continue

                rsi    = calculate_rsi([x[1] for x in price_history[sym][-100:]])
                source = sources.get(sym, "UNKNOWN")

                if growth > 0:
                    text = (
                        f"🚀 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price} ({source})\n"
                        f"Рост: +{growth:.2f}%\n"
                    )
                else:
                    text = (
                        f"📉 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price} ({source})\n"
                        f"Падение: {growth:.2f}%\n"
                    )

                text += f"\n📊 RSI: {rsi:.2f}" if rsi is not None else "\n📊 RSI: ожидание данных"

                send_message(text, config.CHAT_ID)

                db_save_alert(sym, price, growth, rsi, source)
                db_set_cooldown(sym)
                signals_count += 1

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            logging.exception("Ошибка monitor: %s", e)
            await asyncio.sleep(5)


# ================================================================
#  TELEGRAM MESSAGE HANDLER
# ================================================================

def handle_message(msg: dict):
    global current_percent, current_window, monitor_paused

    text    = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]

    # ── /start ─────────────────────────────────────────────────
    if text == "/start":
        send_message(
            f"🚀 Бот запущен\n\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг идёт'}",
            chat_id,
        )
        send_keyboard(chat_id)

    # ── /status ────────────────────────────────────────────────
    elif text == "/status":
        send_message(
            f"📊 Настройки\n\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window} сек\n"
            f"🔔 Кулдаун: {config.COOLDOWN // 60} мин\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Активен'}",
            chat_id,
        )

    # ── Статистика ─────────────────────────────────────────────
    elif text == "📊 Статистика":
        uptime  = int(time.time() - start_time)
        days    = uptime // 86400
        hours   = (uptime % 86400) // 3600
        minutes = (uptime % 3600) // 60
        send_message(
            f"📊 СТАТИСТИКА\n\n"
            f"🟢 Время работы: {days}д {hours}ч {minutes}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"🔔 Сигналов отправлено: {signals_count}\n"
            f"🔄 Циклов проверки: {checks_count}\n"
            f"📈 Порог роста: {current_percent}%\n"
            f"⏱ Период анализа: {current_window // 60} мин\n"
            f"⚡ Интервал проверки: {config.INTERVAL} сек\n"
            f"🕒 Кулдаун: {config.COOLDOWN // 60} мин\n"
            f"{'⏸ Мониторинг на паузе' if monitor_paused else '▶️ Мониторинг активен'}",
            chat_id,
        )

    # ── История алертов (из SQLite) ────────────────────────────
    elif text in ("📋 История", "/history"):
        rows = db_recent_alerts(10)
        if not rows:
            send_message("📋 История пуста", chat_id)
        else:
            lines = ["📋 Последние 10 сигналов:\n"]
            for r in rows:
                ts  = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
                sign = "🚀" if r["growth"] > 0 else "📉"
                rsi_str = f"RSI {r['rsi']:.1f}" if r["rsi"] is not None else "RSI —"
                lines.append(
                    f"{sign} {r['symbol']} {r['growth']:+.2f}% | {rsi_str} | {ts}"
                )
            send_message("\n".join(lines), chat_id)

    # ── Пауза / Продолжить ─────────────────────────────────────
    elif text in ("⏸ Пауза", "/pause"):
        if monitor_paused:
            send_message("⏸ Мониторинг уже на паузе", chat_id)
        else:
            monitor_paused = True
            logging.info("Monitor paused by user")
            send_message("⏸ Мониторинг приостановлен", chat_id)

    elif text in ("▶️ Продолжить", "/resume"):
        if not monitor_paused:
            send_message("▶️ Мониторинг уже активен", chat_id)
        else:
            monitor_paused = False
            logging.info("Monitor resumed by user")
            send_message("▶️ Мониторинг возобновлён", chat_id)

    # ── Пороги роста ───────────────────────────────────────────
    elif text == "📈 0.2%":
        current_percent = 0.2;  send_message("✅ Порог: 0.2%", chat_id)
    elif text == "📈 5%":
        current_percent = 5;    send_message("✅ Порог: 5%", chat_id)
    elif text == "📈 10%":
        current_percent = 10;   send_message("✅ Порог: 10%", chat_id)
    elif text == "📈 15%":
        current_percent = 15;   send_message("✅ Порог: 15%", chat_id)
    elif text == "📈 20%":
        current_percent = 20;   send_message("✅ Порог: 20%", chat_id)

    # ── Периоды ────────────────────────────────────────────────
    elif text == "⏱ 5 мин":
        current_window = 300;   send_message("✅ Период: 5 мин", chat_id)
    elif text == "⏱ 1 час":
        current_window = 3600;  send_message("✅ Период: 1 час", chat_id)
    elif text == "⏱ 4 часа":
        current_window = 14400; send_message("✅ Период: 4 часа", chat_id)
    elif text == "⏱ 1 день":
        current_window = 86400; send_message("✅ Период: 1 день", chat_id)

    else:
        send_message("❓ Неизвестная команда", chat_id)


# ================================================================
#  TELEGRAM LOOP
# ================================================================

def get_updates() -> list:
    global offset
    try:
        resp = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=35,
        )
        data = resp.json()
        if data.get("ok"):
            return data["result"]
    except Exception as e:
        logging.error("Ошибка get_updates: %s", e)
    return []


async def telegram_loop():
    global offset
    while True:
        try:
            for update in get_updates():
                offset = update["update_id"] + 1
                if "message" in update:
                    handle_message(update["message"])
        except Exception as e:
            logging.exception("Ошибка telegram_loop: %s", e)
        await asyncio.sleep(0.2)


# ================================================================
#  BACKGROUND TASKS
# ================================================================

async def heartbeat():
    while True:
        status = "PAUSED" if monitor_paused else "running"
        logging.info("Bot alive | status=%s | signals=%d", status, signals_count)
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        try:
            save_state()
        except Exception:
            pass
        await asyncio.sleep(30)


# ================================================================
#  WATCHDOG  — авто-перезапуск monitor при зависании
# ================================================================

WATCHDOG_TIMEOUT = 90   # секунд без активности monitor → перезапуск

async def watchdog():
    global monitor_task, last_check_time

    # небольшая задержка при старте, чтобы monitor успел инициализироваться
    await asyncio.sleep(60)

    while True:
        try:
            if monitor_paused:
                await asyncio.sleep(30)
                continue

            stall = time.time() - last_check_time if last_check_time else 0

            if stall > WATCHDOG_TIMEOUT:
                logging.warning(
                    "Watchdog: monitor завис (%.0f сек без активности) — перезапуск", stall
                )
                send_message(
                    f"⚠️ Watchdog: monitor завис ({stall:.0f} сек). Перезапуск...",
                    config.CHAT_ID,
                )

                # отменяем старую задачу
                if monitor_task and not monitor_task.done():
                    monitor_task.cancel()
                    try:
                        await monitor_task
                    except asyncio.CancelledError:
                        pass

                # запускаем новую
                monitor_task = asyncio.create_task(monitor())
                last_check_time = time.time()   # сбрасываем таймер
                logging.info("Watchdog: monitor перезапущен")
                send_message("✅ Monitor перезапущен", config.CHAT_ID)

        except Exception as e:
            logging.exception("Ошибка watchdog: %s", e)

        await asyncio.sleep(30)


# ================================================================
#  GLOBAL EXCEPTION HANDLER
# ================================================================

def handle_async_exception(loop, context):
    exc = context.get("exception")
    if exc:
        logging.exception("Unhandled async exception", exc_info=exc)
    else:
        logging.error("Async error: %s", context["message"])


# ================================================================
#  MAIN
# ================================================================

async def main():
    global monitor_task

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)

    monitor_task = asyncio.create_task(monitor())

    await asyncio.gather(
        asyncio.shield(monitor_task),   # watchdog управляет задачей сам
        telegram_loop(),
        heartbeat(),
        save_state_loop(),
        watchdog(),
    )


while True:
    try:
        asyncio.run(main())
    except Exception as e:
        logging.exception("Критическая ошибка: %s", e)
        time.sleep(10)
