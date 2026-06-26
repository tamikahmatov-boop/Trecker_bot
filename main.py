"""
Telegram-бот для мониторинга бессрочных USDT-фьючерсов Bybit (Linear Perpetual).

Источники данных:
  • REST  GET /v5/market/instruments-info?category=linear  — список символов
  • REST  GET /v5/market/tickers?category=linear           — снимок цен (fallback / init)
  • WS    wss://stream.bybit.com/v5/public/linear          — real-time тикеры

Зависимости:
  pip install requests websocket-client pandas ta
"""

import json
import logging
import asyncio
import sqlite3
import threading
import time

import pandas as pd
import requests
import websocket
from ta.momentum import RSIIndicator

import config

# ──────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ──────────────────────────────────────────────────────────────────────────────
#  BYBIT ENDPOINTS
# ──────────────────────────────────────────────────────────────────────────────

BYBIT_REST   = "https://api.bybit.com"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# ──────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ──────────────────────────────────────────────────────────────────────────────

DB_FILE = "alerts.db"


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol  TEXT  NOT NULL,
                price   REAL  NOT NULL,
                growth  REAL  NOT NULL,
                rsi     REAL,
                ts      REAL  NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_a_sym ON alerts(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_a_ts  ON alerts(ts)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cooldowns (
                symbol TEXT PRIMARY KEY,
                ts     REAL NOT NULL
            )
        """)


db_init()


def db_save_alert(symbol: str, price: float, growth: float, rsi: float | None):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, ts) VALUES (?,?,?,?,?)",
            (symbol, price, growth, rsi, time.time()),
        )


def db_get_cooldown(symbol: str) -> float:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT ts FROM cooldowns WHERE symbol=?", (symbol,)
        ).fetchone()
    return row["ts"] if row else 0.0


def db_set_cooldown(symbol: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO cooldowns (symbol, ts) VALUES (?,?)",
            (symbol, time.time()),
        )


def db_recent_alerts(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, growth, rsi, ts FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────────────
#  STATE (лёгкие настройки между перезапусками)
# ──────────────────────────────────────────────────────────────────────────────

STATE_FILE = "state.json"


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(
                {"percent": current_percent, "window": current_window}, f
            )
    except Exception:
        pass


def load_state():
    global current_percent, current_window
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
            current_percent = d.get("percent", config.PERCENT)
            current_window  = d.get("window",  config.WINDOW)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
#  GLOBALS
# ──────────────────────────────────────────────────────────────────────────────

TOKEN = config.BOT_TOKEN
TG_URL = f"https://api.telegram.org/bot{TOKEN}"

tg_offset     = 0
price_history: dict[str, list[tuple[float, float]]] = {}
live_prices:   dict[str, float] = {}          # обновляется из WebSocket
signals_count = 0
checks_count  = 0
start_time    = time.time()
last_check_time: float = 0.0

current_percent: float = config.PERCENT
current_window:  int   = config.WINDOW

monitor_paused = False
monitor_task: asyncio.Task | None = None

load_state()

# ──────────────────────────────────────────────────────────────────────────────
#  TELEGRAM HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def send_message(text: str, chat_id):
    try:
        r = requests.post(
            f"{TG_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20,
        )
        if not r.ok:
            logging.warning("TG error: %s", r.text)
    except Exception as e:
        logging.error("TG send: %s", e)


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
        f"{TG_URL}/sendMessage",
        json={"chat_id": chat_id, "text": "Выберите настройки:", "reply_markup": keyboard},
    )


# ──────────────────────────────────────────────────────────────────────────────
#  BYBIT REST — список символов
# ──────────────────────────────────────────────────────────────────────────────

def get_bybit_symbols() -> set[str]:
    """
    Возвращает все активные USDT-бессрочные (LinearPerpetual) с Bybit.
    Использует пагинацию через cursor.
    """
    symbols: set[str] = set()
    cursor = ""

    while True:
        try:
            params: dict = {"category": "linear", "limit": 1000, "status": "Trading"}
            if cursor:
                params["cursor"] = cursor

            r = requests.get(
                f"{BYBIT_REST}/v5/market/instruments-info",
                params=params,
                timeout=20,
            )
            data = r.json()

            if data.get("retCode") != 0:
                logging.error("instruments-info error: %s", data.get("retMsg"))
                break

            result = data["result"]

            for item in result.get("list", []):
                # только бессрочные котируемые в USDT
                if (
                    item.get("contractType") == "LinearPerpetual"
                    and item.get("quoteCoin") == "USDT"
                    and item.get("status") == "Trading"
                ):
                    symbols.add(item["symbol"])

            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break

        except Exception as e:
            logging.error("get_bybit_symbols: %s", e)
            break

    logging.info("Bybit LinearPerpetual USDT: %d символов", len(symbols))
    return symbols


# ──────────────────────────────────────────────────────────────────────────────
#  BYBIT REST — снимок цен (fallback / первый запуск)
# ──────────────────────────────────────────────────────────────────────────────

def get_bybit_prices_rest(symbols: set[str]) -> dict[str, float]:
    """
    GET /v5/market/tickers?category=linear — возвращает все тикеры одним запросом.
    Фильтрует только нужные символы.
    """
    prices: dict[str, float] = {}
    try:
        r = requests.get(
            f"{BYBIT_REST}/v5/market/tickers",
            params={"category": "linear"},
            timeout=20,
        )
        data = r.json()

        if data.get("retCode") != 0:
            logging.error("tickers REST error: %s", data.get("retMsg"))
            return prices

        for item in data["result"]["list"]:
            sym = item["symbol"]
            if sym not in symbols:
                continue
            try:
                price = float(item["lastPrice"])
                if price > 0:
                    prices[sym] = price
            except (ValueError, KeyError):
                pass

    except Exception as e:
        logging.error("get_bybit_prices_rest: %s", e)

    return prices


# ──────────────────────────────────────────────────────────────────────────────
#  BYBIT WEBSOCKET — real-time тикеры
# ──────────────────────────────────────────────────────────────────────────────

class BybitTickerWS:
    """
    Подписывается на tickers.<symbol> для всех символов через
    wss://stream.bybit.com/v5/public/linear.

    Bybit ограничивает одно соединение — не более ~21 000 символов в args.
    На практике ~300+ символов — нужно разбить на батчи по 100.
    """

    BATCH = 100          # символов на одно соединение
    PING_INTERVAL = 20   # секунд

    def __init__(self, symbols: set[str]):
        self._symbols  = list(symbols)
        self._threads: list[threading.Thread] = []
        self._stop     = threading.Event()

    def start(self):
        batches = [
            self._symbols[i: i + self.BATCH]
            for i in range(0, len(self._symbols), self.BATCH)
        ]
        logging.info("WS: %d батчей × %d символов", len(batches), self.BATCH)

        for idx, batch in enumerate(batches):
            t = threading.Thread(
                target=self._run_ws,
                args=(batch, idx),
                daemon=True,
                name=f"bybit-ws-{idx}",
            )
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()

    def _run_ws(self, batch: list[str], idx: int):
        args = [f"tickers.{s}" for s in batch]

        def on_open(ws):
            ws.send(json.dumps({"op": "subscribe", "args": args}))
            logging.info("WS[%d]: подписан на %d символов", idx, len(batch))

        def on_message(ws, raw):
            try:
                msg = json.loads(raw)
                data = msg.get("data", {})
                sym  = data.get("symbol")
                lp   = data.get("lastPrice")
                if sym and lp:
                    price = float(lp)
                    if price > 0:
                        live_prices[sym] = price
            except Exception:
                pass

        def on_error(ws, err):
            logging.warning("WS[%d] error: %s", idx, err)

        def on_close(ws, code, msg):
            if not self._stop.is_set():
                logging.warning("WS[%d] закрыт (%s %s), переподключение...", idx, code, msg)
                time.sleep(5)
                self._run_ws(batch, idx)   # рекурсивный перезапуск

        ws_app = websocket.WebSocketApp(
            BYBIT_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=self.PING_INTERVAL, ping_timeout=10)


# ──────────────────────────────────────────────────────────────────────────────
#  RSI
# ──────────────────────────────────────────────────────────────────────────────

def calculate_rsi(prices: list[float], window: int = 5) -> float | None:
    try:
        if len(prices) < window + 1:
            return None
        rsi = RSIIndicator(close=pd.Series(prices), window=window).rsi().iloc[-1]
        return None if pd.isna(rsi) else round(float(rsi), 2)
    except Exception as e:
        logging.error("RSI: %s", e)
        return None


# ──────────────────────────────────────────────────────────────────────────────
#  MONITOR
# ──────────────────────────────────────────────────────────────────────────────

async def monitor(symbols: set[str], ws_client: BybitTickerWS):
    global signals_count, checks_count, last_check_time

    # Первичное заполнение через REST, пока WS ещё не прислал данные
    logging.info("Monitor: первичный REST-снимок...")
    rest_snap = get_bybit_prices_rest(symbols)
    live_prices.update(rest_snap)
    logging.info("Monitor: загружено %d цен из REST", len(rest_snap))

    send_message(
        f"✅ Бот запущен\n"
        f"📡 Bybit Linear Perpetual USDT\n"
        f"🪙 Символов: {len(symbols)}",
        config.CHAT_ID,
    )

    last_symbols_update = time.time()

    while True:
        if monitor_paused:
            await asyncio.sleep(2)
            continue

        try:
            now = time.time()
            checks_count   += 1
            last_check_time = now

            # ── обновление списка монет каждые 30 мин ─────────
            if now - last_symbols_update >= 1800:
                new_syms = get_bybit_symbols()
                if new_syms:
                    symbols = new_syms
                    logging.info("Символы обновлены: %d", len(symbols))
                last_symbols_update = now

            # ── обход текущих цен из WebSocket ────────────────
            snapshot = dict(live_prices)   # атомарная копия

            for sym, price in snapshot.items():
                if sym not in symbols:
                    continue

                await asyncio.sleep(0)   # yield event loop

                hist = price_history.setdefault(sym, [])
                hist.append((now, price))

                # чистим старое
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

                # антиспам
                if now - db_get_cooldown(sym) < config.COOLDOWN:
                    continue

                rsi = calculate_rsi([x[1] for x in price_history[sym][-100:]])

                arrow = "🚀" if growth > 0 else "📉"
                sign  = "+" if growth > 0 else ""
                text  = (
                    f"{arrow} СИГНАЛ\n\n"
                    f"Монета:  {sym}\n"
                    f"Цена:    {price}\n"
                    f"Изменение: {sign}{growth:.2f}%\n"
                    f"Период:  {current_window // 60} мин\n"
                )
                text += f"📊 RSI: {rsi:.2f}" if rsi is not None else "📊 RSI: ожидание данных"

                send_message(text, config.CHAT_ID)
                db_save_alert(sym, price, growth, rsi)
                db_set_cooldown(sym)
                signals_count += 1

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            logging.exception("monitor: %s", e)
            await asyncio.sleep(5)


# ──────────────────────────────────────────────────────────────────────────────
#  TELEGRAM HANDLER
# ──────────────────────────────────────────────────────────────────────────────

def handle_message(msg: dict):
    global current_percent, current_window, monitor_paused

    text    = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]

    if text == "/start":
        send_message(
            f"🚀 Бот запущен\n"
            f"📡 Источник: Bybit Linear Perpetual USDT\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Мониторинг активен'}",
            chat_id,
        )
        send_keyboard(chat_id)

    elif text == "/status":
        ws_syms = len(live_prices)
        send_message(
            f"📊 Статус\n\n"
            f"📡 Bybit LinearPerpetual USDT\n"
            f"🟢 WS цен: {ws_syms}\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window} сек\n"
            f"🔔 Кулдаун: {config.COOLDOWN // 60} мин\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Активен'}",
            chat_id,
        )

    elif text == "📊 Статистика":
        uptime  = int(time.time() - start_time)
        d = uptime // 86400; h = (uptime % 86400) // 3600; m = (uptime % 3600) // 60
        send_message(
            f"📊 СТАТИСТИКА\n\n"
            f"🟢 Время работы: {d}д {h}ч {m}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"📡 WS live-цен: {len(live_prices)}\n"
            f"🔔 Сигналов: {signals_count}\n"
            f"🔄 Циклов: {checks_count}\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"⚡ Интервал: {config.INTERVAL} сек\n"
            f"🕒 Кулдаун: {config.COOLDOWN // 60} мин",
            chat_id,
        )

    elif text in ("📋 История", "/history"):
        rows = db_recent_alerts(10)
        if not rows:
            send_message("📋 История пуста", chat_id)
        else:
            lines = ["📋 Последние 10 сигналов:\n"]
            for r in rows:
                ts   = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
                sign = "🚀" if r["growth"] > 0 else "📉"
                rsi_s = f"RSI {r['rsi']:.1f}" if r["rsi"] is not None else "RSI —"
                lines.append(f"{sign} {r['symbol']} {r['growth']:+.2f}% | {rsi_s} | {ts}")
            send_message("\n".join(lines), chat_id)

    elif text in ("⏸ Пауза", "/pause"):
        if monitor_paused:
            send_message("⏸ Уже на паузе", chat_id)
        else:
            monitor_paused = True
            send_message("⏸ Мониторинг приостановлен", chat_id)

    elif text in ("▶️ Продолжить", "/resume"):
        if not monitor_paused:
            send_message("▶️ Уже активен", chat_id)
        else:
            monitor_paused = False
            send_message("▶️ Мониторинг возобновлён", chat_id)

    elif text == "📈 0.2%":  current_percent = 0.2;  save_state(); send_message("✅ Порог: 0.2%", chat_id)
    elif text == "📈 5%":    current_percent = 5;    save_state(); send_message("✅ Порог: 5%", chat_id)
    elif text == "📈 10%":   current_percent = 10;   save_state(); send_message("✅ Порог: 10%", chat_id)
    elif text == "📈 15%":   current_percent = 15;   save_state(); send_message("✅ Порог: 15%", chat_id)
    elif text == "📈 20%":   current_percent = 20;   save_state(); send_message("✅ Порог: 20%", chat_id)
    elif text == "⏱ 5 мин":  current_window = 300;   save_state(); send_message("✅ Период: 5 мин", chat_id)
    elif text == "⏱ 1 час":  current_window = 3600;  save_state(); send_message("✅ Период: 1 час", chat_id)
    elif text == "⏱ 4 часа": current_window = 14400; save_state(); send_message("✅ Период: 4 часа", chat_id)
    elif text == "⏱ 1 день": current_window = 86400; save_state(); send_message("✅ Период: 1 день", chat_id)
    else:
        send_message("❓ Неизвестная команда", chat_id)


# ──────────────────────────────────────────────────────────────────────────────
#  TELEGRAM LOOP
# ──────────────────────────────────────────────────────────────────────────────

def get_updates() -> list:
    global tg_offset
    try:
        r = requests.get(
            f"{TG_URL}/getUpdates",
            params={"timeout": 30, "offset": tg_offset},
            timeout=35,
        )
        data = r.json()
        if data.get("ok"):
            return data["result"]
    except Exception as e:
        logging.error("get_updates: %s", e)
    return []


async def telegram_loop():
    global tg_offset
    while True:
        try:
            for upd in get_updates():
                tg_offset = upd["update_id"] + 1
                if "message" in upd:
                    handle_message(upd["message"])
        except Exception as e:
            logging.exception("telegram_loop: %s", e)
        await asyncio.sleep(0.2)


# ──────────────────────────────────────────────────────────────────────────────
#  BACKGROUND TASKS
# ──────────────────────────────────────────────────────────────────────────────

async def heartbeat():
    while True:
        logging.info(
            "alive | ws_prices=%d | signals=%d | paused=%s",
            len(live_prices), signals_count, monitor_paused,
        )
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        save_state()
        await asyncio.sleep(30)


WATCHDOG_TIMEOUT = 90   # сек без активности monitor → перезапуск


async def watchdog(symbols: set[str], ws_client: BybitTickerWS):
    global monitor_task, last_check_time
    await asyncio.sleep(60)

    while True:
        try:
            if not monitor_paused and last_check_time:
                stall = time.time() - last_check_time
                if stall > WATCHDOG_TIMEOUT:
                    logging.warning("Watchdog: monitor завис (%.0f сек)", stall)
                    send_message(
                        f"⚠️ Watchdog: monitor завис ({stall:.0f} с). Перезапуск...",
                        config.CHAT_ID,
                    )
                    if monitor_task and not monitor_task.done():
                        monitor_task.cancel()
                        try:
                            await monitor_task
                        except asyncio.CancelledError:
                            pass
                    monitor_task = asyncio.create_task(monitor(symbols, ws_client))
                    last_check_time = time.time()
                    send_message("✅ Monitor перезапущен", config.CHAT_ID)
        except Exception as e:
            logging.exception("watchdog: %s", e)
        await asyncio.sleep(30)


# ──────────────────────────────────────────────────────────────────────────────
#  GLOBAL EXCEPTION HANDLER
# ──────────────────────────────────────────────────────────────────────────────

def handle_async_exception(loop, context):
    exc = context.get("exception")
    if exc:
        logging.exception("Unhandled async exc", exc_info=exc)
    else:
        logging.error("Async error: %s", context["message"])


# ──────────────────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    global monitor_task

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)

    # 1. Получаем список символов
    logging.info("Загружаем список символов с Bybit...")
    symbols = get_bybit_symbols()
    if not symbols:
        logging.critical("Не удалось загрузить символы. Выход.")
        return

    # 2. Запускаем WebSocket-клиент в фоновых потоках
    ws_client = BybitTickerWS(symbols)
    ws_client.start()
    logging.info("WebSocket-клиент запущен")

    # 3. Небольшая пауза — даём WS прислать первые данные
    await asyncio.sleep(3)

    # 4. Запускаем monitor как отдельную task (watchdog может перезапустить)
    monitor_task = asyncio.create_task(monitor(symbols, ws_client))

    await asyncio.gather(
        asyncio.shield(monitor_task),
        telegram_loop(),
        heartbeat(),
        save_state_loop(),
        watchdog(symbols, ws_client),
    )


while True:
    try:
        asyncio.run(main())
    except Exception as e:
        logging.exception("Критическая ошибка: %s", e)
        time.sleep(10)
