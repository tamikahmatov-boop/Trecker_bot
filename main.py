import json
import logging
import asyncio
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

# ---------------- STATE ----------------

STATE_FILE = "state.json"

last_alert = {}
last_alert_growth = {}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "last_alert": last_alert,
                "last_alert_growth": last_alert_growth
            }, f)
    except Exception:
        pass


def load_state():
    global last_alert, last_alert_growth
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            last_alert = data.get("last_alert", {})
            last_alert_growth = data.get("last_alert_growth", {})
    except Exception:
        pass


load_state()

# ---------------- CONFIG ----------------

TOKEN = config.BOT_TOKEN
URL = f"https://api.telegram.org/bot{TOKEN}"

offset = 0
price_history = {}
signals_count = 0
checks_count = 0
start_time = time.time()
last_check_time = 0

current_percent = config.PERCENT
current_window = config.WINDOW


def normalize_symbol(sym: str) -> str:
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")


# ---------------- TELEGRAM ----------------

def send_message(text, chat_id):
    try:
        response = requests.post(
            f"{URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
        if not response.ok:
            logging.warning("Ошибка Telegram: %s", response.text)
    except Exception as e:
        logging.error("Ошибка Telegram: %s", e)


def send_keyboard(chat_id):
    keyboard = {
        "keyboard": [
            ["📈 0.2%", "📈 5%", "📈 10%"],
            ["📈 15%", "📈 20%"],
            ["⏱ 5 мин", "⏱ 1 час"],
            ["⏱ 4 часа", "⏱ 1 день"],
            ["📊 Статистика"],
            ["/status"]
        ],
        "resize_keyboard": True
    }
    requests.post(
        f"{URL}/sendMessage",
        json={"chat_id": chat_id, "text": "Выберите настройки:", "reply_markup": keyboard}
    )


# ---------------- SYMBOLS ----------------

def get_symbols():
    symbols = set()

    try:
        r = requests.get("https://public.bybit.com/trading/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        count = 0
        for a in soup.find_all("a"):
            sym = a.text.strip("/")
            if sym.endswith(("USDT", "PERP")):
                symbols.add(sym.replace("/", ""))
                count += 1
        logging.info("Trading: %d", count)
    except Exception as e:
        logging.error("Ошибка trading: %s", e)

    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        count = 0
        for a in soup.find_all("a"):
            sym = a.text.strip("/")
            if sym.endswith("USDT"):
                symbols.add(sym)
                count += 1
        logging.info("Spot: %d", count)
    except Exception as e:
        logging.error("Ошибка spot: %s", e)

    logging.info("Всего монет: %d", len(symbols))
    return symbols


# ---------------- PRICES ----------------

def get_prices(symbols):
    prices = {}
    sources = {}
    normalized_symbols = {normalize_symbol(s): s for s in symbols}

    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=20
        )
        data = r.json()
        if "data" in data:
            for item in data["data"]:
                inst = item["instId"]
                price = float(item["last"])
                sym = normalize_symbol(inst)
                if price > 0 and sym in normalized_symbols:
                    real_sym = normalized_symbols[sym]
                    prices[real_sym] = price
                    sources[real_sym] = "OKX"
    except Exception as e:
        logging.error("Ошибка OKX: %s", e)

    try:
        r = requests.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=20
        )
        data = r.json()
        if data["success"]:
            for item in data["data"]:
                sym = normalize_symbol(item["symbol"])
                price = float(item["lastPrice"])
                if price > 0 and sym in normalized_symbols:
                    real_sym = normalized_symbols[sym]
                    if real_sym not in prices:
                        prices[real_sym] = price
                        sources[real_sym] = "MEXC"
    except Exception as e:
        logging.error("Ошибка MEXC: %s", e)

    return prices, sources


# ---------------- RSI ----------------

def calculate_rsi(prices, window=5):
    try:
        if len(prices) < window + 1:
            return None
        series = pd.Series(prices)
        rsi = RSIIndicator(close=series, window=window).rsi().iloc[-1]
        if pd.isna(rsi):
            return None
        return round(float(rsi), 2)
    except Exception as e:
        logging.error("Ошибка RSI: %s", e)
        return None


# ---------------- RETRY WRAPPER ----------------

async def safe_request(func, retries=5, delay=2):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            logging.warning("Retry %d/%d: %s", attempt + 1, retries, e)
            await asyncio.sleep(delay)
    return None


# ---------------- MONITOR ----------------

async def monitor():
    global current_percent, current_window, signals_count, checks_count, last_check_time

    symbols = get_symbols()
    last_symbols_update = time.time()

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:
        try:
            now = time.time()
            checks_count += 1
            last_check_time = now

            if now - last_symbols_update >= 1800:
                new_symbols = get_symbols()
                if new_symbols:
                    symbols = new_symbols
                    logging.info("Список монет обновлен: %d", len(symbols))
                else:
                    logging.warning("Не удалось обновить список монет, используется старый")
                last_symbols_update = now

            prices, sources = get_prices(symbols)

            for sym, price in prices.items():
                await asyncio.sleep(0)

                if price <= 0:
                    continue

                if sym not in price_history:
                    price_history[sym] = []

                price_history[sym].append((now, price))

                history_time = max(current_window * 2, 86400)
                price_history[sym] = [
                    x for x in price_history[sym]
                    if now - x[0] <= history_time
                ]

                recent_prices = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window
                ]

                if len(recent_prices) < 2:
                    continue

                old_price = recent_prices[0][1]
                if old_price <= 0:
                    continue

                growth = ((price - old_price) / old_price) * 100
                prices_list = [x[1] for x in price_history[sym][-100:]]
                rsi = calculate_rsi(prices_list)

                if abs(growth) >= current_percent:
                    if sym in last_alert and now - last_alert[sym] < config.COOLDOWN:
                        continue

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

                    if rsi is not None:
                        text += f"\n📊 RSI: {rsi:.2f}"
                    else:
                        text += "\n📊 RSI: ожидание данных"

                    send_message(text, config.CHAT_ID)
                    signals_count += 1
                    last_alert[sym] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            logging.exception("Ошибка monitor: %s", e)
            await asyncio.sleep(5)


# ---------------- TELEGRAM HANDLER ----------------

def handle_message(msg):
    global current_percent, current_window

    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":
        send_message(
            f"🚀 Бот запущен\n\n"
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n",
            chat_id
        )
        send_keyboard(chat_id)

    elif text == "/status":
        send_message(
            f"📊 Настройки\n\n"
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window} сек\n"
            f"🔔 Кулдаун: {config.COOLDOWN // 60} мин",
            chat_id
        )

    elif text == "📊 Статистика":
        uptime = int(time.time() - start_time)
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
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
            f"📌 Активных алертов: {len(last_alert)}",
            chat_id
        )

    elif text == "📈 0.2%":
        current_percent = 0.2
        send_message("✅ 0.2%", chat_id)
    elif text == "📈 5%":
        current_percent = 5
        send_message("✅ 5%", chat_id)
    elif text == "📈 10%":
        current_percent = 10
        send_message("✅ 10%", chat_id)
    elif text == "📈 15%":
        current_percent = 15
        send_message("✅ 15%", chat_id)
    elif text == "📈 20%":
        current_percent = 20
        send_message("✅ 20%", chat_id)
    elif text == "⏱ 5 мин":
        current_window = 300
        send_message("✅ 5 мин", chat_id)
    elif text == "⏱ 1 час":
        current_window = 3600
        send_message("✅ 1 час", chat_id)
    elif text == "⏱ 4 часа":
        current_window = 14400
        send_message("✅ 4 часа", chat_id)
    elif text == "⏱ 1 день":
        current_window = 86400
        send_message("✅ 1 день", chat_id)
    else:
        send_message("❓ Неизвестная команда", chat_id)


# ---------------- UPDATES ----------------

def get_updates():
    global offset
    try:
        response = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=35
        )
        data = response.json()
        if data["ok"]:
            return data["result"]
    except Exception as e:
        logging.error("Ошибка get_updates: %s", e)
    return []


async def telegram_loop():
    global offset
    while True:
        try:
            updates = get_updates()
            for update in updates:
                offset = update["update_id"] + 1
                if "message" in update:
                    handle_message(update["message"])
        except Exception as e:
            logging.exception("Ошибка telegram_loop: %s", e)
        await asyncio.sleep(0.2)


# ---------------- BACKGROUND TASKS ----------------

async def heartbeat():
    while True:
        logging.info("Bot alive")
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        try:
            save_state()
        except Exception:
            pass
        await asyncio.sleep(30)


async def watchdog():
    global last_check_time
    while True:
        try:
            if last_check_time and time.time() - last_check_time > 60:
                logging.warning("Monitor appears stalled")
        except Exception as e:
            logging.exception(e)
        await asyncio.sleep(30)


# ---------------- GLOBAL EXCEPTION HANDLER ----------------

def handle_async_exception(loop, context):
    exc = context.get("exception")
    if exc:
        logging.exception("Unhandled async exception", exc_info=exc)
    else:
        logging.error("Async error: %s", context["message"])


# ---------------- MAIN ----------------

async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)

    await asyncio.gather(
        monitor(),
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
