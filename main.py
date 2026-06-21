import asyncio
import requests
import time
import pandas as pd
from ta.momentum import RSIIndicator
from bs4 import BeautifulSoup
import config

TOKEN = config.BOT_TOKEN
URL = f"https://api.telegram.org/bot{TOKEN}"

offset = 0

price_history = {}
last_alert = {}

current_percent = config.PERCENT
current_window = config.WINDOW


# ---------------- TELEGRAM ----------------

def send_message(text, chat_id):
    try:
        response = requests.post(
            f"{URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )

        if not response.ok:
            print("Ошибка Telegram:", response.text)

    except Exception as e:
        print("Ошибка Telegram:", e)


# ---------------- SYMBOLS ----------------

def get_symbols():
    try:
        r = requests.get("https://api.bybit.com/v5/market/instruments-info?category=spot", timeout=20)
        data = r.json()

        symbols = set()

        for item in data.get("result", {}).get("list", []):
            sym = item["symbol"]
            if sym.endswith("USDT"):
                symbols.add(sym)

        print("Загружено монет:", len(symbols))
        return symbols

    except Exception as e:
        print("Ошибка symbols:", e)
        return set()


# ---------------- EXCHANGES ----------------

def get_okx():
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
            timeout=20
        )
        data = r.json().get("data", [])

        return {
            item["instId"].replace("-", ""): float(item["last"])
            for item in data
        }

    except Exception as e:
        print("OKX error:", e)
        return {}


def get_mexc():
    try:
        r = requests.get("https://api.mexc.com/api/v3/ticker/price", timeout=20)
        return {item["symbol"]: float(item["price"]) for item in r.json()}
    except Exception as e:
        print("MEXC error:", e)
        return {}


def get_bitget():
    try:
        r = requests.get(
            "https://api.bitget.com/api/spot/v1/market/tickers",
            timeout=20
        )
        data = r.json().get("data", [])

        return {
            item["symbol"]: float(item["close"])
            for item in data
        }

    except Exception as e:
        print("Bitget error:", e)
        return {}


def get_kucoin():
    try:
        r = requests.get("https://api.kucoin.com/api/v1/market/allTickers", timeout=20)
        data = r.json()["data"]["ticker"]

        return {
            item["symbol"].replace("-", ""): float(item["last"])
            for item in data
        }

    except Exception as e:
        print("KuCoin error:", e)
        return {}


def get_bingx():
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/spot/v1/ticker/price",
            timeout=20
        )
        data = r.json().get("data", [])

        return {
            item["symbol"]: float(item["price"])
            for item in data
        }

    except Exception as e:
        print("BingX error:", e)
        return {}


# ---------------- COMBINED PRICES ----------------

def get_prices(symbols):
    all_prices = {}

    sources = [
        get_okx,
        get_mexc,
        get_bitget,
        get_kucoin,
        get_bingx
    ]

    for source in sources:
        try:
            data = source()

            for sym, price in data.items():
                if sym in symbols:
                    all_prices[sym] = price

        except Exception as e:
            print("Source error:", e)

    return all_prices


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
        print("RSI error:", e)
        return None


# ---------------- MONITOR ----------------

async def monitor():
    global current_percent, current_window

    symbols = get_symbols()
    last_symbols_update = time.time()

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:
        try:
            now = time.time()

            if now - last_symbols_update >= 3600:
                symbols = get_symbols()
                last_symbols_update = now

            prices = get_prices(symbols)

            for sym, price in prices.items():

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
                rsi = calculate_rsi(prices_list, window=5)

                if abs(growth) >= current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    if growth > 0:
                        text = (
                            f"🚀 СИГНАЛ\n\n"
                            f"Монета: {sym}\n"
                            f"Цена: {price}\n"
                            f"Рост: +{growth:.2f}%\n"
                        )
                    else:
                        text = (
                            f"📉 СИГНАЛ\n\n"
                            f"Монета: {sym}\n"
                            f"Цена: {price}\n"
                            f"Падение: {growth:.2f}%\n"
                        )

                    if rsi is not None:
                        text += f"\n📊 RSI: {rsi:.2f}"
                    else:
                        text += "\n📊 RSI: ожидание данных"

                    send_message(text, config.CHAT_ID)
                    last_alert[sym] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            print("Monitor error:", e)
            await asyncio.sleep(5)


# ---------------- TELEGRAM UI ----------------

def send_keyboard(chat_id):
    keyboard = {
        "keyboard": [
            ["📈 0.2%", "📈 5%", "📈 10%"],
            ["📈 15%", "📈 20%"],
            ["⏱ 5 мин", "⏱ 1 час"],
            ["⏱ 4 часа", "⏱ 1 день"],
            ["/status"]
        ],
        "resize_keyboard": True
    }

    requests.post(
        f"{URL}/sendMessage",
        json={"chat_id": chat_id, "text": "Выберите настройки:", "reply_markup": keyboard}
    )


# ---------------- HANDLER ----------------

def handle_message(msg):
    global current_percent, current_window

    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":
        send_message(
            f"🚀 Бот запущен\n\n📈 Рост: {current_percent}%\n⏱ Период: {current_window // 60} мин",
            chat_id
        )
        send_keyboard(chat_id)

    elif text == "/status":
        send_message(
            f"📊 Настройки\n\n📈 Рост: {current_percent}%\n⏱ Период: {current_window} сек\n🔔 Кулдаун: {config.COOLDOWN // 60} мин",
            chat_id
        )

    elif text == "📈 0.2%": current_percent = 0.2
    elif text == "📈 5%": current_percent = 5
    elif text == "📈 10%": current_percent = 10
    elif text == "📈 15%": current_percent = 15
    elif text == "📈 20%": current_percent = 20

    elif text == "⏱ 5 мин": current_window = 300
    elif text == "⏱ 1 час": current_window = 3600
    elif text == "⏱ 4 часа": current_window = 14400
    elif text == "⏱ 1 день": current_window = 86400

    else:
        send_message("❓ Неизвестная команда", chat_id)


# ---------------- TELEGRAM LOOP ----------------

def get_updates():
    global offset

    try:
        r = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=35
        )

        data = r.json()
        if data["ok"]:
            return data["result"]

    except Exception as e:
        print("get_updates error:", e)

    return []


async def telegram_loop():
    global offset

    while True:
        updates = get_updates()

        for update in updates:
            offset = update["update_id"] + 1
            if "message" in update:
                handle_message(update["message"])

        await asyncio.sleep(1)


# ---------------- MAIN ----------------

async def main():
    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


while True:
    try:
        asyncio.run(main())
    except Exception as e:
        print("CRITICAL ERROR:", e)
        time.sleep(10)
