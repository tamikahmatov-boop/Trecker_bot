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
        requests.post(
            f"{URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
    except Exception as e:
        print("Telegram error:", e)


# ---------------- BYBIT (PUBLIC SYMBOLS, NO API) ----------------

import requests

def get_symbols():
    try:
        url = "https://api.bybit.com/v5/market/instruments-info"

        params = {
            "category": "linear"   # USDT perpetual
        }

        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        symbols = set()

        if data.get("retCode") == 0:
            for item in data["result"]["list"]:
                symbols.add(item["symbol"])  # BTCUSDT

        print("Bybit symbols:", len(symbols))
        return symbols

    except Exception as e:
        print("Bybit error:", e)
        return set()


# ---------------- OKX PRICES (MAIN SOURCE) ----------------

def normalize(symbol: str):
    return symbol.replace("-", "").replace("_", "").upper()


def get_prices():
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=20
        )

        data = r.json()
        prices = {}

        if data.get("code") == "0":
            for item in data["data"]:
                sym = normalize(item["instId"])
                prices[sym] = float(item["last"])

        return prices

    except Exception as e:
        print("OKX error:", e)
        return {}


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

    except:
        return None


# ---------------- MONITOR ----------------

async def monitor():
    global current_percent, current_window

    symbols = get_symbols()

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:
        try:
            now = time.time()

            prices = get_prices()

            for sym, price in prices.items():

                if sym not in symbols:
                    continue

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

                recent = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window
                ]

                if len(recent) < 2:
                    continue

                old_price = recent[0][1]

                growth = ((price - old_price) / old_price) * 100

                prices_list = [x[1] for x in price_history[sym][-100:]]
                rsi = calculate_rsi(prices_list)

                if abs(growth) >= current_percent:

                    if sym in last_alert and now - last_alert[sym] < config.COOLDOWN:
                        continue

                    text = (
                        f"🚀 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price}\n"
                        f"{'Рост' if growth > 0 else 'Падение'}: {growth:.2f}%\n"
                        f"RSI: {rsi if rsi else '—'}"
                    )

                    send_message(text, config.CHAT_ID)

                    last_alert[sym] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            print("monitor error:", e)
            await asyncio.sleep(3)


# ---------------- TELEGRAM ----------------

def get_updates():
    global offset

    try:
        r = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=35
        )

        data = r.json()

        if data.get("ok"):
            return data["result"]

    except:
        pass

    return []


async def telegram_loop():
    global offset

    while True:
        updates = get_updates()

        for u in updates:
            offset = u["update_id"] + 1

            if "message" in u:
                handle_message(u["message"])

        await asyncio.sleep(1)


def handle_message(msg):
    global current_percent, current_window

    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":
        send_message(
            f"🚀 Бот запущен\n📈 {current_percent}%\n⏱ {current_window}s",
            chat_id
        )

    elif text == "/status":
        send_message(
            f"📊 {current_percent}%\n⏱ {current_window}s",
            chat_id
        )

    elif text == "📈 5%":
        current_percent = 5
        send_message("OK 5%", chat_id)

    elif text == "⏱ 5 мин":
        current_window = 300
        send_message("OK 5 min", chat_id)


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
        print("CRITICAL:", e)
        time.sleep(10)
