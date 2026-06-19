import asyncio
import time
import requests
from aiohttp import web

import config

TOKEN = config.BOT_TOKEN
URL = f"https://api.telegram.org/bot{TOKEN}"

price_history = {}
last_alert = {}

current_percent = config.PERCENT
current_window = config.WINDOW


# ---------------- TELEGRAM ----------------

def send_message(text):
    requests.post(
        f"{URL}/sendMessage",
        json={
            "chat_id": config.CHAT_ID,
            "text": text
        }
    )


def edit_message(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text
    }

    if reply_markup:
        payload["reply_markup"] = reply_markup

    requests.post(f"{URL}/editMessageText", json=payload)


# ---------------- BYBIT SYMBOLS ----------------

def get_symbols():
    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(r.text, "html.parser")

        return {
            a.text.strip("/")
            for a in soup.find_all("a")
            if a.text.strip("/").endswith("USDT")
        }
    except:
        return set()


# ---------------- MEXC PRICES ----------------

def get_prices(symbols):
    try:
        r = requests.get("https://contract.mexc.com/api/v1/contract/ticker", timeout=20)
        data = r.json()

        prices = {}

        if data["success"]:
            for i in data["data"]:
                sym = i["symbol"].replace("_", "")
                if sym in symbols:
                    prices[sym] = float(i["lastPrice"])

        return prices
    except:
        return {}


# ---------------- MONITOR ----------------

async def monitor():
    symbols = get_symbols()
    send_message("🚀 aiohttp бот запущен")

    while True:
        now = time.time()
        prices = get_prices(symbols)

        for sym, price in prices.items():

            if sym not in price_history:
                price_history[sym] = []

            price_history[sym].append((now, price))

            price_history[sym] = [
                x for x in price_history[sym]
                if now - x[0] <= current_window
            ]

            if len(price_history[sym]) < 2:
                continue

            old = price_history[sym][0][1]
            growth = ((price - old) / old) * 100

            if growth >= current_percent:

                if sym in last_alert and now - last_alert[sym] < config.COOLDOWN:
                    continue

                send_message(
                    f"🚀 СИГНАЛ\n{sym}\nРост: +{growth:.2f}%"
                )

                last_alert[sym] = now

        await asyncio.sleep(60)


# ---------------- WEBHOOK HANDLER ----------------

async def handle(request):
    data = await request.json()

    if "message" in data:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"].get("text", "")

        if text == "/start":
            send_message("🚀 Бот работает (aiohttp версия)")

    return web.Response(text="OK")


# ---------------- APP ----------------

app = web.Application()
app.router.add_post(f"/{TOKEN}", handle)


async def start_background(app):
    asyncio.create_task(monitor())


app.on_startup.append(start_background)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
