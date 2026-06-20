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


def send_message(text, chat_id):
    try:
        requests.post(
            f"{URL}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text
            },
            timeout=20
        )
    except Exception as e:
        print("Ошибка Telegram:", e)


def get_symbols():
    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        symbols = set()

        for a in soup.find_all("a"):
            symbol = a.text.strip("/")

            if symbol.endswith("USDT"):
                symbols.add(symbol)

        print("Загружено монет:", len(symbols))

        return symbols

    except Exception as e:
        print("Ошибка Bybit:", e)
        return set()


def get_prices(symbols):
    prices = {}

    try:
        r = requests.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=20
        )

        data = r.json()

        if data["success"]:
            for item in data["data"]:

                symbol = item["symbol"].replace("_", "")

                if symbol in symbols:

                    try:
                        price = float(item["lastPrice"])

                        if price > 0:
                            prices[symbol] = price

                    except:
                        pass

    except Exception as e:
        print("Ошибка MEXC:", e)

    return prices

def calculate_rsi(prices, period=14):

    if len(prices) < period + 1:
        return None

    close_series = pd.Series(prices)

    rsi = RSIIndicator(close_series, window=period).rsi()

    return round(rsi.iloc[-1], 2)


async def monitor():

    symbols = get_symbols()

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:

        try:

            now = time.time()

            prices = get_prices(symbols)

            for sym, price in prices.items():

                if price <= 0:
                    continue

                if sym not in price_history:
                    price_history[sym] = []

                price_history[sym].append((now, price))

                price_history[sym] = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window
                ]

                if len(price_history[sym]) < 2:
                    continue
                prices_list = [x[1] for x in price_history[sym]]

                rsi = calculate_rsi(prices_list)

                if rsi is None:
                continue   
                old = price_history[sym][0][1]

                if old <= 0:
                    continue

                growth = ((price - old) / old) * 100

                if growth >= current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    send_message(
                        f"🚀 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price}\n"
                        f"Рост: +{growth:.2f}%",
                        config.CHAT_ID
                    )

                    last_alert[sym] = now

            await asyncio.sleep(60)

        except Exception as e:
            print("Ошибка monitor:", e)
            await asyncio.sleep(60)


def get_updates():
    global offset

    try:
        r = requests.get(
            f"{URL}/getUpdates",
            params={
                "timeout": 10,
                "offset": offset
            },
            timeout=20
        )

        return r.json()["result"]

    except:
        return []


def handle_message(msg):
    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":

        send_message(
            f"🚀 Бот запущен\n\n"
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n\n"
            f"/status - настройки",
            chat_id
        )

    elif text == "/status":

        send_message(
            f"📊 Настройки\n\n"
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"🔔 Повтор сигнала: {config.COOLDOWN // 60} мин",
            chat_id
        )


async def telegram_loop():
    global offset

    while True:

        updates = get_updates()

        for update in updates:

            offset = update["update_id"] + 1

            if "message" in update:
                handle_message(update["message"])

        await asyncio.sleep(1)


async def main():
    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


asyncio.run(main())
