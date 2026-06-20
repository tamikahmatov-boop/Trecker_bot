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

from ta.momentum import RSIIndicator
import pandas as pd

def calculate_rsi(prices, window=14):
    try:
        if len(prices) < window + 1:
            return None

        rsi = RSIIndicator(pd.Series(prices), window=window).rsi().iloc[-1]

        if pd.isna(rsi):
            return None

        return round(float(rsi), 2)

    except Exception as e:
        print("Ошибка RSI:", e)
        return None

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

                # Добавляем новую цену
                price_history[sym].append((now, price))

                # Оставляем только последние 100 значений
                if len(price_history[sym]) > 100:
                    price_history[sym] = price_history[sym][-100:]

                # Для роста используем окно current_window секунд
                recent_prices = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window
                ]

                if len(recent_prices) < 2:
                    continue

                old = recent_prices[0][1]

                if old <= 0:
                    continue

                growth = ((price - old) / old) * 100

                # Цены для RSI
                prices_list = [x[1] for x in price_history[sym]]

                rsi = calculate_rsi(prices_list)

                if growth >= current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    text = (
                        f"🚀 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price}\n"
                        f"Рост: +{growth:.2f}%\n"
                    )

                    if rsi is not None:
                        text += f"📊 RSI: {rsi:.2f}\n"

                    send_message(text, config.CHAT_ID)
                    last_alert[sym] = now

                elif growth <= -current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    text = (
                        f"📉 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price}\n"
                        f"Падение: {growth:.2f}%\n"
                    )

                    if rsi is not None:
                        text += f"📊 RSI: {rsi:.2f}\n"

                    send_message(text, config.CHAT_ID)
                    last_alert[sym] = now

            await asyncio.sleep(60)

        except Exception as e:
            print("Ошибка monitor:", e)
            await asyncio.sleep(60)
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
offset = 0

def get_updates():
    global offset

    try:
        response = requests.get(
            f"{URL}/getUpdates",
            params={
                "timeout": 30,
                "offset": offset
            }
        )

        data = response.json()

        if data["ok"]:
            return data["result"]

    except Exception as e:
        print("Ошибка get_updates:", e)

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


async def main():
    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


asyncio.run(main())
