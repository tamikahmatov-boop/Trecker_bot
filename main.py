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

                # добавляем новую цену
                price_history[sym].append((now, price))

                # храним только данные за последние 2 периода
                price_history[sym] = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window * 2
                ]

                # цены за текущий период
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

                # RSI
                prices_list = [x[1] for x in price_history[sym]]
                rsi = calculate_rsi(prices_list, window=5)

                if abs(growth) >= current_percent:

                    # антиспам
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
                        text += f"📊 RSI: {rsi:.2f}"
                    else:
                        text += "📊 RSI: ожидание данных"

                    send_message(text, config.CHAT_ID)
                    last_alert[sym] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            print("Ошибка monitor:", e)
            await asyncio.sleep(config.INTERVAL)
def send_keyboard(chat_id):
    keyboard = {
        "keyboard": [
            ["📈 5%", "📈 10%", "📈 20%"],
            ["⏱ 1 мин", "⏱ 5 мин", "⏱ 15 мин"],
            ["/status"]
        ],
        "resize_keyboard": True
    }

    requests.post(
        f"{URL}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "Выберите настройки:",
            "reply_markup": keyboard
        }
    )


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
            f"⏱ Период: {current_window // 60} мин\n"
            f"🔔 Кулдаун: {config.COOLDOWN // 60} мин",
            chat_id
        )

    # --- ПРОЦЕНТЫ ---
    elif text == "📈 5%":
        current_percent = 5
        send_message("✅ Установлено: 5%", chat_id)

    elif text == "📈 10%":
        current_percent = 10
        send_message("✅ Установлено: 10%", chat_id)

    elif text == "📈 20%":
        current_percent = 20
        send_message("✅ Установлено: 20%", chat_id)

    # --- ВРЕМЯ ---
    elif text == "⏱ 1 мин":
        current_window = 60
        send_message("✅ Период: 1 мин", chat_id)

    elif text == "⏱ 5 мин":
        current_window = 300
        send_message("✅ Период: 5 мин", chat_id)

    elif text == "⏱ 15 мин":
        current_window = 900
        send_message("✅ Период: 15 мин", chat_id)

    else:
        send_message("❓ Неизвестная команда", chat_id)
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
