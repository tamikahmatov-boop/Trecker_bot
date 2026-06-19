import asyncio
import requests
import time
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


asyncio.run(monitor())


# ---------------- TELEGRAM ----------------

def send_message(text, chat_id):
    requests.post(
        f"{URL}/sendMessage",
        json={"chat_id": chat_id, "text": text}
    )


# ---------------- SYMBOLS ----------------

def get_symbols():
    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        return {
            a.text.strip("/")
            for a in soup.find_all("a")
            if a.text.strip("/").endswith("USDT")
        }
    except:
        return set()


# ---------------- PRICES ----------------

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


# ---------------- BOT LOOP ----------------

async def monitor():
    global current_percent, current_window

    symbols = get_symbols()
    print("Бот запущен")

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
                    f"🚀 СИГНАЛ\n{sym}\n+{growth:.2f}%",
                    config.CHAT_ID
                )

                last_alert[sym] = now

        await asyncio.sleep(60)


# ---------------- LONG POLLING ----------------

def get_updates():
    global offset

    try:
        r = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 10, "offset": offset}
        )
        return r.json()["result"]
    except:
        return []


def handle_message(msg):
    global current_percent

    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":

        send_message(
            f"🚀 Бот запущен\n\n"
            f"Рост: {current_percent}%\n"
            f"Период: {current_window // 60} мин\n\n"
            f"Команды:\n"
            f"/percent 0.3 - изменить процент\n"
            f"/status - текущие настройки",
            chat_id
        )

    elif text.startswith("/percent"):

        try:
            value = float(text.split()[1])

            if value <= 0:
                send_message(
                    "❌ Процент должен быть больше 0",
                    chat_id
                )
                return

            current_percent = value

            send_message(
                f"✅ Новый процент: {current_percent}%",
                chat_id
            )

        except:

            send_message(
                "Пример:\n/percent 0.3",
                chat_id
            )

    elif text == "/status":

        send_message(
            f"📊 Настройки\n\n"
            f"Рост: {current_percent}%\n"
            f"Период: {current_window // 60} мин",
            chat_id
        )

async def telegram_loop():
    global offset

    while True:
        updates = get_updates()

        for u in updates:
            offset = u["update_id"] + 1

            if "message" in u:
                handle_message(u["message"])

        await asyncio.sleep(1)


# ---------------- MAIN ----------------

async def main():
    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


asyncio.run(main())
