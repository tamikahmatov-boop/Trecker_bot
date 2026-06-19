import asyncio
import time
import requests
from bs4 import BeautifulSoup
from telegram import Bot

BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = 6716942872

PERCENT = 0.3
WINDOW = 300
INTERVAL = 60
COOLDOWN = 600

price_history = {}
last_alert = {}


def get_bybit_symbols():
    symbols = set()

    try:
        response = requests.get(
            "https://public.bybit.com/spot/",
            timeout=20
        )

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            symbol = link.text.strip("/")

            if symbol.endswith("USDT"):
                symbols.add(symbol)

        print("Монет Bybit:", len(symbols))

    except Exception as e:
        print("Ошибка Bybit:", e)

    return symbols


def get_mexc_prices(bybit_symbols):
    prices = {}

    try:
        response = requests.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=20
        )

        data = response.json()

        if data["success"]:
            for item in data["data"]:
                symbol = item["symbol"].replace("_", "")

                if symbol in bybit_symbols:
                    prices[symbol] = float(item["lastPrice"])

    except Exception as e:
        print("Ошибка MEXC:", e)

    return prices


async def main():

    bot = Bot(token=BOT_TOKEN)

    await bot.send_message(
        chat_id=CHAT_ID,
        text="✅ Бот запущен"
    )

    bybit_symbols = get_bybit_symbols()

    while True:

        try:
            now = time.time()

            # обновление списка монет каждый час
            if int(now) % 3600 < INTERVAL:
                bybit_symbols = get_bybit_symbols()

            prices = get_mexc_prices(bybit_symbols)

            for symbol, price in prices.items():

                if symbol not in price_history:
                    price_history[symbol] = []

                price_history[symbol].append((now, price))

                price_history[symbol] = [
                    x for x in price_history[symbol]
                    if now - x[0] <= WINDOW
                ]

                if len(price_history[symbol]) < 2:
                    continue

                old_price = price_history[symbol][0][1]

                growth = ((price - old_price) / old_price) * 100

                if growth >= PERCENT:

                    if symbol in last_alert:
                        if now - last_alert[symbol] < COOLDOWN:
                            continue

                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"🚀 Рост за 5 минут\n\n"
                            f"Монета: {symbol}\n"
                            f"Цена: {price}\n"
                            f"Рост: +{growth:.2f}%"
                        )
                    )

                    last_alert[symbol] = now

            await asyncio.sleep(INTERVAL)

        except Exception as e:
            print("Основная ошибка:", e)
            await asyncio.sleep(INTERVAL)


asyncio.run(main())
