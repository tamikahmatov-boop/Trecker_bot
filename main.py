
import time
import requests
from bs4 import BeautifulSoup
from telegram import Bot

# ВСТАВЬ СВОИ ДАННЫЕ
BOT_TOKEN = "ТВОЙ_BOT_TOKEN"
CHAT_ID = "ТВОЙ_CHAT_ID"

bot = Bot(token=BOT_TOKEN)

price_history = {}
last_alert = {}


def get_bybit_symbols():
    symbols = set()

    try:
        url = "https://public.bybit.com/spot/"
        response = requests.get(url, timeout=20)

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a"):
            symbol = link.text.strip("/")

            if symbol.endswith("USDT"):
                symbols.add(symbol)

    except Exception as e:
        print("Ошибка получения монет Bybit:", e)

    return symbols


def get_mexc_prices():
    prices = {}

    try:
        response = requests.get(
            "https://api.mexc.com/api/v3/ticker/price",
            timeout=20
        )

        data = response.json()

        for item in data:
            symbol = item["symbol"]

            if symbol in BYBIT_SYMBOLS:
                try:
                    prices[symbol] = float(item["price"])
                except:
                    pass

    except Exception as e:
        print("Ошибка MEXC:", e)

    return prices


print("Загружаем список монет Bybit...")
BYBIT_SYMBOLS = get_bybit_symbols()
print("Монет найдено:", len(BYBIT_SYMBOLS))

while True:
    try:
        now = time.time()

        prices = get_mexc_prices()

        for symbol, price in prices.items():

            if symbol not in price_history:
                price_history[symbol] = []

            price_history[symbol].append((now, price))

            # история только за последние 5 минут
            price_history[symbol] = [
                x for x in price_history[symbol]
                if now - x[0] <= 300
            ]

            if len(price_history[symbol]) < 2:
                continue

            old_price = price_history[symbol][0][1]

            growth = ((price - old_price) / old_price) * 100

            if growth >= 0.3:

                # защита от спама: не чаще одного сообщения в 10 минут
                if symbol in last_alert:
                    if now - last_alert[symbol] < 600:
                        continue

                text = (
                    f"🚀 Рост за 5 минут\n\n"
                    f"Монета: {symbol}\n"
                    f"Цена: {price}\n"
                    f"Рост: +{growth:.2f}%"
                )

                try:
                    bot.send_message(
                        chat_id=CHAT_ID,
                        text=text
                    )

                    print(text)

                    last_alert[symbol] = now

                except Exception as e:
                    print("Ошибка Telegram:", e)

        time.sleep(60)

    except Exception as e:
        print("Основная ошибка:", e)
        time.sleep(60)
