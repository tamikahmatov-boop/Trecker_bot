import time
import requests
from telegram import Bot

# Вставь свои данные
BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = "6716942872"

bot = Bot(token=BOT_TOKEN)

# Хранение истории цен за 5 минут
price_history = {}

# Монеты Bybit
bybit_symbols = set()


def get_bybit_symbols():
    global bybit_symbols

    try:
        url = "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            print("Ошибка Bybit:", response.text)
            return

        data = response.json()

        if data.get("retCode") == 0:
            symbols = set()

            for item in data["result"]["list"]:
                symbol = item["symbol"]

                if symbol.endswith("USDT"):
                    symbols.add(symbol)

            bybit_symbols = symbols
            print("Загружено монет Bybit:", len(bybit_symbols))

    except Exception as e:
        print("Ошибка получения монет Bybit:", e)


def get_prices():
    prices = {}

    try:
        response = requests.get(
            "https://api.mexc.com/api/v3/ticker/price",
            timeout=10
        )

        if response.status_code != 200:
            print("Ошибка MEXC:", response.text)
            return prices

        data = response.json()

        for item in data:
            symbol = item.get("symbol")

            if symbol in bybit_symbols:
                try:
                    prices[symbol] = float(item["price"])
                except:
                    pass

    except Exception as e:
        print("Ошибка получения цен:", e)

    return prices


# Загружаем монеты Bybit один раз
get_bybit_symbols()

while True:
    try:
        now = time.time()
        prices = get_prices()

        for symbol, price in prices.items():

            if symbol not in price_history:
                price_history[symbol] = []

            price_history[symbol].append((now, price))

            # оставляем только последние 5 минут
            price_history[symbol] = [
                x for x in price_history[symbol]
                if now - x[0] <= 300
            ]

            if len(price_history[symbol]) > 1:
                old_price = price_history[symbol][0][1]

                growth = ((price - old_price) / old_price) * 100

                if growth >= 0.3:

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
                    except Exception as e:
                        print("Ошибка Telegram:", e)

                    # сброс после отправки
                    price_history[symbol] = [(now, price)]

        time.sleep(60)

    except Exception as e:
        print("Основная ошибка:", e)
        time.sleep(60)
