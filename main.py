import time
import requests
from telegram import Bot

BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = "6716942872"

bot = Bot(token=BOT_TOKEN)

price_history = {}

def get_bybit_symbols():
    url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
    data = requests.get(url).json()

    symbols = set()

    if data["retCode"] == 0:
        for item in data["result"]["list"]:
            symbol = item["symbol"]
            if symbol.endswith("USDT"):
                symbols.add(symbol)

    return symbols


def get_mexc_prices():
    url = "https://api.mexc.com/api/v3/ticker/price"
    data = requests.get(url).json()

    bybit_symbols = get_bybit_symbols()

    prices = {}

    for item in data:
        symbol = item["symbol"]

        if symbol in bybit_symbols:
            prices[symbol] = float(item["price"])

    return prices


while True:
    try:
        current_time = time.time()
        prices = get_mexc_prices()

        for symbol, price in prices.items():

            if symbol not in price_history:
                price_history[symbol] = []

            price_history[symbol].append((current_time, price))

            # оставляем историю только за последние 5 минут
            price_history[symbol] = [
                x for x in price_history[symbol]
                if current_time - x[0] <= 300
            ]

            oldest_time, old_price = price_history[symbol][0]

            growth = ((price - old_price) / old_price) * 100

            if growth >= 0.3:
                bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        f"🚀 Рост за 5 минут\n\n"
                        f"Монета: {symbol}\n"
                        f"Цена: {price}\n"
                        f"Рост: +{growth:.2f}%"
                    )
                )

                # сброс истории после отправки
                price_history[symbol] = [(current_time, price)]

    except Exception as e:
        print(e)

    time.sleep(60)
