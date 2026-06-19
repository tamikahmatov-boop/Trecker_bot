import time
import requests
from telegram import Bot

BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = "6716942872"

main = Bot(token=BOT_TOKEN)

# Тестовое уведомление при запуске
try:
    main.send_message(
        chat_id=CHAT_ID,
        text="✅ Бот запущен успешно"
    )
    print("Тестовое сообщение отправлено")
except Exception as e:
    print("Ошибка Telegram:", e)

price_history = {}
last_alert = {}

PERCENT = 0.3
WINDOW = 300      # 5 минут
INTERVAL = 60     # проверка раз в минуту
COOLDOWN = 600    # повторное уведомление через 10 минут


def get_futures_prices():
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
                if symbol.endswith("USDT"):
                    prices[symbol] = float(item["lastPrice"])

    except Exception as e:
        print("Ошибка MEXC:", e)

    return prices


print("Бот запущен")

while True:
    try:
        now = time.time()
        prices = get_futures_prices()

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

            print(symbol, round(growth, 3))

            if growth >= PERCENT:

                if symbol in last_alert and now - last_alert[symbol] < COOLDOWN:
                    continue

                text = (
                    f"🚀 Рост за 5 минут\n\n"
                    f"Монета: {symbol}\n"
                    f"Цена: {price}\n"
                    f"Рост: +{growth:.2f}%"
                )

                try:
                    main.send_message(chat_id=CHAT_ID, text=text)
                    print("Уведомление отправлено:", symbol)
                    last_alert[symbol] = now

                except Exception as e:
                    print("Ошибка Telegram:", e)

        time.sleep(INTERVAL)

    except Exception as e:
        print("Основная ошибка:", e)
        time.sleep(INTERVAL)
