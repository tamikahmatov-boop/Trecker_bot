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
signal_count = 0

current_percent = config.PERCENT
current_window = config.WINDOW

def get_keyboard():

    keyboard = []

    row = []

    for i in range(1, 31):

        row.append({
            "text": f"{i}%",
            "callback_data": f"p_{i}"
        })

        if len(row) == 5:
            keyboard.append(row)
            row = []

    keyboard.append([
        {"text": "15 мин", "callback_data": "w_15"},
        {"text": "30 мин", "callback_data": "w_30"},
        {"text": "1 час", "callback_data": "w_60"}
    ])

    keyboard.append([
        {"text": "2 часа", "callback_data": "w_120"},
        {"text": "4 часа", "callback_data": "w_240"}
    ])

    return {
        "inline_keyboard": keyboard
    }
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

    global signal_count

    send_message("✅ Бот запущен", config.CHAT_ID)

    while True:

        try:

            now = time.time()

            # Обновляем список монет Bybit
            symbols = get_symbols()

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

                change = ((price - old) / old) * 100

                if abs(change) >= current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    if change > 0:

                        requests.post(
                            f"{URL}/sendMessage",
                            json={
                                "chat_id": config.CHAT_ID,
                                "text":
                                    f"🚀 РОСТ\n\n"
                                    f"🪙 Монета: {sym}\n"
                                    f"💰 Цена: {price}\n"
                                    f"📈 Изменение: +{change:.2f}%\n"
                                    f"⏱ Период: {current_window // 60} мин",
                                "reply_markup": {
                                    "inline_keyboard": [
                                        [
                                            {
                                                "text": f"📈 Открыть {sym} на Bybit",
                                                "url": f"https://www.bybit.com/trade/usdt/{sym}"
                                            }
                                        ]
                                    ]
                                }
                            }
                        )

                    else:

                        requests.post(
                            f"{URL}/sendMessage",
                            json={
                                "chat_id": config.CHAT_ID,
                                "text":
                                    f"📉 ПАДЕНИЕ\n\n"
                                    f"🪙 Монета: {sym}\n"
                                    f"💰 Цена: {price}\n"
                                    f"📉 Изменение: {change:.2f}%\n"
                                    f"⏱ Период: {current_window // 60} мин",
                                "reply_markup": {
                                    "inline_keyboard": [
                                        [
                                            {
                                                "text": f"📈 Открыть {sym} на Bybit",
                                                "url": f"https://www.bybit.com/trade/usdt/{sym}"
                                            }
                                        ]
                                    ]
                                }
                            }
                        )

                    signal_count += 1
                    last_alert[sym] = now
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

        requests.post(
            f"{URL}/sendMessage",
            json={
                "chat_id": chat_id,
                "text":
                    f"🚀 Бот запущен\n\n"
                    f"📈 Порог: {current_percent}%\n"
                    f"⏱ Период: {current_window // 60} мин\n\n"
                    f"Выберите настройки:",
                "reply_markup": get_keyboard()
            }
        )

    elif text == "/status":

        send_message(
            f"📊 Настройки\n\n"
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"🔔 Повтор сигнала: {config.COOLDOWN // 60} мин",
            chat_id
        )

    elif text == "/stats":

        send_message(
            f"📊 Статистика\n\n"
            f"🪙 Монет отслеживается: {len(price_history)}\n"
            f"📈 Порог: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"🔔 Повтор сигнала: {config.COOLDOWN // 60} мин\n"
            f"📨 Отправлено сигналов: {signal_count}",
            chat_id
        )


def handle_callback(callback):

    global current_percent
    global current_window

    data = callback["data"]

    chat_id = callback["message"]["chat"]["id"]
    message_id = callback["message"]["message_id"]

    if data.startswith("p_"):
        current_percent = float(data.split("_")[1])

    elif data.startswith("w_"):
        current_window = int(data.split("_")[1]) * 60

    requests.post(
        f"{URL}/answerCallbackQuery",
        json={
            "callback_query_id": callback["id"]
        }
    )

    requests.post(
        f"{URL}/editMessageText",
        json={
            "chat_id": chat_id,
            "message_id": message_id,
            "text":
                f"⚙ Настройки\n\n"
                f"📈 Порог: {current_percent}%\n"
                f"⏱ Период: {current_window // 60} мин",
            "reply_markup": get_keyboard()
        }
    )


async def telegram_loop():

    global offset

    while True:

        updates = get_updates()

        for update in updates:

            offset = update["update_id"] + 1

            if "message" in update:
                handle_message(update["message"])

            if "callback_query" in update:
                handle_callback(update["callback_query"])

        await asyncio.sleep(1)


async def main():

    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


asyncio.run(main())
