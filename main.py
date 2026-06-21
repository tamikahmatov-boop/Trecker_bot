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
        response = requests.post(
            f"{URL}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text
            },
            timeout=20
        )

        if not response.ok:
            print("Ошибка Telegram:", response.text)

    except Exception as e:
        print("Ошибка Telegram:", e)
def get_symbols():
    urls = [
        "https://api.bybit-global.com/v5/market/instruments-info?category=linear&limit=1000",
        "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=20)

            # защита от HTML / Cloudflare / блокировок
            try:
                data = r.json()
            except Exception:
                print("Bybit response (не JSON):")
                print(r.text[:500])  # ограничиваем вывод
                continue

            # проверка структуры ответа
            if not isinstance(data, dict):
                print("Bybit response (не dict):", type(data))
                continue

            symbols = set()

            # нормальный ответ Bybit
            if data.get("retCode") == 0 and "result" in data:
                for item in data["result"].get("list", []):
                    symbol = item.get("symbol")

                    if symbol and symbol.endswith("USDT"):
                        symbols.add(symbol)

                print(f"Загружено Bybit Futures: {len(symbols)}")
                return symbols

            else:
                print("Bybit API error:", data)

        except Exception as e:
            print("Ошибка Bybit URL:", url, e)

    # если всё упало
    print("Bybit недоступен — возвращаем пустой список")
    return set()
def get_prices_mexc(symbols):
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
def get_prices_okx(symbols):
    prices = {}

    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=20
        )

        data = r.json()

        if data["code"] == "0":

            for item in data["data"]:

                symbol = (
                    item["instId"]
                    .replace("-", "")
                    .replace("SWAP", "")
                )

                if symbol in symbols:

                    try:
                        price = float(item["last"])

                        if price > 0:
                            prices[symbol] = price

                    except:
                        pass

    except Exception as e:
        print("Ошибка OKX:", e)

    return prices

def get_prices_bitget(symbols):
    prices = {}

    try:
        r = requests.get(
            "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
            timeout=20
        )

        data = r.json()

        if data["code"] == "00000":

            for item in data["data"]:

                symbol = item["symbol"].replace("_", "")

                if symbol in symbols:

                    try:
                        price = float(item["lastPr"])

                        if price > 0:
                            prices[symbol] = price

                    except:
                        pass

    except Exception as e:
        print("Ошибка Bitget:", e)

    return prices
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
    global current_percent, current_window

    symbols = get_symbols()
    last_symbols_update = time.time()

    send_message(
        f"✅ Бот запущен\n\n"
        f"Монет: {len(symbols)}",
        config.CHAT_ID
    )

    while True:
        try:
            now = time.time()

            # обновляем список монет каждый час
            if now - last_symbols_update >= 3600:
                symbols = get_symbols()
                last_symbols_update = now
                print("Список монет обновлен")

            # получаем цены с бирж
            mexc_prices = get_prices_mexc(symbols)
            okx_prices = get_prices_okx(symbols)
            bitget_prices = get_prices_bitget(symbols)

            prices = {}

            # MEXC — основной источник
            for sym, price in mexc_prices.items():
                prices[sym] = (price, "MEXC")

            # OKX — резервный
            for sym, price in okx_prices.items():
                if sym not in prices:
                    prices[sym] = (price, "OKX")

            # Bitget — резервный
            for sym, price in bitget_prices.items():
                if sym not in prices:
                    prices[sym] = (price, "Bitget")

            for sym, (price, exchange) in prices.items():

                if price <= 0:
                    continue

                if sym not in price_history:
                    price_history[sym] = []

                # сохраняем цену
                price_history[sym].append((now, price))

                # храним историю 24 часа
                price_history[sym] = [
                    x for x in price_history[sym]
                    if now - x[0] <= 86400
                ]

                if len(price_history[sym]) < 2:
                    continue

                # цена current_window назад
                old_prices = [
                    p for t, p in price_history[sym]
                    if now - t >= current_window
                ]

                if not old_prices:
                    continue

                old_price = old_prices[0]

                if old_price <= 0:
                    continue

                growth = ((price - old_price) / old_price) * 100

                # RSI по последним 100 значениям
                prices_list = [p for _, p in price_history[sym][-100:]]
                rsi = calculate_rsi(prices_list, window=5)

                # сигнал на рост и падение
                if abs(growth) >= current_percent:

                    # антиспам
                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    if growth > 0:
                        text = (
                            f"🚀 СИГНАЛ\n\n"
                            f"Монета: {sym}\n"
                            f"Биржа: {exchange}\n"
                            f"Цена: {price}\n"
                            f"Рост: +{growth:.2f}%\n"
                        )
                    else:
                        text = (
                            f"📉 СИГНАЛ\n\n"
                            f"Монета: {sym}\n"
                            f"Биржа: {exchange}\n"
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
            await asyncio.sleep(5)
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
            f"⏱ Период: {current_window} сек\n"
            f"🔔 Кулдаун: {config.COOLDOWN // 60} мин",
            chat_id
        )

    # ---------------- ПРОЦЕНТЫ ----------------

    elif text == "📈 0.2%":
        current_percent = 0.2
        send_message("✅ 0.2%", chat_id)

    elif text == "📈 5%":
        current_percent = 5
        send_message("✅ 5%", chat_id)

    elif text == "📈 10%":
        current_percent = 10
        send_message("✅ 10%", chat_id)

    elif text == "📈 15%":
        current_percent = 15
        send_message("✅ 15%", chat_id)

    elif text == "📈 20%":
        current_percent = 20
        send_message("✅ 20%", chat_id)

    # ---------------- ВРЕМЯ ----------------

    elif text == "⏱ 5 мин":
        current_window = 300
        send_message("✅ 5 мин", chat_id)

    elif text == "⏱ 1 час":
        current_window = 3600
        send_message("✅ 1 час", chat_id)

    elif text == "⏱ 4 часа":
        current_window = 14400
        send_message("✅ 4 часа", chat_id)

    elif text == "⏱ 1 день":
        current_window = 86400
        send_message("✅ 1 день", chat_id)

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
            },
            timeout=35
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


while True:
    try:
        asyncio.run(main())
    except Exception as e:
        print("Критическая ошибка:", e)
        time.sleep(10)
