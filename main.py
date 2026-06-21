import asyncio
import requests
import time
import pandas as pd
from ta.momentum import RSIIndicator
import config

TOKEN = config.BOT_TOKEN
URL = f"https://api.telegram.org/bot{TOKEN}"

offset = 0

price_history = {}
last_alert = {}

current_percent = config.PERCENT
current_window = config.WINDOW


# ---------------- TELEGRAM ----------------

def send_message(text, chat_id):
    try:
        requests.post(
            f"{URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
    except Exception as e:
        print("Telegram error:", e)


# ---------------- SYMBOLS (OKX as BASE LIST) ----------------

def normalize(sym: str):
    return sym.replace("-", "").replace("_", "").upper()


def get_symbols():
    """
    Берём список символов с OKX (самый стабильный источник)
    """
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SPOT"},
            timeout=20
        )

        data = r.json()

        symbols = set()

        if data.get("code") == "0":
            for item in data.get("data", []):
                sym = normalize(item.get("instId", ""))
                if sym:
                    symbols.add(sym)

        print("Symbols loaded:", len(symbols))
        return symbols

    except Exception as e:
        print("Symbols error:", e)
        return set()


# ---------------- EXCHANGES ----------------

def get_okx():
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SPOT"},
            timeout=20
        )

        data = r.json().get("data", [])
        prices = {}

        for item in data:
            sym = normalize(item.get("instId", ""))
            price = item.get("last")

            if sym and price:
                prices[sym] = float(price)

        return prices

    except Exception as e:
        print("OKX error:", e)
        return {}


def get_mexc():
    try:
        r = requests.get("https://api.mexc.com/api/v3/ticker/price", timeout=20)
        return {item["symbol"]: float(item["price"]) for item in r.json()}
    except:
        return {}


def get_bitget():
    try:
        r = requests.get(
            "https://api.bitget.com/api/spot/v1/market/tickers",
            timeout=20
        )

        data = r.json().get("data", [])
        if not isinstance(data, list):
            return {}

        prices = {}

        for item in data:
            sym = item.get("symbol")
            price = item.get("close")

            if sym and price:
                try:
                    prices[sym] = float(price)
                except:
                    pass

        return prices

    except Exception as e:
        print("Bitget error:", e)
        return {}


def get_kucoin():
    try:
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            timeout=20
        )

        data = r.json()["data"]["ticker"]
        prices = {}

        for item in data:
            sym = item["symbol"].replace("-", "")
            prices[sym] = float(item["last"])

        return prices

    except:
        return {}


def get_bingx():
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/spot/v1/ticker/price",
            timeout=20
        )

        data = r.json().get("data", [])
        prices = {}

        for item in data:
            sym = item.get("symbol")
            price = item.get("price") or item.get("lastPrice") or item.get("last")

            if sym and price:
                try:
                    prices[sym] = float(price)
                except:
                    pass

        return prices

    except Exception as e:
        print("BingX error:", e)
        return {}


# ---------------- COMBINED PRICES ----------------

def get_prices(symbols):
    all_prices = {}

    sources = [
        get_okx,
        get_mexc,
        get_bitget,
        get_kucoin,
        get_bingx
    ]

    for source in sources:
        try:
            data = source()

            for sym, price in data.items():
                if normalize(sym) in symbols:
                    all_prices[normalize(sym)] = price

        except Exception as e:
            print("Source error:", e)

    return all_prices


# ---------------- RSI ----------------

def calculate_rsi(prices, window=5):
    try:
        if len(prices) < window + 1:
            return None

        series = pd.Series(prices)
        rsi = RSIIndicator(close=series, window=window).rsi().iloc[-1]

        if pd.isna(rsi):
            return None

        return round(float(rsi), 2)

    except:
        return None


# ---------------- MONITOR ----------------

async def monitor():
    global current_percent, current_window

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

                # cleanup
                history_time = max(current_window * 2, 86400)
                price_history[sym] = [
                    x for x in price_history[sym]
                    if now - x[0] <= history_time
                ]

                recent = [
                    x for x in price_history[sym]
                    if now - x[0] <= current_window
                ]

                if len(recent) < 2:
                    continue

                old_price = recent[0][1]

                growth = ((price - old_price) / old_price) * 100

                prices_list = [x[1] for x in price_history[sym][-100:]]
                rsi = calculate_rsi(prices_list)

                if abs(growth) >= current_percent:

                    if sym in last_alert:
                        if now - last_alert[sym] < config.COOLDOWN:
                            continue

                    text = (
                        f"🚀 СИГНАЛ\n\n"
                        f"Монета: {sym}\n"
                        f"Цена: {price}\n"
                        f"{'Рост' if growth > 0 else 'Падение'}: {growth:.2f}%\n"
                        f"RSI: {rsi if rsi else '—'}"
                    )

                    send_message(text, config.CHAT_ID)
                    last_alert[sym] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            print("monitor error:", e)
            await asyncio.sleep(3)


# ---------------- TELEGRAM ----------------

def get_updates():
    global offset

    try:
        r = requests.get(
            f"{URL}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=35
        )

        data = r.json()
        return data.get("result", [])

    except:
        return []


async def telegram_loop():
    global offset

    while True:
        updates = get_updates()

        for u in updates:
            offset = u["update_id"] + 1
            if "message" in u:
                handle_message(u["message"])

        await asyncio.sleep(1)


def handle_message(msg):
    global current_percent, current_window

    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]

    if text == "/start":
        send_message(
            f"🚀 Бот\n📈 {current_percent}%\n⏱ {current_window}s",
            chat_id
        )

    elif text == "/status":
        send_message(
            f"📊 {current_percent}%\n⏱ {current_window}s",
            chat_id
        )

    elif text == "📈 5%":
        current_percent = 5
        send_message("OK", chat_id)

    elif text == "⏱ 5 мин":
        current_window = 300
        send_message("OK", chat_id)


# ---------------- MAIN ----------------

async def main():
    await asyncio.gather(
        monitor(),
        telegram_loop()
    )


while True:
    try:
        asyncio.run(main())
    except Exception as e:
        print("CRITICAL:", e)
        time.sleep(10)
