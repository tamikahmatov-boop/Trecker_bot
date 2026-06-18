import requests
import time

BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = "6716942872"

running = True

THRESHOLD = 15
WINDOW = 300

CHECK_INTERVAL = 30
ALERT_COOLDOWN = 300

history = {}
last_alert = {}

top_pumps = []

message_id = None
offset = 0

percent_buttons = [5, 10, 15, 20, 25, 30]

time_buttons = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600
}


def keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "▶️ START", "callback_data": "start"},
                {"text": "⏹ STOP", "callback_data": "stop"}
            ],
            [
                {"text": "📊 STATUS", "callback_data": "status"},
                {"text": "🔥 TOP", "callback_data": "top"}
            ],
            [
                {"text": "5%", "callback_data": "p5"},
                {"text": "10%", "callback_data": "p10"},
                {"text": "15%", "callback_data": "p15"}
            ],
            [
                {"text": "20%", "callback_data": "p20"},
                {"text": "25%", "callback_data": "p25"},
                {"text": "30%", "callback_data": "p30"}
            ],
            [
                {"text": "1м", "callback_data": "t1m"},
                {"text": "5м", "callback_data": "t5m"},
                {"text": "15м", "callback_data": "t15m"}
            ],
            [
                {"text": "30м", "callback_data": "t30m"},
                {"text": "1ч", "callback_data": "t1h"}
            ]
        ]
    }


def send(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text
            },
            timeout=20
        )
    except Exception as e:
        print(e)


def send_dashboard(text):
    global message_id

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text,
                "reply_markup": str(keyboard()).replace("'", '"')
            },
            timeout=20
        ).json()

        if r.get("ok"):
            message_id = r["result"]["message_id"]

    except Exception as e:
        print(e)


def edit_dashboard(text):
    if message_id is None:
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            data={
                "chat_id": CHAT_ID,
                "message_id": message_id,
                "text": text,
                "reply_markup": str(keyboard()).replace("'", '"')
            }
        )
    except:
        pass

def handle_updates():
    global offset
    global running
    global THRESHOLD
    global WINDOW

    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params={
                "offset": offset + 1,
                "timeout": 0
            },
            timeout=20
        ).json()

        for upd in r.get("result", []):

            offset = upd["update_id"]

            if "callback_query" not in upd:
                continue

            data = upd["callback_query"]["data"]

            # START / STOP
            if data == "start":
                running = True
                send("🟢 Бот включен")

            elif data == "stop":
                running = False
                send("🔴 Бот остановлен")

            # STATUS
            elif data == "status":

                send(
                    f"📊 Статус\n\n"
                    f"Работает: {running}\n"
                    f"Порог: {THRESHOLD}%\n"
                    f"Период: {WINDOW//60} мин\n"
                    f"Монет в памяти: {len(history)}"
                )

            # TOP
            elif data == "top":

                if len(top_pumps) == 0:
                    send("Нет данных")

                else:

                    text = "🔥 TOP PUMPS\n\n"

                    for symbol, growth in top_pumps[:10]:
                        text += f"{symbol} +{growth:.2f}%\n"

                    send(text)

            # Проценты
            elif data.startswith("p"):

                THRESHOLD = int(data[1:])

                send(f"✅ Новый порог: {THRESHOLD}%")

            # Время
            elif data == "t1m":
                WINDOW = 60
                send("✅ Период: 1 минута")

            elif data == "t5m":
                WINDOW = 300
                send("✅ Период: 5 минут")

            elif data == "t15m":
                WINDOW = 900
                send("✅ Период: 15 минут")

            elif data == "t30m":
                WINDOW = 1800
                send("✅ Период: 30 минут")

            elif data == "t1h":
                WINDOW = 3600
                send("✅ Период: 1 час")

    except Exception as e:
        print("UPDATE ERROR:", e)


def get_prices():

    try:

        r = requests.get(
            "https://api.mexc.com/api/v3/ticker/price",
            timeout=30
        )

        data = r.json()

        if not isinstance(data, list):
            return []

        coins = []

        for coin in data:

            symbol = coin.get("symbol")

            if not symbol:
                continue

            if not symbol.endswith("USDT"):
                continue

            try:
                price = float(coin["price"])
            except:
                continue

            coins.append(
                {
                    "symbol": symbol,
                    "price": price
                }
            )

        return coins

    except Exception as e:

        print("MEXC ERROR:", e)

        return []

send("🟢 MEXC BOT запущен")

send_dashboard("📊 Загрузка рынка...")

while True:

    try:

        handle_updates()

        if not running:
            time.sleep(2)
            continue

        now = time.time()

        coins = get_prices()

        pumps = []

        for coin in coins:

            symbol = coin["symbol"]
            price = coin["price"]

            if symbol not in history:
                history[symbol] = []

            history[symbol].append((now, price))

            # оставляем только данные за выбранный период
            history[symbol] = [
                x for x in history[symbol]
                if now - x[0] <= WINDOW
            ]

            if len(history[symbol]) < 2:
                continue

            old_price = history[symbol][0][1]

            if old_price <= 0:
                continue

            growth = (price - old_price) / old_price * 100

            pumps.append((symbol, growth))

            if growth >= THRESHOLD:

                if (
                    symbol not in last_alert
                    or now - last_alert[symbol] > ALERT_COOLDOWN
                ):

                    send(
                        f"🚀 {symbol}\n\n"
                        f"Рост: +{growth:.2f}%\n"
                        f"Период: {WINDOW // 60} мин\n"
                        f"Цена: {price}"
                    )

                    last_alert[symbol] = now

        pumps.sort(key=lambda x: x[1], reverse=True)

        top_pumps = pumps[:10]

        dashboard = (
            "📊 MEXC BOT\n\n"
            f"{'🟢 ВКЛ' if running else '🔴 ВЫКЛ'}\n"
            f"📈 Порог: {THRESHOLD}%\n"
            f"⏱ Период: {WINDOW // 60} мин\n"
            f"🪙 Монет: {len(coins)}\n"
            f"🔥 Сигналов: {len(top_pumps)}\n\n"
            "Кнопки ниже ↓"
        )

        edit_dashboard(dashboard)

        time.sleep(CHECK_INTERVAL)

    except Exception as e:

        print("MAIN ERROR:", e)
