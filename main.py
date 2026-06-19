```python
import asyncio
import time
import requests
from bs4 import BeautifulSoup

import config

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update
)

from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

price_history = {}
last_alert = {}

current_percent = config.PERCENT
current_window = config.WINDOW

bot = Bot(token=config.BOT_TOKEN)


def get_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("5%", callback_data="p_5"),
            InlineKeyboardButton("10%", callback_data="p_10"),
            InlineKeyboardButton("15%", callback_data="p_15")
        ],
        [
            InlineKeyboardButton("20%", callback_data="p_20"),
            InlineKeyboardButton("25%", callback_data="p_25"),
            InlineKeyboardButton("30%", callback_data="p_30")
        ],
        [
            InlineKeyboardButton("15 мин", callback_data="t_15m"),
            InlineKeyboardButton("30 мин", callback_data="t_30m")
        ],
        [
            InlineKeyboardButton("1 час", callback_data="t_1h"),
            InlineKeyboardButton("2 часа", callback_data="t_2h"),
            InlineKeyboardButton("4 часа", callback_data="t_4h")
        ]
    ]

    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text = (
        "✅ Бот запущен\n\n"
        f"Рост: {current_percent}%\n"
        f"Период: {current_window // 60} мин"
    )

    await update.message.reply_text(
        text,
        reply_markup=get_keyboard()
    )


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):

    global current_percent
    global current_window

    query = update.callback_query

    await query.answer()

    data = query.data

    if data.startswith("p_"):
        current_percent = int(data.split("_")[1])

    elif data.startswith("t_"):
        key = data.split("_")[1]
        current_window = config.WINDOWS[key]

    await query.edit_message_text(
        text=(
            f"📈 Рост: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин"
        ),
        reply_markup=get_keyboard()
    )```python
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


async def monitor():

    global current_percent
    global current_window

    bybit_symbols = get_bybit_symbols()

    while True:

        try:
            now = time.time()

            prices = get_mexc_prices(bybit_symbols)

            for symbol, price in prices.items():

                if symbol not in price_history:
                    price_history[symbol] = []

                price_history[symbol].append((now, price))

                price_history[symbol] = [
                    x for x in price_history[symbol]
                    if now - x[0] <= current_window
                ]

                if len(price_history[symbol]) < 2:
                    continue

                old_price = price_history[symbol][0][1]

                growth = ((price - old_price) / old_price) * 100

                if growth >= current_percent:

                    if symbol in last_alert:
                        if now - last_alert[symbol] < config.COOLDOWN:
                            continue

                    await bot.send_message(
                        chat_id=config.CHAT_ID,
                        text=(
                            f"🚀 Сигнал\n\n"
                            f"Монета: {symbol}\n"
                            f"Цена: {price}\n"
                            f"Рост: +{growth:.2f}%\n"
                            f"Период: {current_window // 60} мин"
                        )
                    )

                    last_alert[symbol] = now

            await asyncio.sleep(config.INTERVAL)

        except Exception as e:
            print("Ошибка:", e)
            await asyncio.sleep(config.INTERVAL)
```

``` 


