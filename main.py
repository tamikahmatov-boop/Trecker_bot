import asyncio
import requests
from bs4 import BeautifulSoup

from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

import config

BOT = Bot(token=config.BOT_TOKEN)

price_history = {}
last_alert = {}

current_percent = config.PERCENT
current_window = config.WINDOW


# -------------------- TELEGRAM UI --------------------

def get_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("5%", callback_data="p_5"),
            InlineKeyboardButton("10%", callback_data="p_10"),
            InlineKeyboardButton("15%", callback_data="p_15"),
        ],
        [
            InlineKeyboardButton("20%", callback_data="p_20"),
            InlineKeyboardButton("25%", callback_data="p_25"),
            InlineKeyboardButton("30%", callback_data="p_30"),
        ],
        [
            InlineKeyboardButton("15m", callback_data="t_15m"),
            InlineKeyboardButton("30m", callback_data="t_30m"),
            InlineKeyboardButton("1h", callback_data="t_1h"),
        ],
        [
            InlineKeyboardButton("2h", callback_data="t_2h"),
            InlineKeyboardButton("4h", callback_data="t_4h"),
        ]
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🚀 Бот запущен\n\nРост: {current_percent}%\nПериод: {current_window//60} мин",
        reply_markup=get_keyboard()
    )


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_percent, current_window

    q = update.callback_query
    await q.answer()

    data = q.data

    if data.startswith("p_"):
        current_percent = int(data.split("_")[1])

    if data.startswith("t_"):
        current_window = config.WINDOWS[data.split("_")[1]]

    await q.edit_message_text(
        f"📊 Настройки\nРост: {current_percent}%\nПериод: {current_window//60} мин",
        reply_markup=get_keyboard()
    )


# -------------------- MARKET --------------------

def get_bybit_symbols():
    symbols = set()
    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a"):
            s = a.text.strip("/")
            if s.endswith("USDT"):
                symbols.add(s)
    except:
        pass

    return symbols


def get_prices(symbols):
    prices = {}

    try:
        r = requests.get("https://contract.mexc.com/api/v1/contract/ticker", timeout=20)
        data = r.json()

        if data["success"]:
            for i in data["data"]:
                sym = i["symbol"].replace("_", "")
                if sym in symbols:
                    prices[sym] = float(i["lastPrice"])
    except:
        pass

    return prices


# -------------------- LOOP --------------------

async def monitor():
    symbols = get_bybit_symbols()

    while True:
        now = asyncio.get_event_loop().time()
        prices = get_prices(symbols)

        for sym, price in prices.items():

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
            growth = ((price - old) / old) * 100

            if growth >= current_percent:

                if sym in last_alert and now - last_alert[sym] < config.COOLDOWN:
                    continue

                await BOT.send_message(
                    config.CHAT_ID,
                    f"🚀 Сигнал\n{sym}\nРост: +{growth:.2f}%"
                )

                last_alert[sym] = now

        await asyncio.sleep(60)


# -------------------- MAIN --------------------

async def post_init(app):
    asyncio.create_task(monitor())


def main():
    app = (
        Application.builder()
        .token(config.BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))

    app.run_webhook(
        listen="0.0.0.0",
        port=8080,
        url_path=config.BOT_TOKEN,
        webhook_url=f"https://YOUR-RAILWAY-URL/{config.BOT_TOKEN}"
    )


if __name__ == "__main__":
    main()
