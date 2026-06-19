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
    )
```
