from telegram import Bot

# ВСТАВЬ СВОИ ДАННЫЕ
BOT_TOKEN = "8626739818:AAFt7kmdfTgTVlXD-5FnKOVYq1fvNW9hUAw"
CHAT_ID = 6716942872  # без кавычек

main = Bot(token=BOT_TOKEN)

try:
    msg = main.send_message(
        chat_id=CHAT_ID,
        text="✅ Тестовое уведомление\nБот работает."
    )

    print("Сообщение отправлено. ID:", msg.message_id)

except Exception as e:
    print("Ошибка Telegram:", e)
