import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID = int(os.getenv("CHAT_ID", "0"))
    PERCENT = float(os.getenv("PERCENT", "20"))
    WINDOW = int(os.getenv("WINDOW", "360000"))
    INTERVAL = int(os.getenv("INTERVAL", "5"))
    COOLDOWN = int(os.getenv("COOLDOWN", "600"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    SYMBOLS_UPDATE_INTERVAL = 1800
    MAX_PRICE_HISTORY = 1000
    REQUEST_TIMEOUT = 20
    
    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN не установлен")
        return True

config = Config()
