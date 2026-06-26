import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Set, Optional, Tuple, Any
from datetime import datetime

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from ta.momentum import RSIIndicator

import config

# ========== НАСТРОЙКИ ==========

@dataclass
class BotConfig:
    percent: float = 5.0
    window: int = 300  # секунд
    interval: int = 10  # секунд между проверками
    cooldown: int = 600  # секунд между сигналами для одной монеты
    symbols_update_interval: int = 1800  # 30 минут
    state_save_interval: int = 30
    heartbeat_interval: int = 300
    price_history_limit: int = 100  # для RSI
    max_retries: int = 3
    retry_delay: int = 2

# ========== ЛОГГИРОВАНИЕ ==========

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========

class BotState:
    def __init__(self):
        self.price_history: Dict[str, List[Tuple[float, float]]] = {}
        self.last_alert: Dict[str, float] = {}
        self.last_alert_growth: Dict[str, float] = {}
        self.signals_count: int = 0
        self.checks_count: int = 0
        self.start_time: float = time.time()
        self.last_symbols_update: float = 0
        self.symbols: Set[str] = set()
        
    def get_uptime(self) -> str:
        uptime = int(time.time() - self.start_time)
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
        minutes = (uptime % 3600) // 60
        return f"{days}д {hours}ч {minutes}м"

bot_state = BotState()
bot_config = BotConfig()

# ========== РАБОТА С СОСТОЯНИЕМ ==========

STATE_FILE = "state.json"

def save_state():
    """Сохраняет состояние бота в файл"""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "last_alert": bot_state.last_alert,
                "last_alert_growth": bot_state.last_alert_growth,
                "signals_count": bot_state.signals_count,
                "checks_count": bot_state.checks_count,
                "start_time": bot_state.start_time,
                "bot_config": {
                    "percent": bot_config.percent,
                    "window": bot_config.window,
                    "interval": bot_config.interval,
                    "cooldown": bot_config.cooldown
                }
            }, f, indent=2)
        logger.debug("State saved")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_state():
    """Загружает состояние бота из файла"""
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            bot_state.last_alert = data.get("last_alert", {})
            bot_state.last_alert_growth = data.get("last_alert_growth", {})
            bot_state.signals_count = data.get("signals_count", 0)
            bot_state.checks_count = data.get("checks_count", 0)
            bot_state.start_time = data.get("start_time", time.time())
            
            # Загружаем настройки если они есть
            if "bot_config" in data:
                cfg = data["bot_config"]
                bot_config.percent = cfg.get("percent", bot_config.percent)
                bot_config.window = cfg.get("window", bot_config.window)
                bot_config.interval = cfg.get("interval", bot_config.interval)
                bot_config.cooldown = cfg.get("cooldown", bot_config.cooldown)
        logger.info("State loaded successfully")
    except FileNotFoundError:
        logger.info("No saved state found, starting fresh")
    except Exception as e:
        logger.error(f"Failed to load state: {e}")

# Загружаем состояние при старте
load_state()

# ========== TELEGRAM ==========

TOKEN = config.BOT_TOKEN
URL = f"https://api.telegram.org/bot{TOKEN}"
offset = 0
telegram_semaphore = asyncio.Semaphore(10)  # Rate limiting

async def send_message(text: str, chat_id: int) -> bool:
    """Асинхронная отправка сообщения с rate limiting"""
    async with telegram_semaphore:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{URL}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        logger.debug(f"Message sent to {chat_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Telegram error: {response.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

async def send_keyboard(chat_id: int):
    """Отправляет клавиатуру с настройками"""
    keyboard = {
        "keyboard": [
            ["📈 0.2%", "📈 5%", "📈 10%", "📈 15%", "📈 20%"],
            ["⏱ 5м", "⏱ 1ч", "⏱ 4ч", "⏱ 1д"],
            ["📊 Статистика", "🔄 Обновить", "/status"]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{URL}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": "📊 Выберите настройки:",
                    "reply_markup": keyboard
                },
                timeout=aiohttp.ClientTimeout(total=10)
            )
    except Exception as e:
        logger.error(f"Error sending keyboard: {e}")

# ========== НОРМАЛИЗАЦИЯ ==========

def normalize_symbol(sym: str) -> str:
    """Нормализует символ для поиска"""
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")

# ========== ПОЛУЧЕНИЕ СПИСКА МОНЕТ ==========

def get_symbols() -> Set[str]:
    """Получает список монет с Bybit"""
    symbols = set()
    
    # Bybit Futures
    try:
        r = requests.get("https://public.bybit.com/trading/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        
        for a in soup.find_all("a"):
            sym = a.text.strip("/")
            if sym.endswith(("USDT", "PERP")):
                symbols.add(sym.replace("/", ""))
        
        logger.info(f"Found {len(symbols)} symbols on Bybit Futures")
    except Exception as e:
        logger.error(f"Error fetching trading symbols: {e}")
    
    # Bybit Spot
    try:
        r = requests.get("https://public.bybit.com/spot/", timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        
        for a in soup.find_all("a"):
            sym = a.text.strip("/")
            if sym.endswith("USDT"):
                symbols.add(sym)
    except Exception as e:
        logger.error(f"Error fetching spot symbols: {e}")
    
    logger.info(f"Total symbols: {len(symbols)}")
    return symbols

# ========== ПОЛУЧЕНИЕ ЦЕН ==========

async def get_prices(symbols: Set[str], session: aiohttp.ClientSession) -> Tuple[Dict[str, float], Dict[str, str]]:
    """Получает цены с OKX и MEXC"""
    prices = {}
    sources = {}
    normalized_symbols = {normalize_symbol(s): s for s in symbols}
    
    # OKX
    try:
        async with session.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            data = await response.json()
            if "data" in data:
                for item in data["data"]:
                    inst = item["instId"]
                    price = float(item["last"])
                    sym = normalize_symbol(inst)
                    
                    if price > 0 and sym in normalized_symbols:
                        real_sym = normalized_symbols[sym]
                        prices[real_sym] = price
                        sources[real_sym] = "OKX"
    except Exception as e:
        logger.error(f"Error fetching OKX prices: {e}")
    
    # MEXC
    try:
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            data = await response.json()
            if data.get("success"):
                for item in data["data"]:
                    sym = normalize_symbol(item["symbol"])
                    price = float(item["lastPrice"])
                    
                    if price > 0 and sym in normalized_symbols:
                        real_sym = normalized_symbols[sym]
                        if real_sym not in prices:  # Приоритет OKX
                            prices[real_sym] = price
                            sources[real_sym] = "MEXC"
    except Exception as e:
        logger.error(f"Error fetching MEXC prices: {e}")
    
    return prices, sources

# ========== RSI ==========

def calculate_rsi(prices: List[float], window: int = 5) -> Optional[float]:
    """Вычисляет RSI"""
    try:
        if len(prices) < window + 1:
            return None
        
        series = pd.Series(prices)
        rsi = RSIIndicator(close=series, window=window).rsi().iloc[-1]
        
        if pd.isna(rsi):
            return None
        
        return round(float(rsi), 2)
    except Exception as e:
        logger.error(f"Error calculating RSI: {e}")
        return None

# ========== МОНИТОРИНГ ==========

async def monitor():
    """Основной цикл мониторинга цен"""
    logger.info("Monitor started")
    
    # Первоначальное получение списка монет
    bot_state.symbols = get_symbols()
    bot_state.last_symbols_update = time.time()
    
    await send_message("✅ Бот запущен", config.CHAT_ID)
    
    while True:
        try:
            now = time.time()
            bot_state.checks_count += 1
            
            # Обновление списка монет
            if now - bot_state.last_symbols_update >= bot_config.symbols_update_interval:
                new_symbols = get_symbols()
                if len(new_symbols) > 0:
                    bot_state.symbols = new_symbols
                    logger.info(f"Symbols updated: {len(bot_state.symbols)}")
                else:
                    logger.warning("Failed to update symbols, using old list")
                bot_state.last_symbols_update = now
            
            if not bot_state.symbols:
                logger.warning("No symbols available, skipping check")
                await asyncio.sleep(bot_config.interval)
                continue
            
            # Получение цен
            async with aiohttp.ClientSession() as session:
                prices, sources = await get_prices(bot_state.symbols, session)
            
            # Проверка каждой монеты
            for sym, price in prices.items():
                if price <= 0:
                    continue
                
                # Инициализация истории
                if sym not in bot_state.price_history:
                    bot_state.price_history[sym] = []
                
                # Добавление цены
                bot_state.price_history[sym].append((now, price))
                
                # Очистка старых данных
                history_time = max(bot_config.window * 2, 86400)
                cutoff = now - history_time
                bot_state.price_history[sym] = [
                    x for x in bot_state.price_history[sym]
                    if x[0] > cutoff
                ]
                
                # Проверка изменения цены
                recent_prices = [
                    x for x in bot_state.price_history[sym]
                    if now - x[0] <= bot_config.window
                ]
                
                if len(recent_prices) < 2:
                    continue
                
                old_price = recent_prices[0][1]
                if old_price <= 0:
                    continue
                
                growth = ((price - old_price) / old_price) * 100
                
                # Вычисление RSI
                prices_list = [x[1] for x in bot_state.price_history[sym][-bot_config.price_history_limit:]]
                rsi = calculate_rsi(prices_list)
                
                # Проверка на сигнал
                if abs(growth) >= bot_config.percent:
                    # Антиспам
                    if sym in bot_state.last_alert:
                        if now - bot_state.last_alert[sym] < bot_config.cooldown:
                            continue
                    
                    # Формирование сообщения
                    source = sources.get(sym, "UNKNOWN")
                    if growth > 0:
                        text = f"🚀 СИГНАЛ\n\nМонета: {sym}\nЦена: {price} ({source})\nРост: +{growth:.2f}%"
                    else:
                        text = f"📉 СИГНАЛ\n\nМонета: {sym}\nЦена: {price} ({source})\nПадение: {growth:.2f}%"
                    
                    if rsi is not None:
                        text += f"\n📊 RSI: {rsi:.2f}"
                    
                    # Отправка сигнала
                    if await send_message(text, config.CHAT_ID):
                        bot_state.signals_count += 1
                        bot_state.last_alert[sym] = now
                        logger.info(f"Signal sent: {sym} {growth:.2f}%")
            
            await asyncio.sleep(bot_config.interval)
            
        except Exception as e:
            logger.error(f"Monitor error: {e}", exc_info=True)
            await asyncio.sleep(5)

# ========== ОБРАБОТКА КОМАНД ==========

def parse_time_period(text: str) -> Optional[int]:
    """Парсит текстовое представление времени"""
    mapping = {
        "⏱ 5м": 300,
        "⏱ 1ч": 3600,
        "⏱ 4ч": 14400,
        "⏱ 1д": 86400
    }
    return mapping.get(text)

def parse_percent(text: str) -> Optional[float]:
    """Парсит процент из текста"""
    if text.startswith("📈 "):
        try:
            return float(text.replace("📈 ", "").replace("%", ""))
        except ValueError:
            return None
    return None

async def handle_message(msg: Dict[str, Any]):
    """Обрабатывает сообщения от пользователя"""
    global offset
    
    text = msg.get("text", "")
    chat_id = msg["chat"]["id"]
    
    # Приветствие
    if text == "/start":
        await send_message(
            f"🚀 Бот запущен\n\n"
            f"📈 Порог: {bot_config.percent}%\n"
            f"⏱ Период: {bot_config.window // 60} мин\n"
            f"🔄 Интервал: {bot_config.interval} сек",
            chat_id
        )
        await send_keyboard(chat_id)
    
    # Статус
    elif text == "/status":
        await send_message(
            f"📊 Настройки\n\n"
            f"📈 Порог: {bot_config.percent}%\n"
            f"⏱ Период: {bot_config.window} сек ({bot_config.window // 60} мин)\n"
            f"🔄 Интервал: {bot_config.interval} сек\n"
            f"🔔 Кулдаун: {bot_config.cooldown // 60} мин",
            chat_id
        )
    
    # Статистика
    elif text == "📊 Статистика":
        await send_message(
            f"📊 СТАТИСТИКА\n\n"
            f"🟢 Время работы: {bot_state.get_uptime()}\n"
            f"🪙 Монет в истории: {len(bot_state.price_history)}\n"
            f"🔔 Сигналов: {bot_state.signals_count}\n"
            f"🔄 Проверок: {bot_state.checks_count}\n"
            f"📈 Порог: {bot_config.percent}%\n"
            f"⏱ Период: {bot_config.window // 60} мин\n"
            f"⚡ Интервал: {bot_config.interval} сек\n"
            f"🕒 Кулдаун: {bot_config.cooldown // 60} мин\n"
            f"📌 Активных алертов: {len(bot_state.last_alert)}",
            chat_id
        )
    
    # Обновить список монет
    elif text == "🔄 Обновить":
        await send_message("🔄 Обновляю список монет...", chat_id)
        bot_state.symbols = get_symbols()
        bot_state.last_symbols_update = time.time()
        await send_message(f"✅ Обновлено! {len(bot_state.symbols)} монет", chat_id)
    
    # Изменение процента
    elif text.startswith("📈 "):
        percent = parse_percent(text)
        if percent is not None:
            bot_config.percent = percent
            await send_message(f"✅ Порог изменен на {percent}%", chat_id)
        else:
            await send_message("❌ Неверный формат", chat_id)
    
    # Изменение периода
    elif text.startswith("⏱ "):
        window = parse_time_period(text)
        if window is not None:
            bot_config.window = window
            await send_message(f"✅ Период изменен на {window // 60} мин", chat_id)
        else:
            await send_message("❌ Неверный формат", chat_id)
    
    else:
        await send_message("❓ Неизвестная команда. Используйте /start", chat_id)

# ========== ПОЛУЧЕНИЕ ОБНОВЛЕНИЙ ==========

async def get_updates() -> List[Dict]:
    """Получает обновления от Telegram"""
    global offset
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{URL}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35)
            ) as response:
                data = await response.json()
                if data["ok"]:
                    return data["result"]
                else:
                    logger.error(f"Telegram API error: {data}")
                    return []
    except Exception as e:
        logger.error(f"Error getting updates: {e}")
        return []

async def telegram_loop():
    """Цикл обработки сообщений Telegram"""
    global offset
    
    logger.info("Telegram loop started")
    
    while True:
        try:
            updates = await get_updates()
            
            for update in updates:
                offset = update["update_id"] + 1
                if "message" in update:
                    await handle_message(update["message"])
            
        except Exception as e:
            logger.error(f"Telegram loop error: {e}", exc_info=True)
        
        await asyncio.sleep(0.2)

# ========== СЛУЖЕБНЫЕ ЗАДАЧИ ==========

async def heartbeat():
    """Периодическое логирование о работе бота"""
    while True:
        logger.info(f"Bot alive - Signals: {bot_state.signals_count}, Checks: {bot_state.checks_count}")
        await asyncio.sleep(bot_config.heartbeat_interval)

async def save_state_loop():
    """Периодическое сохранение состояния"""
    while True:
        save_state()
        await asyncio.sleep(bot_config.state_save_interval)

async def watchdog():
    """Следит за зависаниями монитора"""
    last_check = time.time()
    monitor_task = None
    
    while True:
        try:
            current_time = time.time()
            if current_time - last_check > 60:
                logger.warning("⚠️ Monitor may be stalled!")
            
            # Проверка, что количество проверок растет
            if bot_state.checks_count > 0:
                last_check = current_time
                
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Watchdog error: {e}")
            await asyncio.sleep(30)

# ========== ОБРАБОТЧИК ОШИБОК ==========

def handle_async_exception(loop, context):
    """Глобальный обработчик исключений"""
    exception = context.get("exception")
    if exception:
        logger.error(f"Async exception: {exception}", exc_info=exception)
    else:
        logger.error(f"Async error: {context.get('message', 'Unknown error')}")

# ========== RETRY ДЕКОРАТОР ==========

async def retry_async(func, retries: int = 3, delay: int = 2):
    """Повторяет асинхронную функцию при ошибке"""
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            if attempt == retries - 1:
                raise
            logger.warning(f"Retry {attempt + 1}/{retries}: {e}")
            await asyncio.sleep(delay)
    return None

# ========== GRACEFUL SHUTDOWN ==========

async def shutdown():
    """Корректное завершение работы"""
    logger.info("Shutting down...")
    save_state()
    logger.info("State saved. Goodbye!")

def signal_handler():
    """Обработчик сигналов для graceful shutdown"""
    import signal
    
    def handler(signum, frame):
        logger.info(f"Received signal {signum}")
        asyncio.create_task(shutdown())
        # Даем время на сохранение
        time.sleep(1)
        exit(0)
    
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

# ========== MAIN ==========

async def main():
    """Главная функция"""
    # Настройка обработчиков
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)
    
    # Обработка сигналов
    signal_handler()
    
    logger.info("=" * 50)
    logger.info("🚀 BOT STARTED")
    logger.info(f"📈 Threshold: {bot_config.percent}%")
    logger.info(f"⏱ Window: {bot_config.window}s")
    logger.info(f"🔄 Interval: {bot_config.interval}s")
    logger.info("=" * 50)
    
    # Запуск всех задач
    try:
        await asyncio.gather(
            monitor(),
            telegram_loop(),
            heartbeat(),
            save_state_loop(),
            watchdog()
        )
    except asyncio.CancelledError:
        await shutdown()
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        await shutdown()
        raise

# ========== ЗАПУСК ==========

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break
        except Exception as e:
            logger.critical(f"Critical error: {e}", exc_info=True)
            logger.info("Restarting in 10 seconds...")
            time.sleep(10)
