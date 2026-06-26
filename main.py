import asyncio
import logging
import time
import signal
from config import config
from state_manager import StateManager
from telegram_client import TelegramClient
from price_fetcher import PriceFetcher
from analyzer import Analyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class CryptoBot:
    def __init__(self):
        self.config = config
        self.state = StateManager()
        self.analyzer = Analyzer()
        self.percent = config.PERCENT
        self.window = config.WINDOW
        self.interval = config.INTERVAL
        self.cooldown = config.COOLDOWN
        self.running = False
        self.checks = 0
        self.state.state.start_time = time.time()
    
    async def start(self):
        self.telegram = TelegramClient(config.BOT_TOKEN, config.CHAT_ID)
        self.fetcher = PriceFetcher()
        
        async with self.telegram, self.fetcher:
            await self.telegram.send_message("✅ Бот запущен")
            self.running = True
            
            tasks = [
                self.monitor_loop(),
                self.telegram_loop(),
                self.save_loop()
            ]
            
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                pass
    
    async def monitor_loop(self):
        symbols = await self.fetcher.get_symbols()
        
        while self.running:
            try:
                self.checks += 1
                now = time.time()
                
                prices, sources = await self.fetcher.get_prices(symbols)
                
                for symbol, price in prices.items():
                    self.analyzer.add_price(symbol, price, now)
                    
                    result = self.analyzer.analyze_symbol(
                        symbol, price, now, self.window,
                        self.percent, self.cooldown,
                        self.state.state.last_alert
                    )
                    
                    if result:
                        result["source"] = sources.get(symbol, "UNKNOWN")
                        await self.send_signal(result)
                        self.state.record_signal(symbol, result["growth"])
                
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(5)
    
    async def send_signal(self, signal):
        symbol = signal["symbol"]
        growth = signal["growth"]
        emoji = "🚀" if growth > 0 else "📉"
        action = "Рост" if growth > 0 else "Падение"
        
        text = (
            f"{emoji} <b>СИГНАЛ</b>\n\n"
            f"Монета: <b>{symbol}</b>\n"
            f"Цена: {signal['price']:.4f} ({signal['source']})\n"
            f"{action}: <b>{growth:+.2f}%</b>"
        )
        if signal.get("rsi"):
            text += f"\n📊 RSI: {signal['rsi']:.2f}"
        
        await self.telegram.send_message(text)
        logger.info(f"Сигнал: {symbol} {growth:+.2f}%")
    
    async def telegram_loop(self):
        while self.running:
            try:
                updates = await self.telegram.get_updates()
                for update in updates:
                    if "message" in update:
                        await self.handle_message(update["message"])
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Telegram error: {e}")
    
    async def handle_message(self, msg):
        chat_id = msg["chat"]["id"]
        if chat_id != config.CHAT_ID:
            return
        
        text = msg.get("text", "")
        
        if text == "/start":
            await self.telegram.send_message(f"Бот запущен\nПорог: {self.percent}%\nПериод: {self.window//60} мин")
            await self.telegram.send_keyboard(chat_id)
        
        elif text == "/status":
            await self.telegram.send_message(f"Порог: {self.percent}%\nПериод: {self.window//60} мин")
        
        elif text == "📊 Статистика":
            uptime = int(time.time() - self.state.state.start_time)
            await self.telegram.send_message(
                f"Сигналов: {self.state.state.signals_count}\n"
                f"Проверок: {self.checks}\n"
                f"Время: {uptime//3600}ч {(uptime%3600)//60}м"
            )
        
        elif text.startswith("📈 "):
            try:
                self.percent = float(text.replace("📈 ", "").replace("%", ""))
                await self.telegram.send_message(f"✅ {self.percent}%")
            except:
                pass
        
        elif text.startswith("⏱ "):
            try:
                if "мин" in text:
                    minutes = int(text.replace("⏱ ", "").replace(" мин", ""))
                    self.window = minutes * 60
                    await self.telegram.send_message(f"✅ {minutes} мин")
            except:
                pass
    
    async def save_loop(self):
        while self.running:
            self.state.save()
            await asyncio.sleep(30)

async def main():
    bot = CryptoBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Stopped")

if __name__ == "__main__":
    asyncio.run(main())
