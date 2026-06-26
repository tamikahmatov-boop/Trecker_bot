import asyncio
import logging
import time
from config import config
from state_manager import StateManager
from telegram_client import TelegramClient
from price_fetcher import PriceFetcher
from analyzer import Analyzer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class CryptoBot:
    def __init__(self):
        self.config = config
        self.state = StateManager()
        self.analyzer = Analyzer()
        self.telegram = None
        self.fetcher = None
        self.running = False
        self.checks = 0
        self.symbols = set()
        self.percent = config.PERCENT
        self.window = config.WINDOW
        self.interval = config.INTERVAL
        self.cooldown = config.COOLDOWN
        
        try:
            config.validate()
        except ValueError as e:
            logger.error(f"Ошибка конфигурации: {e}")
            exit(1)
    
    async def start(self):
        logger.info("🚀 Запуск...")
        
        self.telegram = TelegramClient(config.BOT_TOKEN, config.CHAT_ID)
        self.fetcher = PriceFetcher()
        
        async with self.telegram, self.fetcher:
            await self.telegram.send_message(
                f"✅ Бот запущен!\n\n"
                f"📈 Порог: {self.percent}%\n"
                f"⏱ Период: {self.window // 60} мин"
            )
            
            # 🔥 ОТПРАВЛЯЕМ ТЕСТОВЫЕ КНОПКИ
            await self.telegram.send_test_buttons(config.CHAT_ID)
            
            await self.telegram.send_main_menu(config.CHAT_ID)
            
            self.running = True
            self.state.state.start_time = time.time()
            
            tasks = [
                self.monitor_loop(),
                self.telegram_loop()
            ]
            
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                logger.info("Остановка")
            finally:
                await self.stop()
    
    async def stop(self):
        self.running = False
        self.state.save()
        logger.info("🛑 Бот остановлен")
    
    async def monitor_loop(self):
        while self.running:
            try:
                self.checks += 1
                now = time.time()
                
                if not self.symbols or self.checks % 360 == 0:
                    self.symbols = await self.fetcher.get_symbols()
                    logger.info(f"📊 Символов: {len(self.symbols)}")
                
                prices, sources = await self.fetcher.get_prices(self.symbols)
                
                if not prices:
                    await asyncio.sleep(self.interval)
                    continue
                
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
                        self.state.save()
                
                await asyncio.sleep(self.interval)
                
            except Exception as e:
                logger.error(f"❌ Ошибка: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def send_signal(self, signal):
        """Отправка сигнала с кнопками"""
        symbol = signal["symbol"]
        growth = signal["growth"]
        emoji = "🚀" if growth > 0 else "📉"
        action = "Рост" if growth > 0 else "Падение"
        
        text = (
            f"{emoji} <b>СИГНАЛ</b>\n\n"
            f"Монета: <b>{symbol}</b>\n"
            f"Цена: <b>{signal['price']:.4f}</b>\n"
            f"{action}: <b>{growth:+.2f}%</b>"
        )
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔕 Игнорировать", "callback_data": f"ignore_{symbol}"}]
            ]
        }
        
        await self.telegram.send_message(text, reply_markup=keyboard)
        logger.info(f"📨 Сигнал: {symbol} {growth:+.2f}%")
    
    async def telegram_loop(self):
        """Обработка команд"""
        while self.running:
            try:
                updates = await self.telegram.get_updates()
                
                for update in updates:
                    if "message" in update:
                        await self.handle_message(update["message"])
                    elif "callback_query" in update:
                        callback = update["callback_query"]
                        logger.info(f"🔘 Callback получен: {callback.get('data')}")
                        await self.handle_callback(callback)
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Ошибка: {e}")
                await asyncio.sleep(1)
    
    async def handle_callback(self, callback):
        """ОБРАБОТКА КНОПОК"""
        callback_id = callback["id"]
        data = callback.get("data", "")
        message = callback.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        
        logger.info(f"🔘 Обработка: {data}")
        
        if chat_id != config.CHAT_ID:
            await self.telegram.answer_callback(callback_id, "⛔ Доступ запрещен", True)
            return
        
        # ТЕСТОВЫЕ КНОПКИ
        if data == "btn1":
            await self.telegram.answer_callback(callback_id, "✅ Нажата кнопка 1!", False)
            await self.telegram.edit_message(chat_id, message_id, "✅ Вы нажали КНОПКУ 1!")
            return
        
        if data == "btn2":
            await self.telegram.answer_callback(callback_id, "✅ Нажата кнопка 2!", False)
            await self.telegram.edit_message(chat_id, message_id, "✅ Вы нажали КНОПКУ 2!")
            return
        
        if data == "btn3":
            await self.telegram.answer_callback(callback_id, "✅ Нажата кнопка 3!", False)
            await self.telegram.edit_message(chat_id, message_id, "✅ Вы нажали КНОПКУ 3!")
            return
        
        # ИГНОРИРОВАНИЕ
        if data.startswith("ignore_"):
            symbol = data.replace("ignore_", "")
            
            await self.telegram.answer_callback(
                callback_id, 
                f"🔕 {symbol} игнорируется",
                False
            )
            
            await self.telegram.send_message(
                f"🔕 <b>{symbol}</b> добавлен в игнор-лист",
                chat_id
            )
            
            logger.info(f"✅ Игнорируем: {symbol}")
            return
        
        await self.telegram.answer_callback(callback_id, "❌ Неизвестно", True)
    
    async def handle_message(self, msg):
        chat_id = msg["chat"]["id"]
        
        if chat_id != config.CHAT_ID:
            return
        
        text = msg.get("text", "")
        logger.info(f"📩 Сообщение: {text}")
        
        if text == "/start":
            await self.telegram.send_main_menu(chat_id)
            await self.telegram.send_test_buttons(chat_id)
        
        elif text == "/test":
            await self.telegram.send_test_buttons(chat_id)
        
        elif text == "📊 Статистика":
            await self.telegram.send_message(
                f"📊 Статистика\n\n"
                f"Сигналов: {self.state.state.signals_count}\n"
                f"Проверок: {self.checks}"
            )
        
        elif text == "/status":
            await self.telegram.send_message(
                f"📊 Статус\n\n"
                f"Порог: {self.percent}%\n"
                f"Период: {self.window // 60} мин"
            )
        
        else:
            await self.telegram.send_message("❓ Неизвестная команда")

async def main():
    bot = CryptoBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Остановка")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
