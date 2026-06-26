import asyncio
import logging
import time
import os
from config import config
from state_manager import StateManager
from telegram_client import TelegramClient
from price_fetcher import PriceFetcher
from analyzer import Analyzer

logging.basicConfig(
    level=logging.DEBUG,  # Включаем DEBUG для отладки
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        self.symbols = set()
        self.ignored_symbols = set()
        self.telegram = None
        self.fetcher = None
        
        try:
            config.validate()
        except ValueError as e:
            logger.error(f"Ошибка конфигурации: {e}")
            raise
    
    async def start(self):
        logger.info("🚀 Запуск бота...")
        
        self.telegram = TelegramClient(config.BOT_TOKEN, config.CHAT_ID)
        self.fetcher = PriceFetcher()
        
        async with self.telegram, self.fetcher:
            await self.telegram.send_main_menu(config.CHAT_ID)
            
            await self.telegram.send_message(
                f"✅ <b>Бот запущен!</b>\n\n"
                f"📈 Порог: <b>{self.percent}%</b>\n"
                f"⏱ Период: <b>{self.window // 60} мин</b>\n"
                f"⚡ Интервал: <b>{self.interval} сек</b>\n\n"
                f"Используйте кнопки для настройки"
            )
            
            self.running = True
            self.state.state.start_time = time.time()
            
            tasks = [
                self.monitor_loop(),
                self.telegram_loop(),
                self.save_loop(),
                self.heartbeat_loop()
            ]
            
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                logger.info("Задачи отменены")
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
                    logger.info(f"Символов: {len(self.symbols)}")
                
                prices, sources = await self.fetcher.get_prices(self.symbols)
                
                if not prices:
                    await asyncio.sleep(self.interval)
                    continue
                
                for symbol, price in prices.items():
                    if symbol in self.ignored_symbols:
                        continue
                    
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
                logger.error(f"Ошибка в monitor_loop: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def send_signal(self, signal):
        symbol = signal["symbol"]
        growth = signal["growth"]
        emoji = "🚀" if growth > 0 else "📉"
        action = "Рост" if growth > 0 else "Падение"
        
        text = (
            f"{emoji} <b>СИГНАЛ</b>\n\n"
            f"Монета: <b>{symbol}</b>\n"
            f"Цена: <b>{signal['price']:.4f}</b> ({signal['source']})\n"
            f"{action}: <b>{growth:+.2f}%</b>"
        )
        if signal.get("rsi") is not None:
            text += f"\n📊 RSI: <b>{signal['rsi']:.2f}</b>"
        else:
            text += "\n📊 RSI: ожидание данных"
        
        text += "\n\n🔄 Нажмите кнопку ниже чтобы игнорировать монету"
        
        inline_keyboard = {
            "inline_keyboard": [
                [
                    {"text": "🔕 Игнорировать", "callback_data": f"ignore_{symbol}"},
                    {"text": "ℹ️ Инфо", "callback_data": f"info_{symbol}"}
                ]
            ]
        }
        
        await self.telegram.send_message(text, reply_markup=inline_keyboard)
        logger.info(f"Сигнал: {symbol} {growth:+.2f}%")
    
    async def telegram_loop(self):
        while self.running:
            try:
                updates = await self.telegram.get_updates()
                for update in updates:
                    if "message" in update:
                        await self.handle_message(update["message"])
                    elif "callback_query" in update:
                        logger.info(f"Получен callback: {update['callback_query'].get('data')}")
                        await self.handle_callback(update["callback_query"])
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Ошибка в telegram_loop: {e}")
                await asyncio.sleep(1)
    
    async def handle_callback(self, callback):
        callback_id = callback["id"]
        data = callback.get("data", "")
        message = callback.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        
        logger.info(f"Callback: {data} от {chat_id}")
        
        if chat_id != config.CHAT_ID:
            await self.telegram.answer_callback(callback_id, "⛔ Доступ запрещен", True)
            return
        
        if data.startswith("ignore_"):
            symbol = data.replace("ignore_", "")
            self.ignored_symbols.add(symbol)
            
            await self.telegram.answer_callback(
                callback_id, 
                f"🔕 {symbol} добавлен в игнор-лист",
                False
            )
            
            await self.telegram.send_message(
                f"🔕 <b>{symbol}</b> добавлен в игнор-лист\n"
                f"Сигналы по этой монете больше не будут приходить",
                chat_id
            )
            
            # Удаляем кнопки
            await self.telegram.edit_message_reply_markup(chat_id, message_id, None)
            logger.info(f"Игнорируем: {symbol}")
        
        elif data.startswith("info_"):
            symbol = data.replace("info_", "")
            await self.telegram.answer_callback(callback_id, f"ℹ️ Информация о {symbol}", False)
            
            if symbol in self.analyzer.price_history:
                prices = [p for _, p in self.analyzer.price_history[symbol][-10:]]
                if prices:
                    avg_price = sum(prices) / len(prices)
                    last_price = prices[-1]
                    await self.telegram.send_message(
                        f"ℹ️ <b>Информация о {symbol}</b>\n\n"
                        f"Последняя цена: {last_price:.4f}\n"
                        f"Средняя (10): {avg_price:.4f}\n"
                        f"Изменение: {((last_price - avg_price) / avg_price * 100):+.2f}%",
                        chat_id
                    )
    
    async def handle_message(self, msg):
        chat_id = msg["chat"]["id"]
        
        if chat_id != config.CHAT_ID:
            logger.warning(f"Неавторизованный доступ от {chat_id}")
            await self.telegram.send_message("⛔ Доступ запрещен", chat_id)
            return
        
        text = msg.get("text", "")
        logger.info(f"Получено сообщение: {text}")
        
        if text == "/start":
            await self.telegram.send_main_menu(chat_id)
        
        elif text == "🏠 Главное меню" or text == "🔙 Назад":
            await self.telegram.send_main_menu(chat_id)
        
        elif text == "📈 Настройки порога":
            await self.telegram.send_percent_menu(chat_id)
        
        elif text == "⏱ Настройки периода":
            await self.telegram.send_window_menu(chat_id)
        
        elif text.startswith("📈 ") and text != "📈 Настройки порога":
            try:
                percent_str = text.replace("📈 ", "").replace("%", "")
                if percent_str == "Пользовательский":
                    await self.telegram.send_message(
                        "Введите желаемый порог в процентах (например: 7.5):",
                        chat_id
                    )
                    return
                
                percent = float(percent_str)
                self.percent = percent
                await self.telegram.send_message(
                    f"✅ Установлен порог: <b>{percent}%</b>",
                    chat_id,
                    {"remove_keyboard": True}
                )
                await asyncio.sleep(1)
                await self.telegram.send_main_menu(chat_id)
                logger.info(f"Порог изменен на {percent}%")
            except Exception as e:
                await self.telegram.send_message(f"❌ Ошибка: {e}", chat_id)
        
        elif text.startswith("⏱ ") and text != "⏱ Настройки периода":
            try:
                time_str = text.replace("⏱ ", "")
                minutes = 0
                
                if "минута" in time_str or "минут" in time_str:
                    minutes = int(''.join(filter(str.isdigit, time_str)))
                elif "час" in time_str:
                    hours = int(''.join(filter(str.isdigit, time_str)))
                    minutes = hours * 60
                elif "день" in time_str:
                    days = int(''.join(filter(str.isdigit, time_str)))
                    minutes = days * 24 * 60
                else:
                    await self.telegram.send_message("❌ Неверный формат", chat_id)
                    return
                
                self.window = minutes * 60
                await self.telegram.send_message(
                    f"✅ Установлен период: <b>{minutes} мин</b>",
                    chat_id,
                    {"remove_keyboard": True}
                )
                await asyncio.sleep(1)
                await self.telegram.send_main_menu(chat_id)
                logger.info(f"Период изменен на {minutes} мин")
            except Exception as e:
                await self.telegram.send_message(f"❌ Ошибка: {e}", chat_id)
        
        elif text == "📊 Статистика":
            uptime = int(time.time() - self.state.state.start_time)
            days = uptime // 86400
            hours = (uptime % 86400) // 3600
            minutes = (uptime % 3600) // 60
            
            await self.telegram.send_message(
                f"📊 <b>СТАТИСТИКА</b>\n\n"
                f"🟢 Время работы: <b>{days}д {hours}ч {minutes}м</b>\n"
                f"🪙 Монет в истории: <b>{len(self.analyzer.price_history)}</b>\n"
                f"🔔 Сигналов: <b>{self.state.state.signals_count}</b>\n"
                f"🔄 Проверок: <b>{self.checks}</b>\n"
                f"📈 Текущий порог: <b>{self.percent}%</b>\n"
                f"⏱ Текущий период: <b>{self.window // 60} мин</b>\n"
                f"🔕 Игнорируется: <b>{len(self.ignored_symbols)}</b> монет\n"
                f"📌 Активных алертов: <b>{len(self.state.state.last_alert)}</b>",
                chat_id
            )
        
        elif text == "📋 Список монет":
            if not self.symbols:
                await self.telegram.send_message("🔄 Загрузка списка монет...", chat_id)
                self.symbols = await self.fetcher.get_symbols()
            
            symbols_list = sorted(list(self.symbols))[:50]
            text_msg = f"📋 <b>Список монет ({len(self.symbols)} всего)</b>\n\n"
            text_msg += "Первые 50 монет:\n"
            for i, sym in enumerate(symbols_list, 1):
                ignored = "🔕" if sym in self.ignored_symbols else "✅"
                text_msg += f"{i}. {ignored} {sym}\n"
            
            if len(self.symbols) > 50:
                text_msg += f"\n... и еще {len(self.symbols) - 50} монет"
            
            await self.telegram.send_message(text_msg, chat_id)
        
        elif text == "🔄 Обновить список":
            await self.telegram.send_message("🔄 Обновление списка монет...", chat_id)
            self.symbols = await self.fetcher.get_symbols()
            await self.telegram.send_message(
                f"✅ Список обновлен! <b>{len(self.symbols)}</b> монет",
                chat_id
            )
        
        elif text == "❌ Очистить историю":
            self.analyzer.price_history.clear()
            self.state.state.last_alert.clear()
            await self.telegram.send_message(
                "✅ История очищена! Все алерты сброшены.",
                chat_id
            )
            logger.info("История очищена")
        
        elif text == "/status":
            await self.telegram.send_message(
                f"📊 <b>Текущие настройки</b>\n\n"
                f"📈 Порог: <b>{self.percent}%</b>\n"
                f"⏱ Период: <b>{self.window} сек ({self.window // 60} мин)</b>\n"
                f"⚡ Интервал: <b>{self.interval} сек</b>\n"
                f"🕒 Кулдаун: <b>{self.cooldown // 60} мин</b>\n"
                f"🪙 Монет в истории: <b>{len(self.analyzer.price_history)}</b>",
                chat_id
            )
        
        elif text == "/help":
            await self.telegram.send_message(
                "🤖 <b>Помощь</b>\n\n"
                "📈 Настройки порога - изменить процент изменения цены\n"
                "⏱ Настройки периода - изменить период анализа\n"
                "📊 Статистика - показать статистику работы\n"
                "📋 Список монет - показать отслеживаемые монеты\n"
                "🔄 Обновить список - обновить список монет\n"
                "❌ Очистить историю - очистить историю цен\n"
                "🔕 Игнорировать - нажать под сигналом чтобы игнорировать монету\n\n"
                "Сигналы приходят при достижении выбранного процента изменения цены"
            )
        
        else:
            try:
                percent = float(text.replace("%", "").strip())
                if 0 < percent <= 1000:
                    self.percent = percent
                    await self.telegram.send_message(
                        f"✅ Установлен порог: <b>{percent}%</b>",
                        chat_id,
                        {"remove_keyboard": True}
                    )
                    await asyncio.sleep(1)
                    await self.telegram.send_main_menu(chat_id)
                    logger.info(f"Порог изменен на {percent}%")
                else:
                    await self.telegram.send_message(
                        "❌ Пожалуйста, введите число от 0 до 1000",
                        chat_id
                    )
            except ValueError:
                await self.telegram.send_message(
                    "❓ Неизвестная команда. Используйте кнопки меню.",
                    chat_id
                )
    
    async def save_loop(self):
        while self.running:
            try:
                self.state.save()
            except Exception as e:
                logger.error(f"Ошибка сохранения: {e}")
            await asyncio.sleep(30)
    
    async def heartbeat_loop(self):
        while self.running:
            await asyncio.sleep(300)
            logger.info(f"❤️ Бот работает | Порог: {self.percent}% | Проверок: {self.checks}")

async def main():
    bot = CryptoBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
