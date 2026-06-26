import logging
import asyncio
from typing import Optional, Dict, List, Any
import aiohttp
import json

logger = logging.getLogger(__name__)

class TelegramClient:
    def __init__(self, token: str, chat_id: int):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request_time = 0
        self.min_interval = 0.3
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self.last_request_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self.last_request_time = asyncio.get_event_loop().time()
    
    async def send_message(self, text: str, chat_id: Optional[int] = None, 
                          reply_markup: Optional[Dict] = None) -> bool:
        if chat_id is None:
            chat_id = self.chat_id
        
        await self._rate_limit()
        
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self.session.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 200:
                    logger.debug(f"Сообщение отправлено в {chat_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка sendMessage: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка send_message: {e}")
            return False
    
    async def send_main_menu(self, chat_id: int):
        """Главное меню с кнопками"""
        keyboard = {
            "keyboard": [
                ["📈 Настройки порога", "⏱ Настройки периода"],
                ["📊 Статистика", "📋 Список монет"],
                ["🔄 Обновить список", "❌ Очистить историю"],
                ["/status", "/help"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "🏠 <b>Главное меню</b>\n\nВыберите действие:",
            chat_id,
            keyboard
        )
    
    async def send_percent_menu(self, chat_id: int):
        """Меню выбора порога"""
        keyboard = {
            "keyboard": [
                ["📈 0.2%", "📈 5%", "📈 10%"],
                ["📈 15%", "📈 20%", "📈 30%"],
                ["📈 50%", "📈 100%", "📈 Пользовательский"],
                ["🔙 Назад"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "📈 <b>Выберите порог изменения цены</b>\n\nСигнал будет отправлен при достижении выбранного процента:",
            chat_id,
            keyboard
        )
    
    async def send_window_menu(self, chat_id: int):
        """Меню выбора периода"""
        keyboard = {
            "keyboard": [
                ["⏱ 1 минута", "⏱ 5 минут", "⏱ 15 минут"],
                ["⏱ 30 минут", "⏱ 1 час", "⏱ 4 часа"],
                ["⏱ 12 часов", "⏱ 1 день", "⏱ 3 дня"],
                ["🔙 Назад"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "⏱ <b>Выберите период анализа</b>\n\nБот будет анализировать изменение цены за выбранный период:",
            chat_id,
            keyboard
        )
    
    async def send_signal_with_buttons(self, chat_id: int, symbol: str, price: float, 
                                       growth: float, source: str, rsi: Optional[float] = None):
        """Отправка сигнала с инлайн кнопками"""
        emoji = "🚀" if growth > 0 else "📉"
        action = "Рост" if growth > 0 else "Падение"
        
        text = (
            f"{emoji} <b>СИГНАЛ</b>\n\n"
            f"Монета: <b>{symbol}</b>\n"
            f"Цена: <b>{price:.4f}</b> ({source})\n"
            f"{action}: <b>{growth:+.2f}%</b>"
        )
        if rsi is not None:
            text += f"\n📊 RSI: <b>{rsi:.2f}</b>"
        
        text += "\n\n⬇️ Нажмите кнопку ниже:"
        
        # Инлайн кнопки (не обычные)
        inline_keyboard = {
            "inline_keyboard": [
                [
                    {"text": "🔕 Игнорировать", "callback_data": f"ignore_{symbol}"},
                    {"text": "ℹ️ Инфо", "callback_data": f"info_{symbol}"}
                ]
            ]
        }
        
        await self.send_message(text, chat_id, inline_keyboard)
    
    async def get_updates(self) -> List[Dict[str, Any]]:
        """Получение обновлений от Telegram"""
        await self._rate_limit()
        
        try:
            async with self.session.get(
                f"{self.base_url}/getUpdates",
                params={
                    "timeout": 30,
                    "offset": self.offset,
                    "allowed_updates": ["message", "callback_query"]
                },
                timeout=aiohttp.ClientTimeout(total=35)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        updates = data.get("result", [])
                        if updates:
                            self.offset = updates[-1]["update_id"] + 1
                            logger.info(f"📥 Получено {len(updates)} обновлений")
                            # Логируем типы обновлений
                            for update in updates:
                                if "callback_query" in update:
                                    logger.info(f"🔘 Callback: {update['callback_query'].get('data')}")
                        return updates
                    else:
                        logger.error(f"Telegram API error: {data}")
                        return []
                elif response.status == 409:
                    logger.warning("Conflict 409 - возможно дублирующийся бот")
                    await asyncio.sleep(5)
                    return []
                else:
                    logger.error(f"HTTP error: {response.status}")
                    return []
        except asyncio.TimeoutError:
            logger.warning("Timeout в get_updates")
            return []
        except Exception as e:
            logger.error(f"Ошибка get_updates: {e}")
            return []
    
    async def answer_callback(self, callback_id: str, text: str, show_alert: bool = False) -> bool:
        """Ответ на callback запрос - ОБЯЗАТЕЛЬНО для инлайн кнопок!"""
        await self._rate_limit()
        
        try:
            payload = {
                "callback_query_id": callback_id,
                "text": text,
                "show_alert": show_alert
            }
            
            async with self.session.post(
                f"{self.base_url}/answerCallbackQuery",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ Callback ответ отправлен: {text}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Ошибка answerCallbackQuery: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Ошибка answer_callback: {e}")
            return False
    
    async def edit_message_reply_markup(self, chat_id: int, message_id: int, 
                                       reply_markup: Optional[Dict] = None) -> bool:
        """Редактирование кнопок в сообщении"""
        await self._rate_limit()
        
        try:
            payload = {
                "chat_id": chat_id,
                "message_id": message_id
            }
            
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            
            async with self.session.post(
                f"{self.base_url}/editMessageReplyMarkup",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    logger.debug(f"Кнопки обновлены в сообщении {message_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка editMessageReplyMarkup: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка edit_message_reply_markup: {e}")
            return False
