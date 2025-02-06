import aiohttp
import json
from utils.logger import logger
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

class TelegramNotifier:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.session = None
        
        # Validate configuration
        if not self.bot_token or not self.chat_id:
            logger.error("❌ Telegram configuration missing! Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
            raise ValueError("Telegram configuration missing")
            
        logger.info("✅ TelegramNotifier initialized")

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def send_message(self, message: str) -> bool:
        """Send a message to the Telegram chat"""
        try:
            await self._ensure_session()
            
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "MarkdownV2",
                "disable_web_page_preview": True
            }
            
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get("ok"):
                        return True
                    else:
                        error = result.get("description", "Unknown error")
                        logger.error(f"❌ Telegram API error: {error}")
                else:
                    logger.error(f"❌ Telegram HTTP error {response.status}")
                
                return False
                
        except aiohttp.ClientError as e:
            logger.error(f"❌ Telegram connection error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Error sending Telegram message: {str(e)}")
            return False

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            try:
                await self.session.close()
                self.session = None
            except Exception as e:
                logger.error(f"❌ Error closing Telegram session: {str(e)}")