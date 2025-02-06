import requests
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from utils.logger import logger

class TelegramNotifier:
    def __init__(self, token=TELEGRAM_TOKEN, chat_id=TELEGRAM_CHAT_ID):
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def send_message(self, text, parse_mode="Markdown"):
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": False,
        }
        try:
            response = requests.post(self.api_url, data=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Telegram message sent successfully.")
            else:
                logger.error(f"Telegram error: {response.text}")
        except Exception as e:
            logger.error(f"Exception in send_message: {e}")