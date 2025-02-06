"""
This file makes the config directory a Python package.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_float_env(key: str, default: float) -> float:
    """Safely get float value from environment variable"""
    try:
        value = os.getenv(key)
        if value is None:
            return default
        # Remove any whitespace and convert to float
        return float(value.strip())
    except (ValueError, AttributeError) as e:
        print(f"Warning: Invalid value for {key}, using default {default}. Error: {e}")
        return default

def get_int_env(key: str, default: int) -> int:
    """Safely get integer value from environment variable"""
    try:
        value = os.getenv(key)
        if value is None:
            return default
        # Remove any whitespace and convert to int
        return int(value.strip())
    except (ValueError, AttributeError) as e:
        print(f"Warning: Invalid value for {key}, using default {default}. Error: {e}")
        return default

# Bot settings
ARBITRAGE_THRESHOLD = float(os.getenv("ARBITRAGE_THRESHOLD", "10"))  # Default 10%
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # Default 50 tokens per batch
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "60"))  # Default 60 seconds

# Liquidity thresholds
MIN_CEX_24H_VOLUME = float(os.getenv("MIN_CEX_24H_VOLUME", "1000000"))  # Default $1M
MIN_DEX_LIQUIDITY = float(os.getenv("MIN_DEX_LIQUIDITY", "500000"))  # Default $500K

# List of tokens to monitor (use token symbols as used by the exchanges)
WATCHLIST = [
    'ALPHAOFSOL'  # Example token
]

# Exchange API Configurations
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET')

KUCOIN_API_KEY = os.getenv('KUCOIN_API_KEY')
KUCOIN_API_SECRET = os.getenv('KUCOIN_API_SECRET')
KUCOIN_API_PASSPHRASE = os.getenv('KUCOIN_API_PASSPHRASE')

GATEIO_API_KEY = os.getenv('GATEIO_API_KEY')
GATEIO_API_SECRET = os.getenv('GATEIO_API_SECRET')

BITGET_API_KEY = os.getenv('BITGET_API_KEY')
BITGET_API_SECRET = os.getenv('BITGET_API_SECRET')
BITGET_API_PASSPHRASE = os.getenv('BITGET_API_PASSPHRASE')

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')

MEXC_API_KEY = os.getenv('MEXC_API_KEY')
MEXC_API_SECRET = os.getenv('MEXC_API_SECRET')

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Retry Settings
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # Seconds between retries 