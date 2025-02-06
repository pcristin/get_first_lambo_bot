import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Replace these with your actual Telegram bot credentials.
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_TOKEN'
TELEGRAM_CHAT_ID = 'YOUR_TELEGRAM_CHAT_ID'

# Arbitrage threshold (example: 30% spread)
ARBITRAGE_THRESHOLD = float(os.getenv('ARBITRAGE_THRESHOLD', '0.02'))

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

GATEIO_API_KEY = 'YOUR_GATEIO_API_KEY'
GATEIO_API_SECRET = 'YOUR_GATEIO_API_SECRET'

BITGET_API_KEY = 'YOUR_BITGET_API_KEY'
BITGET_API_SECRET = 'YOUR_BITGET_API_SECRET'
BITGET_API_PASSPHRASE = 'YOUR_BITGET_API_PASSPHRASE'

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')

MEXC_API_KEY = os.getenv('MEXC_API_KEY')
MEXC_API_SECRET = os.getenv('MEXC_API_SECRET')

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Liquidity Thresholds
MIN_CEX_24H_VOLUME = float(os.getenv('MIN_CEX_24H_VOLUME', '1000000'))
MIN_DEX_LIQUIDITY = float(os.getenv('MIN_DEX_LIQUIDITY', '500000'))

# Rate Limiting and Performance Settings
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))  # Number of tokens to process in parallel
UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', '30'))  # Seconds between full update cycles

# Retry Settings
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # Seconds between retries