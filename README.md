# Crypto Arbitrage Bot

An advanced cryptocurrency arbitrage bot that monitors price differences between DEX and CEX platforms, with built-in liquidity analysis and comprehensive rate limiting to ensure executable trades.

## Features

### Multi-Exchange Support
- **CEX Support:**
  - Binance
  - KuCoin
  - Bybit
  - OKX
  - MEXC
  - Gate.io
  - BitGet
- **DEX Support:**
  - All DEXes via DexScreener API

> **Note:** The bot will automatically skip exchanges where API credentials are not provided or empty. At least one exchange must be configured for the bot to operate.
  
### Smart Liquidity Analysis
- Real-time monitoring of 24h trading volume on CEXes
- DEX liquidity pool tracking
- Configurable minimum liquidity thresholds
- Automatic filtering of low-liquidity pairs
- Multi-exchange volume aggregation

### Advanced Rate Limiting
- Exchange-specific rate limits:
  - Per-endpoint limits (market/private)
  - IP-based rate limits
  - Request weight handling
- Automatic request throttling
- Smart batch processing
- Fallback limits for safety

### Real-time Arbitrage Detection
- Continuous monitoring of price differences
- Configurable arbitrage spread threshold
- Detailed spread calculations
- Best opportunity selection across all exchanges
- Parallel processing of tokens

### Comprehensive Notifications
- Telegram notifications for arbitrage opportunities
- Includes:
  - Price spread details
  - Liquidity information across all exchanges
  - Token contract addresses
  - Exchange-specific links
  - Deposit/withdrawal status
  - Network information

## Prerequisites

- Python 3.7+
- Telegram Bot Token (for notifications)
- API keys for at least one supported exchange

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd get_first_lambo_bot
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Configure your settings:
   - Copy `.env.example` to `.env`
   - Add your API keys for the exchanges you want to use
   - Leave API keys empty or as empty strings ("") for exchanges you want to skip
   - Adjust thresholds if needed

## Configuration

### Environment Variables (.env)
```env
# Exchange API Keys (leave empty to skip exchange)
BINANCE_API_KEY=your_binance_api_key    # Optional
BINANCE_API_SECRET=your_binance_secret  # Optional

MEXC_API_KEY=your_mexc_api_key         # Optional
MEXC_API_SECRET=your_mexc_secret       # Optional

# ... other exchange keys (all optional)

# Required Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token  # Required
TELEGRAM_CHAT_ID=your_telegram_chat_id      # Required

# Optional Settings (with defaults)
ARBITRAGE_THRESHOLD=0.02  # 2% spread
MIN_CEX_24H_VOLUME=1000000  # $1M daily volume
MIN_DEX_LIQUIDITY=500000    # $500K liquidity
BATCH_SIZE=10              # Number of tokens to process in parallel
UPDATE_INTERVAL=30         # Seconds between full update cycles
MAX_RETRIES=3             # Maximum retry attempts
RETRY_DELAY=5             # Seconds between retries
```

### Exchange Configuration
The bot supports multiple exchanges, but not all need to be configured. Here's how it works:

1. **Required Exchanges:**
   - At least one exchange must be configured with valid API credentials
   - The bot will raise an error if no exchanges are configured

2. **Optional Exchanges:**
   - Leave API keys empty or as empty strings ("") to skip an exchange
   - The bot will automatically detect and skip unconfigured exchanges
   - You'll see a warning message for each skipped exchange

3. **Dynamic Operation:**
   - The bot automatically adjusts to use only configured exchanges
   - Common tokens are found across only active exchanges
   - Rate limits are applied only to active exchanges

### Rate Limits
The bot respects the following rate limits for configured exchanges:

#### Exchange-Specific Limits
- MEXC: 20 req/sec (market), 60 req/min (private)
- Bybit: 50 req/sec (market), 600 req/min (private)
- OKX: 20 req/2sec (market), 300 req/min (private)
- KuCoin: 30 req/sec (market), 180 req/min (private)
- Gate.io: 300 req/min (market), 180 req/min (private)
- BitGet: 20 req/sec (market), 300 req/min (private)
- Binance: 1200 req/min (market), 60 req/min (private)
- DexScreener: 30 req/min

#### IP-Based Limits
- Binance: 2400 req/min
- OKX: 500 req/min
- Bybit: 1200 req/min
- KuCoin: 1800 req/min
- Gate.io: 900 req/min
- MEXC: 1800 req/min
- DexScreener: 60 req/min

## Usage

Run the bot:
```bash
python main.py
```

The bot will:
1. Start monitoring all available tokens across exchanges
2. Apply rate limiting and batch processing
3. Check liquidity levels
4. Calculate price differences
5. Send notifications for profitable opportunities

### Token Discovery Process
1. Fetches all available futures trading pairs from each CEX
2. Finds tokens common to all exchanges
3. Verifies DEX availability
4. Filters by liquidity thresholds
5. Monitors in configurable batch sizes

## Performance Optimization

The bot includes several optimizations:
- Async/await for efficient API calls
- Parallel processing of token batches
- Smart rate limiting with request queuing
- Session reuse for better performance
- Automatic cleanup of expired rate limit records

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This bot is for informational purposes only. Use at your own risk. Always verify opportunities and conduct your own research before trading. The bot implements rate limiting according to exchange specifications, but it's your responsibility to ensure compliance with exchange terms of service. 