# Crypto Arbitrage Bot

A high-performance crypto arbitrage bot that monitors price differences across multiple CEX (Centralized Exchanges) and DEX (Decentralized Exchanges) platforms.

## Features

- **Multi-Exchange Support**:
  - CEX Support: Binance, KuCoin, Bybit, OKX, Gate.io, MEXC, BitGet
  - DEX Support: Jupiter (Solana)
  - DEX Price Discovery: DexScreener API

- **Arbitrage Types**:
  - CEX-to-CEX Arbitrage (Spot & Futures)
  - DEX-to-CEX Arbitrage (Solana tokens)
  - Cross-market Arbitrage (Spot vs Futures)

- **Advanced Features**:
  - Real-time Price Monitoring
  - Parallel Processing (50 tokens per batch)
  - Automatic Liquidity Analysis
  - Smart Rate Limiting
  - Telegram Notifications
  - Deposit/Withdrawal Status Checking

- **Performance Optimizations**:
  - Asynchronous API Calls
  - Connection Pooling
  - Smart Caching
  - Efficient Batch Processing

## Requirements

- Python 3.8+
- aiohttp
- asyncio
- python-telegram-bot

## Installation

1. Clone the repository:
```bash
git clone <repository_url>
cd get_first_lambo_bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy the example environment file and fill in your API keys:
```bash
cp .env.example .env
```

## Configuration

Edit the `.env` file with your API credentials:

```env
# Exchange API Keys
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret

KUCOIN_API_KEY=your_kucoin_api_key
KUCOIN_API_SECRET=your_kucoin_api_secret
KUCOIN_API_PASSPHRASE=your_kucoin_passphrase

BYBIT_API_KEY=your_bybit_api_key
BYBIT_API_SECRET=your_bybit_api_secret

OKX_API_KEY=your_okx_api_key
OKX_API_SECRET=your_okx_api_secret
OKX_API_PASSPHRASE=your_okx_passphrase

GATEIO_API_KEY=your_gateio_api_key
GATEIO_API_SECRET=your_gateio_api_secret

MEXC_API_KEY=your_mexc_api_key
MEXC_API_SECRET=your_mexc_api_secret

BITGET_API_KEY=your_bitget_api_key
BITGET_API_SECRET=your_bitget_api_secret
BITGET_API_PASSPHRASE=your_bitget_passphrase

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

# Optional Settings
ARBITRAGE_THRESHOLD=0.01  # 1% minimum spread
MIN_CEX_24H_VOLUME=1000000  # Minimum 24h volume in USD
MIN_DEX_LIQUIDITY=500000  # Minimum DEX liquidity in USD
```

## Usage

Run the bot:
```bash
python main.py
```

The bot will:
1. Initialize connections to all configured exchanges
2. Start monitoring prices across all platforms
3. Process tokens in efficient batches
4. Send notifications when arbitrage opportunities are found

## Notifications

The bot sends detailed Telegram notifications for each arbitrage opportunity, including:
- Token symbol and current prices
- Spread percentage and absolute difference
- Trading volume and liquidity information
- Deposit/withdrawal status for each exchange
- Direct trading links
- Contract addresses (for DEX trades)

## Rate Limits

The bot implements smart rate limiting for each exchange:
- Market Data: Shared limit across all market data requests
- Private API: Separate limit for authenticated requests
- Automatic backoff on rate limit errors

## Error Handling

- Automatic retry on temporary failures
- Connection pool management
- Graceful shutdown on interruption
- Detailed error logging

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

Trading cryptocurrencies carries significant risk. This bot is for educational purposes only. Always test thoroughly with small amounts first. 