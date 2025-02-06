# Crypto Arbitrage Bot

An advanced cryptocurrency arbitrage bot that monitors price differences between DEX and CEX platforms, with built-in liquidity analysis to ensure executable trades.

## Features

- **Multi-Exchange Support**
  - CEX Support: Binance, KuCoin, Bybit, OKX, MEXC
  - DEX Support: via DexScreener API
  
- **Smart Liquidity Analysis**
  - Monitors 24h trading volume on CEXes
  - Tracks DEX liquidity pools
  - Configurable minimum liquidity thresholds
  - Automatic filtering of low-liquidity pairs

- **Real-time Arbitrage Detection**
  - Continuous monitoring of price differences
  - Configurable arbitrage spread threshold
  - Detailed spread calculations

- **Comprehensive Notifications**
  - Telegram notifications for arbitrage opportunities
  - Includes:
    - Price spread details
    - Liquidity information
    - Token contract addresses
    - Exchange links
    - Deposit/withdrawal status

## Prerequisites

- Python 3.7+
- Telegram Bot Token (for notifications)
- API keys for supported exchanges

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
   - Copy `config.example.py` to `config.py`
   - Add your API keys and settings
   - Adjust liquidity thresholds if needed

## Configuration

### Liquidity Thresholds
Default thresholds in `utils/liquidity_analyzer.py`:
- Minimum CEX 24h Volume: $1,000,000
- Minimum DEX Liquidity: $500,000

You can adjust these values based on your trading requirements.

### Exchange API Keys
Add your exchange API keys in `config.py`:
```python
BINANCE_API_KEY = "your-api-key"
BINANCE_API_SECRET = "your-api-secret"
# Add other exchange keys as needed
```

### Token Watchlist
Configure your token watchlist in `config.py`:
```python
WATCHLIST = ["BTC", "ETH", "SOL", ...]
```

### Arbitrage Settings
Set your desired arbitrage threshold in `config.py`:
```python
ARBITRAGE_THRESHOLD = 0.02  # 2% spread
```

## Usage

Run the bot:
```bash
python main.py
```

The bot will:
1. Start monitoring configured tokens
2. Check liquidity levels across exchanges
3. Calculate price differences
4. Send notifications when profitable opportunities are found

## Notifications

Telegram notifications include:
- Token symbol
- Price spread percentage
- DEX and CEX prices
- Liquidity information
- Maximum trading volume
- Deposit/withdrawal status
- Contract address and network
- Direct links to exchanges

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This bot is for informational purposes only. Use at your own risk. Always verify opportunities and conduct your own research before trading. 