import time
from config import WATCHLIST, ARBITRAGE_THRESHOLD
from dex.dexscreener import DexScreener
from cex.mexc import MEXC
from notifier.telegram_notifier import TelegramNotifier
from utils.logger import logger
from utils.liquidity_analyzer import LiquidityAnalyzer

class ArbitrageEngine:
    def __init__(self):
        self.dex = DexScreener()
        self.cex = MEXC()  # Example: using MEXC; swap with other CEX modules as desired.
        self.notifier = TelegramNotifier()
        self.liquidity_analyzer = LiquidityAnalyzer()

    async def check_arbitrage(self, token_symbol):
        # First check if token has sufficient liquidity
        liquidity_analysis = await self.liquidity_analyzer.analyze_token_liquidity(token_symbol)
        if not liquidity_analysis["has_sufficient_liquidity"]:
            logger.info(f"Skipping {token_symbol} due to insufficient liquidity. " +
                     f"CEX Volume: ${liquidity_analysis['total_cex_volume']:,.2f}, " +
                     f"DEX Liquidity: ${liquidity_analysis['total_dex_liquidity']:,.2f}")
            return

        # Fetch DEX data (price, contract, network, URL)
        dex_data = self.dex.get_token_data(token_symbol)
        if not dex_data:
            logger.error(f"Could not retrieve DEX data for {token_symbol}")
            return

        dex_price = dex_data["price"]

        # For the CEX side, here we use the futures price from MEXC.
        cex_price = self.cex.get_futures_price(token_symbol)
        if not cex_price:
            logger.error(f"Could not retrieve CEX data for {token_symbol}")
            return

        spread = abs(cex_price - dex_price) / dex_price
        logger.info(f"Arbitrage spread for {token_symbol}: {spread*100:.2f}%")

        if spread >= ARBITRAGE_THRESHOLD:
            # Get deposit/withdraw info from the CEX (MEXC)
            dw_info = self.cex.get_deposit_withdraw_info(token_symbol)
            # Build clickable links:
            # For MEXC: assume coin-detail page
            mexc_link = f"https://www.mexc.com/exchange/coin-detail/{token_symbol}"
            # For Dexscreener: use the URL from DexScreener API data
            dex_link = dex_data["dex_url"]

            # Build the Telegram message (using Markdown formatting)
            message = (
                f"*Монета:* {token_symbol}\n\n"
                f"*Спред:* {spread*100:.2f}% между [DEX]({dex_link}) и [MEXC]({mexc_link})\n\n"
                f"*Цена DEX:* {dex_price}\n"
                f"*Цена MEXC (futures):* {cex_price}\n\n"
                f"*Ликвидность:*\n"
                f"CEX Volume 24h: ${liquidity_analysis['total_cex_volume']:,.2f}\n"
                f"DEX Liquidity: ${liquidity_analysis['total_dex_liquidity']:,.2f}\n\n"
                f"*Max Volume:* {dw_info.get('max_volume')}\n"
                f"*Deposit:* {dw_info.get('deposit')}\n"
                f"*Withdraw:* {dw_info.get('withdraw')}\n\n"
                f"*Контракт:* [ {dex_data.get('contract')}](tg://copy?text={dex_data.get('contract')})\n"
                f"*Сеть:* {dex_data.get('network')}\n"
            )
            self.notifier.send_message(message)

    async def run(self):
        logger.info("Starting Arbitrage Engine...")
        while True:
            # Get list of high liquidity tokens
            high_liquidity_tokens = await self.liquidity_analyzer.get_high_liquidity_tokens(WATCHLIST)
            logger.info(f"Found {len(high_liquidity_tokens)} tokens with sufficient liquidity")
            
            for token_data in high_liquidity_tokens:
                await self.check_arbitrage(token_data["symbol"])
            
            # Adjust sleep interval based on rate limits and desired frequency.
            time.sleep(10)