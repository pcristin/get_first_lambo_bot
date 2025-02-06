import asyncio
from typing import List
from config import ARBITRAGE_THRESHOLD, BATCH_SIZE, UPDATE_INTERVAL
from dex.dexscreener import DexScreener
from cex.manager import CEXManager
from notifier.telegram_notifier import TelegramNotifier
from utils.logger import logger
from utils.liquidity_analyzer import LiquidityAnalyzer

class ArbitrageEngine:
    def __init__(self):
        self.dex = DexScreener()
        self.cex_manager = CEXManager()
        self.notifier = TelegramNotifier()
        self.liquidity_analyzer = LiquidityAnalyzer()
        self.known_tokens = set()

    async def process_token_batch(self, tokens: List[str]):
        """Process a batch of tokens in parallel"""
        tasks = [self.check_arbitrage(token) for token in tokens]
        await asyncio.gather(*tasks)

    async def get_available_tokens(self):
        """
        Fetch all available tokens that are listed on both CEX futures and DEX.
        Returns a list of token symbols.
        """
        try:
            # Get tokens available on all exchanges
            cex_tokens = await self.cex_manager.get_common_symbols()
            if not cex_tokens:
                logger.error("No common tokens found across exchanges")
                return []

            tokens = []
            for symbol in cex_tokens:
                # Check if token exists on DEX
                dex_data = await self.dex.get_token_data(symbol)
                if dex_data:
                    tokens.append(symbol)
                    if symbol not in self.known_tokens:
                        logger.info(f"Found new token {symbol} listed on both CEX futures and DEX")
                        self.known_tokens.add(symbol)

            # Log only if the number of tokens has changed
            if len(tokens) != len(self.known_tokens):
                logger.info(f"Total tokens available for arbitrage: {len(tokens)}")
                removed_tokens = self.known_tokens - set(tokens)
                if removed_tokens:
                    logger.info(f"Tokens no longer available: {removed_tokens}")
                self.known_tokens = set(tokens)

            return tokens
        except Exception as e:
            logger.error(f"Error in get_available_tokens: {e}")
            return []

    async def check_arbitrage(self, token_symbol):
        """Check arbitrage opportunities for a single token"""
        try:
            # First check if token has sufficient liquidity
            liquidity_analysis = await self.liquidity_analyzer.analyze_token_liquidity(token_symbol)
            if not liquidity_analysis["has_sufficient_liquidity"]:
                logger.debug(f"Skipping {token_symbol} due to insufficient liquidity. " +
                         f"CEX Volume: ${liquidity_analysis['total_cex_volume']:,.2f}, " +
                         f"DEX Liquidity: ${liquidity_analysis['total_dex_liquidity']:,.2f}")
                return

            # Fetch DEX data
            dex_data = await self.dex.get_token_data(token_symbol)
            if not dex_data:
                return

            dex_price = dex_data["price"]

            # Get prices from all CEXes
            cex_prices = await self.cex_manager.get_futures_prices(token_symbol)
            if not any(cex_prices.values()):
                return

            # Find best arbitrage opportunity
            best_spread = 0
            best_cex = None
            best_cex_price = None

            for cex_name, cex_price in cex_prices.items():
                if cex_price is None:
                    continue
                spread = abs(cex_price - dex_price) / dex_price
                if spread > best_spread:
                    best_spread = spread
                    best_cex = cex_name
                    best_cex_price = cex_price

            if best_spread >= ARBITRAGE_THRESHOLD:
                await self._send_arbitrage_notification(
                    token_symbol, best_spread, best_cex, best_cex_price,
                    dex_price, dex_data, liquidity_analysis
                )

        except Exception as e:
            logger.error(f"Error checking arbitrage for {token_symbol}: {e}")

    async def _send_arbitrage_notification(self, token_symbol, spread, cex_name, 
                                         cex_price, dex_price, dex_data, liquidity_analysis):
        """Send arbitrage opportunity notification"""
        try:
            # Get deposit/withdraw info from all exchanges
            dw_info = await self.cex_manager.get_deposit_withdraw_info(token_symbol)
            cex_info = dw_info.get(cex_name, {})

            # Build clickable links
            cex_link = f"https://www.{cex_name.lower()}.com/trade/{token_symbol}_USDT"
            dex_link = dex_data["dex_url"]

            # Get volumes from all exchanges
            volumes = await self.cex_manager.get_24h_volumes(token_symbol)
            total_volume = sum(vol for vol in volumes.values() if vol is not None)

            # Build the Telegram message
            message = (
                f"*Монета:* {token_symbol}\n\n"
                f"*Спред:* {spread*100:.2f}% между [DEX]({dex_link}) и [{cex_name}]({cex_link})\n\n"
                f"*Цена DEX:* {dex_price}\n"
                f"*Цена {cex_name}:* {cex_price}\n\n"
                f"*Ликвидность:*\n"
                f"Total CEX Volume 24h: ${total_volume:,.2f}\n"
                f"DEX Liquidity: ${dex_data['liquidity']:,.2f}\n\n"
                f"*{cex_name} Info:*\n"
                f"Max Volume: {cex_info.get('max_volume', 'N/A')}\n"
                f"Deposit: {cex_info.get('deposit', 'N/A')}\n"
                f"Withdraw: {cex_info.get('withdraw', 'N/A')}\n\n"
                f"*Контракт:* [ {dex_data.get('contract')}](tg://copy?text={dex_data.get('contract')})\n"
                f"*Сеть:* {dex_data.get('network')}\n"
            )
            await self.notifier.send_message(message)
        except Exception as e:
            logger.error(f"Error sending notification for {token_symbol}: {e}")

    async def run(self):
        """Main loop of the arbitrage engine"""
        logger.info("Starting Arbitrage Engine...")
        try:
            while True:
                try:
                    # Get all available tokens
                    available_tokens = await self.get_available_tokens()
                    if not available_tokens:
                        logger.warning("No tokens available for arbitrage, waiting before retry...")
                        await asyncio.sleep(UPDATE_INTERVAL)
                        continue

                    # Get list of high liquidity tokens
                    high_liquidity_tokens = await self.liquidity_analyzer.get_high_liquidity_tokens(available_tokens)
                    if high_liquidity_tokens:
                        logger.info(f"Processing {len(high_liquidity_tokens)} tokens with sufficient liquidity")
                        
                        # Process tokens in batches to control rate limits
                        for i in range(0, len(high_liquidity_tokens), BATCH_SIZE):
                            batch = high_liquidity_tokens[i:i + BATCH_SIZE]
                            await self.process_token_batch([token["symbol"] for token in batch])
                            
                            # Small delay between batches to prevent rate limit issues
                            if i + BATCH_SIZE < len(high_liquidity_tokens):
                                await asyncio.sleep(1)
                    
                    # Wait before next update cycle
                    await asyncio.sleep(UPDATE_INTERVAL)
                    
                except Exception as e:
                    logger.error(f"Error in arbitrage engine main loop: {e}")
                    await asyncio.sleep(UPDATE_INTERVAL)
        finally:
            # Ensure we close all exchange connections
            await self.cex_manager.close()
            await self.dex.close()