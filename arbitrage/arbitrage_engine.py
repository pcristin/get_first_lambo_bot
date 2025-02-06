import asyncio
import time
from typing import List, Optional, Dict, Tuple
from config import ARBITRAGE_THRESHOLD, BATCH_SIZE, UPDATE_INTERVAL
from dex.dexscreener import DexScreener
from dex.jupiter import JupiterAPI
from cex.manager import CEXManager
from notifier.telegram_notifier import TelegramNotifier
from utils.logger import logger
from utils.liquidity_analyzer import LiquidityAnalyzer

class ArbitrageEngine:
    def __init__(self):
        self.dex = DexScreener()
        self.jupiter = JupiterAPI()
        self.cex_manager = CEXManager()
        self.notifier = TelegramNotifier()
        self.liquidity_analyzer = LiquidityAnalyzer(cex_manager=self.cex_manager)
        self.known_tokens = set()
        # Cache for token data
        self._token_cache = {}
        self._cache_expiry = {}
        self._CACHE_DURATION = 60  # Cache duration in seconds
        self._running = True  # Flag to control the main loop
        self._shutdown_event = asyncio.Event()  # Event for coordinating shutdown
        
        # Verify threshold at startup
        logger.info("🚀 ArbitrageEngine initialized")
        logger.info(f"🎯 Arbitrage threshold set to: {ARBITRAGE_THRESHOLD * 100}%")
        if ARBITRAGE_THRESHOLD < 0.00001:  # Less than 0.001%
            logger.warning(f"Very low arbitrage threshold detected: {ARBITRAGE_THRESHOLD*100:.6f}%. This may generate many signals.")

    async def _test_notification(self):
        """Send a test notification to verify Telegram bot functionality"""
        try:
            # In MarkdownV2, we need to escape special characters: . - ! ( )
            threshold = str(ARBITRAGE_THRESHOLD * 100).replace('.', '\.')
            message = (
                "🤖 *Arbitrage Bot Started*\n\n"
                "⚙️ *Settings:*\n"
                f"• Threshold: `{threshold}%`\n"
                f"• Batch Size: `{BATCH_SIZE}`\n"
                f"• Update Interval: `{UPDATE_INTERVAL}s`\n\n"
                "✅ Bot is running and monitoring for opportunities\\!"
            )
            
            logger.info("Sending test notification...")
            success = await self.notifier.send_message(message)
            
            if success:
                logger.info("✅ Test notification sent successfully")
            else:
                logger.error("❌ Test notification failed to send")
                
        except Exception as e:
            logger.error(f"❌ Failed to send test notification: {e}")
            # Re-raise to ensure startup issues are visible
            raise

    async def process_token_batch(self, tokens: List[str]) -> int:
        """Process a batch of tokens in parallel with improved efficiency"""
        opportunities = 0
        
        logger.info(f"\n{'='*20} Processing batch of {len(tokens)} tokens {'='*20}")
        logger.info(f"Tokens in batch: {tokens}")
        
        # First get DEX data to filter for Solana tokens
        dex_data_tasks = [self.dex.get_token_data(token) for token in tokens]
        dex_data_results = await asyncio.gather(*dex_data_tasks, return_exceptions=True)
        
        # Filter for Solana tokens and get their contract addresses
        solana_tokens: Dict[str, Tuple[str, dict]] = {}  # {symbol: (contract, dex_data)}
        for token, dex_result in zip(tokens, dex_data_results):
            if isinstance(dex_result, Exception):
                logger.error(f"Error getting DEX data for {token}: {dex_result}")
                continue
            if dex_result is None:
                logger.warning(f"No DEX data found for {token}")
                continue
            if dex_result.get("network") == "solana":
                contract = dex_result.get("contract")
                if contract:
                    solana_tokens[token] = (contract, dex_result)
                    logger.info(f"Found Solana token: {token} | Contract: {contract}")
                else:
                    logger.warning(f"No contract found for Solana token: {token}")
        
        if not solana_tokens:
            logger.info("No Solana tokens found in this batch")
            return 0
            
        logger.info(f"Found {len(solana_tokens)} Solana tokens in batch")
        logger.info(f"Solana tokens: {list(solana_tokens.keys())}")
            
        # Get Jupiter prices and CEX prices in parallel
        valid_tokens = list(solana_tokens.keys())
        jupiter_tasks = [
            self.jupiter.get_token_price(solana_tokens[token][0]) 
            for token in valid_tokens
        ]
        price_tasks = [self.cex_manager.get_all_prices(token) for token in valid_tokens]
        liquidity_tasks = [self.liquidity_analyzer.analyze_token_liquidity(token) for token in valid_tokens]
        
        # Gather all results
        all_results = await asyncio.gather(
            *jupiter_tasks, *price_tasks, *liquidity_tasks,
            return_exceptions=True
        )
        
        # Split results
        num_tokens = len(valid_tokens)
        jupiter_results = all_results[:num_tokens]
        price_results = all_results[num_tokens:2*num_tokens]
        liquidity_results = all_results[2*num_tokens:]
        
        # Process each token
        for i, token in enumerate(valid_tokens):
            try:
                jupiter_price = jupiter_results[i]
                if isinstance(jupiter_price, Exception) or jupiter_price is None:
                    logger.error(f"Failed to get Jupiter price for {token}: {jupiter_price if isinstance(jupiter_price, Exception) else 'None'}")
                    continue
                
                cex_prices = price_results[i]
                if isinstance(cex_prices, Exception):
                    logger.error(f"Failed to get CEX prices for {token}: {cex_prices}")
                    continue
                
                # Check if we have any valid prices
                has_valid_prices = False
                for market_type in ['spot', 'futures']:
                    if any(price is not None for price in cex_prices[market_type].values()):
                        has_valid_prices = True
                        break
                
                if not has_valid_prices:
                    logger.warning(f"No valid CEX prices found for {token}")
                    continue
                
                # Log the prices we got
                logger.info(f"\nProcessing {token}:")
                logger.info(f"Jupiter Price: ${jupiter_price:.6f}")
                logger.info("CEX Prices:")
                logger.info(f"• Spot: {', '.join([f'{cex}: ${price:.6f}' for cex, price in cex_prices['spot'].items() if price is not None])}")
                logger.info(f"• Futures: {', '.join([f'{cex}: ${price:.6f}' for cex, price in cex_prices['futures'].items() if price is not None])}")
                
                liquidity_data = liquidity_results[i]
                if isinstance(liquidity_data, Exception):
                    logger.warning(f"Failed to get liquidity data for {token}, using defaults")
                    liquidity_data = {
                        "has_sufficient_liquidity": False,
                        "total_cex_volume": 0,
                        "total_dex_liquidity": 0
                    }
                
                # Process with Jupiter price instead of DEXscreener price
                opportunities += await self._process_single_token(
                    token, 
                    {"price": jupiter_price, **solana_tokens[token][1]},
                    liquidity_data,
                    cex_prices
                )
            except Exception as e:
                logger.error(f"Error processing token {token}: {e}")
                
        logger.info(f"\nBatch processing complete. Found {opportunities} opportunities.")
        return opportunities

    async def _process_single_token(self, token: str, dex_data: dict, 
                                  liquidity_data: dict, prices: dict) -> int:
        """Process a single token with pre-fetched data"""
        try:
            opportunities_found = 0
            
            # Log all available prices for debugging
            logger.info(f"\n{'='*20} {token} {'='*20}")
            
            # First check for arbitrage between different CEXes futures prices
            logger.info("Checking CEX-CEX Futures opportunities...")
            futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None]
            for i, (cex1, price1) in enumerate(futures_prices):
                for cex2, price2 in futures_prices[i+1:]:
                    # Calculate spread both ways to ensure we don't miss opportunities
                    spread1 = (price1 - price2) / price2  # CEX2 -> CEX1
                    spread2 = (price2 - price1) / price1  # CEX1 -> CEX2
                    spread = max(abs(spread1), abs(spread2))
                    
                    if spread >= ARBITRAGE_THRESHOLD:
                        # Determine which exchange has higher/lower price
                        if price1 > price2:
                            high_cex, high_price = cex1, price1
                            low_cex, low_price = cex2, price2
                        else:
                            high_cex, high_price = cex2, price2
                            low_cex, low_price = cex1, price1
                        
                        logger.info(f"🎯 Found CEX-CEX Futures arbitrage opportunity: {token}")
                        logger.info(f"   {high_cex}: ${high_price:.6f}")
                        logger.info(f"   {low_cex}: ${low_price:.6f}")
                        logger.info(f"   Spread: {spread*100:.4f}%")
                        
                        # Get liquidity data only when we find an opportunity
                        if not liquidity_data.get("has_sufficient_liquidity"):
                            liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                            if not liquidity_data["has_sufficient_liquidity"]:
                                logger.info(f"Skipping {token} due to insufficient liquidity")
                                continue
                        
                        await self._send_cex_arbitrage_notification(
                            token, spread,
                            high_cex, high_price,
                            low_cex, low_price,
                            liquidity_data
                        )
                        opportunities_found += 1
                        return opportunities_found  # Return immediately after finding an opportunity

            # Check for arbitrage between different CEXes spot prices
            logger.info("Checking CEX-CEX Spot opportunities...")
            spot_prices = [(cex, price) for cex, price in prices["spot"].items() if price is not None]
            for i, (cex1, price1) in enumerate(spot_prices):
                for cex2, price2 in spot_prices[i+1:]:
                    spread1 = (price1 - price2) / price2  # CEX2 -> CEX1
                    spread2 = (price2 - price1) / price1  # CEX1 -> CEX2
                    spread = max(abs(spread1), abs(spread2))
                    
                    if spread >= ARBITRAGE_THRESHOLD:
                        if price1 > price2:
                            high_cex, high_price = cex1, price1
                            low_cex, low_price = cex2, price2
                        else:
                            high_cex, high_price = cex2, price2
                            low_cex, low_price = cex1, price1
                        
                        logger.info(f"🎯 Found CEX-CEX Spot arbitrage opportunity: {token}")
                        logger.info(f"   {high_cex}: ${high_price:.6f}")
                        logger.info(f"   {low_cex}: ${low_price:.6f}")
                        logger.info(f"   Spread: {spread*100:.4f}%")
                        
                        # Get liquidity data only when we find an opportunity
                        if not liquidity_data.get("has_sufficient_liquidity"):
                            liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                            if not liquidity_data["has_sufficient_liquidity"]:
                                logger.info(f"Skipping {token} due to insufficient liquidity")
                                continue
                        
                        await self._send_cex_arbitrage_notification(
                            token, spread,
                            high_cex, high_price,
                            low_cex, low_price,
                            liquidity_data
                        )
                        opportunities_found += 1
                        return opportunities_found  # Return immediately after finding an opportunity

            # Only proceed with DEX checks if we have valid DEX data and no CEX-CEX opportunities found
            if opportunities_found == 0 and dex_data.get("price"):
                dex_price = dex_data["price"]
                logger.info(f"No CEX-CEX opportunities found. Checking DEX price: ${dex_price:.6f}")
                
                # Check spot prices against DEX
                logger.info("Checking CEX-DEX Spot opportunities...")
                for cex_name, spot_price in prices["spot"].items():
                    if spot_price is not None:
                        spread1 = (spot_price - dex_price) / dex_price  # DEX -> CEX
                        spread2 = (dex_price - spot_price) / spot_price  # CEX -> DEX
                        spread = max(abs(spread1), abs(spread2))
                        
                        if spread >= ARBITRAGE_THRESHOLD:
                            # Get liquidity data only when we find an opportunity
                            if not liquidity_data.get("has_sufficient_liquidity"):
                                liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                if not liquidity_data["has_sufficient_liquidity"]:
                                    logger.info(f"Skipping {token} due to insufficient liquidity")
                                    continue
                            
                            logger.info(f"🎯 Found spot arbitrage opportunity: {token} on {cex_name}")
                            logger.info(f"   DEX Price: ${dex_price:.6f}")
                            logger.info(f"   CEX Price: ${spot_price:.6f}")
                            logger.info(f"   Spread: {spread*100:.4f}%")
                            await self._send_arbitrage_notification(
                                token, spread, cex_name, spot_price,
                                dex_price, dex_data, liquidity_data, "spot"
                            )
                            opportunities_found += 1
                            return opportunities_found  # Return immediately after finding an opportunity

                # Check futures prices against DEX
                logger.info("Checking CEX-DEX Futures opportunities...")
                for cex_name, futures_price in prices["futures"].items():
                    if futures_price is not None:
                        spread1 = (futures_price - dex_price) / dex_price  # DEX -> CEX
                        spread2 = (dex_price - futures_price) / futures_price  # CEX -> DEX
                        spread = max(abs(spread1), abs(spread2))
                        
                        if spread >= ARBITRAGE_THRESHOLD:
                            # Get liquidity data only when we find an opportunity
                            if not liquidity_data.get("has_sufficient_liquidity"):
                                liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                if not liquidity_data["has_sufficient_liquidity"]:
                                    logger.info(f"Skipping {token} due to insufficient liquidity")
                                    continue
                            
                            logger.info(f"🎯 Found futures arbitrage opportunity: {token} on {cex_name}")
                            logger.info(f"   DEX Price: ${dex_price:.6f}")
                            logger.info(f"   CEX Price: ${futures_price:.6f}")
                            logger.info(f"   Spread: {spread*100:.4f}%")
                            await self._send_arbitrage_notification(
                                token, spread, cex_name, futures_price,
                                dex_price, dex_data, liquidity_data, "futures"
                            )
                            opportunities_found += 1
                            return opportunities_found  # Return immediately after finding an opportunity

            if opportunities_found > 0:
                logger.info(f"Found {opportunities_found} arbitrage opportunities for {token}")
            else:
                logger.debug(f"No arbitrage opportunities found for {token}")
            logger.debug("="*50)
            return opportunities_found
        except Exception as e:
            logger.error(f"Error in _process_single_token for {token}: {e}")
            return 0

    async def _send_arbitrage_notification(self, token_symbol, spread, cex_name, 
                                         cex_price, dex_price, dex_data, liquidity_analysis,
                                         market_type="futures"):
        """Send arbitrage opportunity notification"""
        try:
            # Get deposit/withdraw info from all exchanges
            dw_info = await self.cex_manager.get_deposit_withdraw_info(token_symbol)
            cex_info = dw_info.get(cex_name, {})

            # Build clickable links - escape special characters in URLs
            cex_link = f"https://www\\.{cex_name.lower()}\\.com/trade/{token_symbol}_USDT"
            dex_link = dex_data["dex_url"].replace(".", "\\.").replace("-", "\\-")

            # Get volumes from all exchanges
            volumes = await self.cex_manager.get_24h_volumes(token_symbol)
            total_volume = sum(vol for vol in volumes.values() if vol is not None)

            # Calculate price difference in USD
            price_diff_usd = abs(dex_price - cex_price)
            
            # Calculate potential profit on 1000 USDT trade
            potential_profit = (1000 * spread)

            # Escape special characters in numbers and text
            spread_str = f"{spread*100:.2f}".replace(".", "\\.")
            price_diff_str = f"{price_diff_usd:.4f}".replace(".", "\\.")
            cex_price_str = f"{cex_price:.4f}".replace(".", "\\.")
            dex_price_str = f"{dex_price:.4f}".replace(".", "\\.")
            total_volume_str = f"{total_volume:,.2f}".replace(".", "\\.").replace(",", "\\,")
            dex_liquidity_str = f"{dex_data['liquidity']:,.2f}".replace(".", "\\.").replace(",", "\\,")
            potential_profit_str = f"{potential_profit:.2f}".replace(".", "\\.")
            current_time = time.strftime('%Y\\-%m\\-%d %H:%M:%S UTC')
            network = dex_data.get('network', 'Unknown').replace("-", "\\-")
            contract = dex_data.get('contract', '').replace("-", "\\-")

            message = (
                f"🚨 *НОВЫЙ DEX\\-CEX АРБИТРАЖ\\!* 🚨\n\n"
                f"💎 *Токен:* `{token_symbol}`\n"
                f"📊 *Спред:* `{spread_str}%` _\\(${price_diff_str}\\)_\n\n"
                
                f"🔄 *Цены:*\n"
                f"• DEX \\([{network}]({dex_link})\\): `${dex_price_str}`\n"
                f"• {cex_name} \\({market_type}\\) \\([Торговать]({cex_link})\\): `${cex_price_str}`\n\n"
                
                f"💰 *Ликвидность:*\n"
                f"• CEX Volume 24h: `${total_volume_str}`\n"
                f"• DEX Liquidity: `${dex_liquidity_str}`\n\n"
                
                f"📈 *Потенциальная прибыль \\(1000 USDT\\):* `${potential_profit_str}`\n\n"
                
                f"🏦 *{cex_name} Информация:*\n"
                f"• Max Volume: `{cex_info.get('max_volume', 'N/A')}`\n"
                f"• Deposit: `{cex_info.get('deposit', 'N/A')}` {'✅' if cex_info.get('deposit') == 'Enabled' else '❌'}\n"
                f"• Withdraw: `{cex_info.get('withdraw', 'N/A')}` {'✅' if cex_info.get('withdraw') == 'Enabled' else '❌'}\n\n"
                
                f"📝 *Контракт:*\n"
                f"`{contract}` \\[(Copy)](tg://copy?text={contract})\n\n"
                
                f"ℹ️ *Дополнительная информация:*\n"
                f"• Сеть: `{network}`\n"
                f"• Тип: `{market_type.upper()}`\n"
                f"• Время: `{current_time}`\n"
            )
            await self.notifier.send_message(message)
        except Exception as e:
            logger.error(f"Error sending notification for {token_symbol}: {e}")

    async def _send_cex_arbitrage_notification(self, token_symbol, spread, 
                                         high_cex, high_price,
                                         low_cex, low_price,
                                         liquidity_analysis):
        """Send arbitrage opportunity notification for CEX-CEX opportunities"""
        try:
            # Get deposit/withdraw info from both exchanges
            dw_info = await self.cex_manager.get_deposit_withdraw_info(token_symbol)
            high_cex_info = dw_info.get(high_cex, {})
            low_cex_info = dw_info.get(low_cex, {})

            # Build clickable links - escape special characters in URLs
            high_cex_link = f"https://www\\.{high_cex.lower()}\\.com/trade/{token_symbol}_USDT"
            low_cex_link = f"https://www\\.{low_cex.lower()}\\.com/trade/{token_symbol}_USDT"

            # Get volumes
            volumes = await self.cex_manager.get_24h_volumes(token_symbol)
            total_volume = sum(vol for vol in volumes.values() if vol is not None)

            # Calculate price difference in USD
            price_diff_usd = abs(high_price - low_price)
            
            # Calculate potential profit on 1000 USDT trade
            potential_profit = (1000 * spread)

            # Escape special characters in numbers
            spread_str = f"{spread*100:.2f}".replace(".", "\\.")
            price_diff_str = f"{price_diff_usd:.4f}".replace(".", "\\.")
            high_price_str = f"{high_price:.4f}".replace(".", "\\.")
            low_price_str = f"{low_price:.4f}".replace(".", "\\.")
            total_volume_str = f"{total_volume:,.2f}".replace(".", "\\.").replace(",", "\\,")
            potential_profit_str = f"{potential_profit:.2f}".replace(".", "\\.")
            current_time = time.strftime('%Y\\-%m\\-%d %H:%M:%S UTC')

            message = (
                f"🚨 *НОВЫЙ CEX\\-CEX АРБИТРАЖ\\!* 🚨\n\n"
                f"💎 *Токен:* `{token_symbol}`\n"
                f"📊 *Спред:* `{spread_str}%` _\\(${price_diff_str}\\)_\n\n"
                
                f"🔄 *Цены:*\n"
                f"• {high_cex} \\([Торговать]({high_cex_link})\\): `${high_price_str}`\n"
                f"• {low_cex} \\([Торговать]({low_cex_link})\\): `${low_price_str}`\n\n"
                
                f"💰 *Объем торгов:*\n"
                f"• Total CEX Volume 24h: `${total_volume_str}`\n\n"
                
                f"📈 *Потенциальная прибыль \\(1000 USDT\\):* `${potential_profit_str}`\n\n"
                
                f"🏦 *{high_cex} Информация:*\n"
                f"• Max Volume: `{high_cex_info.get('max_volume', 'N/A')}`\n"
                f"• Deposit: `{high_cex_info.get('deposit', 'N/A')}` {'✅' if high_cex_info.get('deposit') == 'Enabled' else '❌'}\n"
                f"• Withdraw: `{high_cex_info.get('withdraw', 'N/A')}` {'✅' if high_cex_info.get('withdraw') == 'Enabled' else '❌'}\n\n"
                
                f"🏦 *{low_cex} Информация:*\n"
                f"• Max Volume: `{low_cex_info.get('max_volume', 'N/A')}`\n"
                f"• Deposit: `{low_cex_info.get('deposit', 'N/A')}` {'✅' if low_cex_info.get('deposit') == 'Enabled' else '❌'}\n"
                f"• Withdraw: `{low_cex_info.get('withdraw', 'N/A')}` {'✅' if low_cex_info.get('withdraw') == 'Enabled' else '❌'}\n\n"
                
                f"ℹ️ *Дополнительная информация:*\n"
                f"• Тип: `FUTURES`\n"
                f"• Время: `{current_time}`\n"
            )
            await self.notifier.send_message(message)
        except Exception as e:
            logger.error(f"Error sending CEX-CEX notification for {token_symbol}: {e}")

    async def get_available_tokens(self):
        """
        Fetch available tokens by:
        1. Getting tokens from the exchange with most listings
        2. Checking their availability on other exchanges
        3. Only then checking DEXscreener for Solana tokens
        Returns a list of token symbols.
        """
        try:
            # Get all symbols from each exchange and find the one with most tokens
            exchange_symbols = await self.cex_manager.get_all_exchange_symbols()
            if not exchange_symbols:
                logger.error("Failed to get symbols from exchanges")
                return []

            # Find exchange with the most tokens
            base_exchange, base_tokens = max(exchange_symbols.items(), key=lambda x: len(x[1]))
            logger.info(f"Using {base_exchange} as base exchange with {len(base_tokens)} tokens")

            tokens = []
            # Process tokens in batches for efficiency
            batch_size = 50
            for i in range(0, len(base_tokens), batch_size):
                batch = base_tokens[i:i + batch_size]
                
                # First check availability on other exchanges (parallel)
                available_cex_tokens = await self.cex_manager.check_tokens_availability(batch)
                if not available_cex_tokens:
                    continue

                # Check DEXscreener for Solana tokens
                dex_tasks = [self.dex.get_token_data(symbol) for symbol in available_cex_tokens]
                dex_results = await asyncio.gather(*dex_tasks, return_exceptions=True)
                
                # Add only Solana tokens that exist on both CEX and DEX
                for token, dex_result in zip(available_cex_tokens, dex_results):
                    if not isinstance(dex_result, Exception) and dex_result is not None:
                        if dex_result.get("network") == "solana" and dex_result.get("contract"):
                            if token not in self.known_tokens:
                                logger.info(f"Found new Solana token {token} listed on both CEX and DEX")
                                self.known_tokens.add(token)
                            tokens.append(token)

            # Log changes in available tokens
            if len(tokens) != len(self.known_tokens):
                logger.info(f"Total Solana tokens available for arbitrage: {len(tokens)}")
                removed_tokens = self.known_tokens - set(tokens)
                if removed_tokens:
                    logger.info(f"Tokens no longer available: {removed_tokens}")
                self.known_tokens = set(tokens)

            return tokens
        except Exception as e:
            logger.error(f"Error in get_available_tokens: {e}")
            return []

    async def check_arbitrage(self, token_symbol) -> Optional[bool]:
        """Check arbitrage opportunities for a single token. Returns True if opportunity found."""
        try:
            # First check if token has sufficient liquidity
            liquidity_analysis = await self.liquidity_analyzer.analyze_token_liquidity(token_symbol)
            if not liquidity_analysis["has_sufficient_liquidity"]:
                logger.debug(f"Skipping {token_symbol} due to insufficient liquidity. " +
                          f"CEX Volume: ${liquidity_analysis['total_cex_volume']:,.2f}, " +
                          f"DEX Liquidity: ${liquidity_analysis['total_dex_liquidity']:,.2f}")
                return None

            # Fetch DEX data
            dex_data = await self.dex.get_token_data(token_symbol)
            if not dex_data:
                return None

            # Get prices from all exchanges
            prices = await self.cex_manager.get_all_prices(token_symbol)
            
            # Debug log to show all received prices
            logger.info(f"\nReceived prices for {token_symbol}:")
            logger.info("Spot prices:")
            spot_prices = [(cex, price) for cex, price in prices["spot"].items() if price is not None]
            if spot_prices:
                for cex, price in spot_prices:
                    logger.info(f"• {cex}: ${price:.6f}")
            else:
                logger.info("• No valid spot prices received")
                
            logger.info("Futures prices:")
            futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None]
            if futures_prices:
                for cex, price in futures_prices:
                    logger.info(f"• {cex}: ${price:.6f}")
            else:
                logger.info("• No valid futures prices received")

            # Find best arbitrage opportunities for both spot and futures
            best_opportunities = []

            # Check spot prices
            for cex_name, spot_price in spot_prices:
                if spot_price is not None:
                    spread = abs(spot_price - dex_data["price"]) / dex_data["price"]
                    if spread >= ARBITRAGE_THRESHOLD:
                        best_opportunities.append({
                            "type": "spot",
                            "cex_name": cex_name,
                            "cex_price": spot_price,
                            "spread": spread
                        })

            # Check futures prices
            for cex_name, futures_price in futures_prices:
                if futures_price is not None:
                    spread = abs(futures_price - dex_data["price"]) / dex_data["price"]
                    if spread >= ARBITRAGE_THRESHOLD:
                        best_opportunities.append({
                            "type": "futures",
                            "cex_name": cex_name,
                            "cex_price": futures_price,
                            "spread": spread
                        })

            # Send notifications for all opportunities found
            for opportunity in best_opportunities:
                await self._send_arbitrage_notification(
                    token_symbol, 
                    opportunity["spread"],
                    opportunity["cex_name"],
                    opportunity["cex_price"],
                    dex_data["price"],
                    dex_data,
                    liquidity_analysis,
                    market_type=opportunity["type"]
                )

            return len(best_opportunities) > 0

        except Exception as e:
            logger.error(f"Error checking arbitrage for {token_symbol}: {e}")
            return None

    async def stop(self):
        """Stop the arbitrage engine gracefully"""
        if not self._running:
            return
            
        logger.info("Stopping arbitrage engine...")
        self._running = False
        self._shutdown_event.set()
        
        # Give ongoing operations a chance to complete
        try:
            await asyncio.wait_for(self._cleanup(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Some cleanup operations timed out")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    async def _cleanup(self):
        """Internal cleanup method"""
        cleanup_tasks = []
        
        # Close all connections with timeout protection
        if self.cex_manager:
            cleanup_tasks.append(self.cex_manager.close())
        if self.dex:
            cleanup_tasks.append(self.dex.close())
        if self.jupiter:
            cleanup_tasks.append(self.jupiter.close())
        if self.liquidity_analyzer:
            cleanup_tasks.append(self.liquidity_analyzer.close())
        if hasattr(self.notifier, 'close'):
            cleanup_tasks.append(self.notifier.close())
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def close(self):
        """Close all connections and cleanup resources"""
        try:
            self._running = False
            self._shutdown_event.set()
            await self._cleanup()
            logger.info("Successfully closed all connections")
        except Exception as e:
            logger.error(f"Error during arbitrage engine cleanup: {str(e)}")

    async def run(self):
        """Main loop for the arbitrage engine"""
        try:
            logger.info("🔄 Starting arbitrage engine...")
            
            # Send test notification on startup
            await self._test_notification()
            
            while self._running and not self._shutdown_event.is_set():
                try:
                    start_time = time.time()
                    logger.info("\n" + "="*50)
                    logger.info("🔍 Starting new scan cycle")
                    
                    # Get all symbols from each exchange and find the one with most tokens
                    exchange_symbols = await self.cex_manager.get_all_exchange_symbols()
                    if not exchange_symbols:
                        logger.error("Failed to get symbols from exchanges")
                        continue

                    # Find exchange with the most tokens
                    base_exchange, base_tokens = max(exchange_symbols.items(), key=lambda x: len(x[1]))
                    logger.info(f"Using {base_exchange} as base exchange with {len(base_tokens)} tokens")
                    
                    # Process tokens in parallel batches for speed
                    batch_size = 50
                    processed = 0
                    opportunities = 0
                    
                    for i in range(0, len(base_tokens), batch_size):
                        if not self._running or self._shutdown_event.is_set():
                            break
                            
                        batch = base_tokens[i:i + batch_size]
                        price_tasks = [self.cex_manager.get_all_prices(token) for token in batch]
                        price_results = await asyncio.gather(*price_tasks, return_exceptions=True)
                        
                        for token, prices in zip(batch, price_results):
                            if isinstance(prices, Exception):
                                logger.error(f"Error getting prices for {token}: {prices}")
                                continue
                                
                            # Check futures prices first (usually more liquid)
                            futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None]
                            for i, (cex1, price1) in enumerate(futures_prices):
                                for cex2, price2 in futures_prices[i+1:]:
                                    spread = abs(price1 - price2) / min(price1, price2)
                                    
                                    if spread >= ARBITRAGE_THRESHOLD:
                                        logger.info(f"🎯 Found futures arbitrage for {token}:")
                                        logger.info(f"   {cex1}: ${price1:.6f}")
                                        logger.info(f"   {cex2}: ${price2:.6f}")
                                        logger.info(f"   Spread: {spread*100:.2f}%")
                                        
                                        # Determine high/low prices
                                        if price1 > price2:
                                            high_cex, high_price = cex1, price1
                                            low_cex, low_price = cex2, price2
                                        else:
                                            high_cex, high_price = cex2, price2
                                            low_cex, low_price = cex1, price1
                                        
                                        # Quick liquidity check before notification
                                        liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                        if liquidity_data["has_sufficient_liquidity"]:
                                            await self._send_cex_arbitrage_notification(
                                                token, spread,
                                                high_cex, high_price,
                                                low_cex, low_price,
                                                liquidity_data
                                            )
                                            opportunities += 1
                            
                            # Check spot prices
                            spot_prices = [(cex, price) for cex, price in prices["spot"].items() if price is not None]
                            for i, (cex1, price1) in enumerate(spot_prices):
                                for cex2, price2 in spot_prices[i+1:]:
                                    spread = abs(price1 - price2) / min(price1, price2)
                                    
                                    if spread >= ARBITRAGE_THRESHOLD:
                                        logger.info(f"🎯 Found spot arbitrage for {token}:")
                                        logger.info(f"   {cex1}: ${price1:.6f}")
                                        logger.info(f"   {cex2}: ${price2:.6f}")
                                        logger.info(f"   Spread: {spread*100:.2f}%")
                                        
                                        # Determine high/low prices
                                        if price1 > price2:
                                            high_cex, high_price = cex1, price1
                                            low_cex, low_price = cex2, price2
                                        else:
                                            high_cex, high_price = cex2, price2
                                            low_cex, low_price = cex1, price1
                                        
                                        # Quick liquidity check before notification
                                        liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                        if liquidity_data["has_sufficient_liquidity"]:
                                            await self._send_cex_arbitrage_notification(
                                                token, spread,
                                                high_cex, high_price,
                                                low_cex, low_price,
                                                liquidity_data
                                            )
                                            opportunities += 1
                            
                            processed += 1
                            if processed % 100 == 0:
                                logger.info(f"Processed {processed}/{len(base_tokens)} tokens, found {opportunities} opportunities")
                    
                    # Calculate and log cycle statistics
                    cycle_time = time.time() - start_time
                    logger.info(f"🏁 Scan cycle completed in {cycle_time:.2f}s")
                    logger.info(f"📈 Processed {processed} tokens, found {opportunities} opportunities")
                    logger.info("="*50 + "\n")
                    
                    if not self._running or self._shutdown_event.is_set():
                        break
                    
                    # Wait for next cycle or shutdown
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=UPDATE_INTERVAL)
                        break  # If we get here, shutdown was requested
                    except asyncio.TimeoutError:
                        continue  # Normal timeout, continue to next cycle
                    
                except asyncio.CancelledError:
                    logger.info("Main loop cancelled, initiating shutdown...")
                    raise
                except Exception as e:
                    logger.error(f"❌ Error in main loop: {e}")
                    if self._running and not self._shutdown_event.is_set():
                        await asyncio.sleep(5)  # Wait before retrying
                    
        except asyncio.CancelledError:
            logger.info("🛑 Arbitrage engine stopping due to cancellation...")
            raise
        finally:
            logger.info("Cleaning up resources...")
            await self.close()
            logger.info("Arbitrage engine stopped")