import asyncio
import time
from typing import List, Optional, Dict, Tuple, Set
from config import ARBITRAGE_THRESHOLD, BATCH_SIZE, UPDATE_INTERVAL, MIN_CEX_24H_VOLUME, MIN_DEX_LIQUIDITY
from dex.dexscreener import DexScreener
from dex.jupiter import JupiterAPI
from cex.manager import CEXManager
from cex.websocket_manager import WebSocketManager
from cex.binance_ws import BinanceWebSocket
from cex.okx_ws import OKXWebSocket
from notifier.telegram_notifier import TelegramNotifier
from utils.logger import logger
from utils.liquidity_analyzer import LiquidityAnalyzer
from utils.database import Database

class ArbitrageEngine:
    def __init__(self):
        self.dex = DexScreener()
        self.jupiter = JupiterAPI()
        self.cex_manager = CEXManager()
        self.notifier = TelegramNotifier()
        self.liquidity_analyzer = LiquidityAnalyzer(cex_manager=self.cex_manager)
        
        # Initialize WebSocket connections
        self.ws_manager = WebSocketManager()
        self.binance_ws = BinanceWebSocket(self.ws_manager)
        self.okx_ws = OKXWebSocket(self.ws_manager)
        
        # Initialize database
        self.db = Database()
        
        # Track active symbols
        self.active_symbols: Set[str] = set()
        self.known_tokens = set()
        
        # Cache for token data
        self._token_cache = {}
        self._cache_expiry = {}
        self._CACHE_DURATION = 60  # Cache duration in seconds
        self._running = True  # Flag to control the main loop
        self._shutdown_event = asyncio.Event()  # Event for coordinating shutdown
        
        # Verify threshold at startup
        logger.info("🚀 ArbitrageEngine initialized")
        logger.info(f"🎯 Arbitrage threshold set to: {str(ARBITRAGE_THRESHOLD).replace('.', ',')}%")
        if ARBITRAGE_THRESHOLD < 1:  # Less than 1%
            logger.warning(f"Very low arbitrage threshold detected: {str(ARBITRAGE_THRESHOLD).replace('.', ',')}%. This may generate many signals.")

    async def _test_notification(self):
        """Send a test notification to verify Telegram bot functionality"""
        try:
            # Initialize database
            await self.db.init()
            
            # Get summary stats if available
            stats = await self.db.get_summary_stats()
            
            # In MarkdownV2, we need to escape special characters: . - ! ( )
            threshold = str(ARBITRAGE_THRESHOLD).replace('.', ',')  # Convert to percentage for display
            message = (
                "🤖 *Arbitrage Bot Started*\n\n"
                "⚙️ *Settings:*\n"
                f"• Threshold: `{threshold}%`\n"
                f"• Batch Size: `{BATCH_SIZE}`\n"
                f"• Update Interval: `{UPDATE_INTERVAL}s`\n\n"
            )
            
            # Add stats if available
            if stats:
                message += (
                    "📊 *Historical Stats:*\n"
                    f"• Total Opportunities: `{stats['total_opportunities']}`\n"
                    f"• Total Trades: `{stats['total_trades']}`\n"
                    f"• Total Profit: `${str(stats['total_profit_usd']).replace('.', ',')}`\n"
                    f"• Average Spread: `{str(stats['avg_spread']).replace('.', ',')}%`\n\n"
                )
            
            message += "✅ Bot is running and monitoring for opportunities\\!"
            
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

    async def _subscribe_to_symbol(self, symbol: str):
        """Subscribe to real-time price updates for a symbol"""
        try:
            # Subscribe to Binance spot
            await self.binance_ws.subscribe_to_price(symbol)
            
            # Subscribe to OKX spot and futures
            await self.okx_ws.subscribe_to_price(symbol, "SPOT")
            await self.okx_ws.subscribe_to_price(symbol, "FUTURES")
            
            self.active_symbols.add(symbol)
            logger.info(f"Subscribed to real-time updates for {symbol}")
        except Exception as e:
            logger.error(f"Error subscribing to {symbol}: {e}")

    async def _unsubscribe_from_symbol(self, symbol: str):
        """Unsubscribe from price updates for a symbol"""
        try:
            # Unsubscribe from Binance spot
            await self.binance_ws.unsubscribe_from_price(symbol)
            
            # Unsubscribe from OKX spot and futures
            await self.okx_ws.unsubscribe_from_price(symbol, "SPOT")
            await self.okx_ws.unsubscribe_from_price(symbol, "FUTURES")
            
            self.active_symbols.discard(symbol)
            logger.info(f"Unsubscribed from {symbol}")
        except Exception as e:
            logger.error(f"Error unsubscribing from {symbol}: {e}")

    async def process_token_batch(self, tokens: List[str]) -> int:
        """Process a batch of tokens in parallel with improved efficiency"""
        opportunities = 0
        
        logger.info(f"\n{'='*20} Processing batch of {len(tokens)} tokens {'='*20}")
        
        for token in tokens:
            try:
                # Get prices from all exchanges
                prices = await self.cex_manager.get_all_prices(token)
                
                # Check if we have any valid prices
                spot_prices = [(cex, price) for cex, price in prices["spot"].items() if price is not None and price > 0]
                futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None and price > 0]
                
                # First check CEX-to-CEX opportunities
                # Check spot prices
                for i, (cex1, price1) in enumerate(spot_prices):
                    for cex2, price2 in spot_prices[i+1:]:
                        try:
                            spread1 = (price1 - price2) / price2 * 100
                            spread2 = (price2 - price1) / price1 * 100
                            spread = max(abs(spread1), abs(spread2))
                            
                            if spread >= ARBITRAGE_THRESHOLD:
                                if price1 > price2:
                                    high_cex, high_price = cex1, price1
                                    low_cex, low_price = cex2, price2
                                else:
                                    high_cex, high_price = cex2, price2
                                    low_cex, low_price = cex1, price1
                                
                                # Get liquidity data for informational purposes only
                                liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                
                                # Log opportunity and send notification
                                opportunity_id = await self.db.log_opportunity(
                                    token=token,
                                    spread=spread,
                                    high_exchange=high_cex,
                                    high_price=high_price,
                                    low_exchange=low_cex,
                                    low_price=low_price,
                                    market_type="spot",
                                    volume_24h=liquidity_data.get("total_cex_volume"),
                                    liquidity_score=liquidity_data.get("liquidity_score")
                                )
                                
                                await self._send_cex_arbitrage_notification(
                                    token, spread,
                                    high_cex, high_price,
                                    low_cex, low_price,
                                    liquidity_data,
                                    opportunity_id
                                )
                                opportunities += 1
                                continue  # Move to next token after finding opportunity
                                
                        except ZeroDivisionError:
                            continue
                
                # Check futures prices if no spot opportunity found
                if opportunities == 0:
                    for i, (cex1, price1) in enumerate(futures_prices):
                        for cex2, price2 in futures_prices[i+1:]:
                            try:
                                spread1 = (price1 - price2) / price2 * 100
                                spread2 = (price2 - price1) / price1 * 100
                                spread = max(abs(spread1), abs(spread2))
                                
                                if spread >= ARBITRAGE_THRESHOLD:
                                    if price1 > price2:
                                        high_cex, high_price = cex1, price1
                                        low_cex, low_price = cex2, price2
                                    else:
                                        high_cex, high_price = cex2, price2
                                        low_cex, low_price = cex1, price1
                                    
                                    # Get liquidity data for informational purposes only
                                    liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                    
                                    # Log opportunity and send notification
                                    opportunity_id = await self.db.log_opportunity(
                                        token=token,
                                        spread=spread,
                                        high_exchange=high_cex,
                                        high_price=high_price,
                                        low_exchange=low_cex,
                                        low_price=low_price,
                                        market_type="futures",
                                        volume_24h=liquidity_data.get("total_cex_volume"),
                                        liquidity_score=liquidity_data.get("liquidity_score")
                                    )
                                    
                                    await self._send_cex_arbitrage_notification(
                                        token, spread,
                                        high_cex, high_price,
                                        low_cex, low_price,
                                        liquidity_data,
                                        opportunity_id
                                    )
                                    opportunities += 1
                                    continue  # Move to next token after finding opportunity
                                    
                            except ZeroDivisionError:
                                continue
                
                # If no CEX-to-CEX opportunities found, check DEX
                if opportunities == 0:
                    # Get DEX data
                    dex_data = await self.dex.get_token_data(token)
                    if dex_data and dex_data.get("network") == "solana" and dex_data.get("price"):
                        dex_price = dex_data["price"]
                        
                        # Check against spot prices
                        for cex_name, spot_price in spot_prices:
                            try:
                                spread1 = (spot_price - dex_price) / dex_price * 100
                                spread2 = (dex_price - spot_price) / spot_price * 100
                                spread = max(abs(spread1), abs(spread2))
                                
                                if spread >= ARBITRAGE_THRESHOLD:
                                    # Get liquidity data for informational purposes only
                                    liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                    
                                    await self._send_arbitrage_notification(
                                        token, spread, cex_name, spot_price,
                                        dex_price, dex_data, liquidity_data, "spot"
                                    )
                                    opportunities += 1
                                    break  # Move to next token after finding opportunity
                                    
                            except ZeroDivisionError:
                                continue
                        
                        # Check against futures prices if no spot-DEX opportunity found
                        if opportunities == 0:
                            for cex_name, futures_price in futures_prices:
                                try:
                                    spread1 = (futures_price - dex_price) / dex_price * 100
                                    spread2 = (dex_price - futures_price) / futures_price * 100
                                    spread = max(abs(spread1), abs(spread2))
                                    
                                    if spread >= ARBITRAGE_THRESHOLD:
                                        # Get liquidity data for informational purposes only
                                        liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                        
                                        await self._send_arbitrage_notification(
                                            token, spread, cex_name, futures_price,
                                            dex_price, dex_data, liquidity_data, "futures"
                                        )
                                        opportunities += 1
                                        break  # Move to next token after finding opportunity
                                        
                                except ZeroDivisionError:
                                    continue
                
            except Exception as e:
                logger.error(f"Error processing token {token}: {e}")
                continue
                
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
            futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None and price > 0]
            for i, (cex1, price1) in enumerate(futures_prices):
                for cex2, price2 in futures_prices[i+1:]:
                    try:
                        # Calculate spread both ways to ensure we don't miss opportunities
                        spread1 = (price1 - price2) / price2 * 100  # CEX2 -> CEX1 (in percentage)
                        spread2 = (price2 - price1) / price1 * 100  # CEX1 -> CEX2 (in percentage)
                        spread = max(abs(spread1), abs(spread2))  # Get max spread in percentage
                    except ZeroDivisionError:
                        logger.warning(f"Skipping price comparison for {token} due to zero price: {cex1}=${price1}, {cex2}=${price2}")
                        continue
                    
                    if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
                        # Determine which exchange has higher/lower price
                        if price1 > price2:
                            high_cex, high_price = cex1, price1
                            low_cex, low_price = cex2, price2
                        else:
                            high_cex, high_price = cex2, price2
                            low_cex, low_price = cex1, price1
                        
                        logger.info(f"🎯 Found CEX-CEX Futures arbitrage opportunity: {token}")
                        logger.info(f"   {high_cex}: ${str(high_price).replace('.', ',')}")
                        logger.info(f"   {low_cex}: ${str(low_price).replace('.', ',')}")
                        logger.info(f"   Spread: {str(spread).replace('.', ',')}%")
                        
                        # Get liquidity data only when we find an opportunity
                        if not liquidity_data.get("has_sufficient_liquidity"):
                            liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                            if not liquidity_data["has_sufficient_liquidity"]:
                                logger.info(f"Skipping {token} due to insufficient liquidity")
                                continue
                        
                        # Log opportunity to database
                        opportunity_id = await self.db.log_opportunity(
                            token=token,
                            spread=spread,
                            high_exchange=high_cex,
                            high_price=high_price,
                            low_exchange=low_cex,
                            low_price=low_price,
                            market_type="futures",
                            volume_24h=liquidity_data.get("total_cex_volume"),
                            liquidity_score=liquidity_data.get("liquidity_score")
                        )
                        
                        await self._send_cex_arbitrage_notification(
                            token, spread,
                            high_cex, high_price,
                            low_cex, low_price,
                            liquidity_data,
                            opportunity_id
                        )
                        opportunities_found += 1
                        return opportunities_found  # Return immediately after finding an opportunity

            # Check for arbitrage between different CEXes spot prices
            logger.info("Checking CEX-CEX Spot opportunities...")
            spot_prices = [(cex, price) for cex, price in prices["spot"].items() if price is not None and price > 0]
            for i, (cex1, price1) in enumerate(spot_prices):
                for cex2, price2 in spot_prices[i+1:]:
                    try:
                        # Calculate spread both ways to ensure we don't miss opportunities
                        spread1 = (price1 - price2) / price2 * 100  # CEX2 -> CEX1 (in percentage)
                        spread2 = (price2 - price1) / price1 * 100  # CEX1 -> CEX2 (in percentage)
                        spread = max(abs(spread1), abs(spread2))  # Get max spread in percentage
                    except ZeroDivisionError:
                        logger.warning(f"Skipping price comparison for {token} due to zero price: {cex1}=${price1}, {cex2}=${price2}")
                        continue
                    
                    if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
                        if price1 > price2:
                            high_cex, high_price = cex1, price1
                            low_cex, low_price = cex2, price2
                        else:
                            high_cex, high_price = cex2, price2
                            low_cex, low_price = cex1, price1
                        
                        logger.info(f"🎯 Found CEX-CEX Spot arbitrage opportunity: {token}")
                        logger.info(f"   {high_cex}: ${str(high_price).replace('.', ',')}")
                        logger.info(f"   {low_cex}: ${str(low_price).replace('.', ',')}")
                        logger.info(f"   Spread: {str(spread).replace('.', ',')}%")
                        
                        # Get liquidity data only when we find an opportunity
                        if not liquidity_data.get("has_sufficient_liquidity"):
                            liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                            if not liquidity_data["has_sufficient_liquidity"]:
                                logger.info(f"Skipping {token} due to insufficient liquidity")
                                continue
                        
                        # Log opportunity to database
                        opportunity_id = await self.db.log_opportunity(
                            token=token,
                            spread=spread,
                            high_exchange=high_cex,
                            high_price=high_price,
                            low_exchange=low_cex,
                            low_price=low_price,
                            market_type="spot",
                            volume_24h=liquidity_data.get("total_cex_volume"),
                            liquidity_score=liquidity_data.get("liquidity_score")
                        )
                        
                        await self._send_cex_arbitrage_notification(
                            token, spread,
                            high_cex, high_price,
                            low_cex, low_price,
                            liquidity_data,
                            opportunity_id
                        )
                        opportunities_found += 1
                        return opportunities_found  # Return immediately after finding an opportunity

            # Only proceed with DEX checks if we have valid DEX data and no CEX-CEX opportunities found
            if opportunities_found == 0 and dex_data.get("price"):
                dex_price = dex_data["price"]
                logger.info(f"No CEX-CEX opportunities found. Checking DEX price: ${str(dex_price).replace('.', ',')}")
                
                # Check spot prices against DEX
                logger.info("Checking CEX-DEX Spot opportunities...")
                for cex_name, spot_price in prices["spot"].items():
                    if spot_price is not None:
                        # Calculate spread both ways to ensure we don't miss opportunities
                        spread1 = (spot_price - dex_price) / dex_price * 100  # DEX -> CEX (in percentage)
                        spread2 = (dex_price - spot_price) / spot_price * 100  # CEX -> DEX (in percentage)
                        spread = max(abs(spread1), abs(spread2))
                        
                        if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
                            # Get liquidity data only when we find an opportunity
                            if not liquidity_data.get("has_sufficient_liquidity"):
                                liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                if not liquidity_data["has_sufficient_liquidity"]:
                                    logger.info(f"Skipping {token} due to insufficient liquidity")
                                    continue
                            
                            logger.info(f"🎯 Found spot arbitrage opportunity: {token} on {cex_name}")
                            logger.info(f"   DEX Price: ${str(dex_price).replace('.', ',')}")
                            logger.info(f"   CEX Price: ${str(spot_price).replace('.', ',')}")
                            logger.info(f"   Spread: {str(spread).replace('.', ',')}%")
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
                        # Calculate spread both ways to ensure we don't miss opportunities
                        spread1 = (futures_price - dex_price) / dex_price * 100  # DEX -> CEX (in percentage)
                        spread2 = (dex_price - futures_price) / futures_price * 100  # CEX -> DEX (in percentage)
                        spread = max(abs(spread1), abs(spread2))
                        
                        if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
                            # Get liquidity data only when we find an opportunity
                            if not liquidity_data.get("has_sufficient_liquidity"):
                                liquidity_data = await self.liquidity_analyzer.analyze_token_liquidity(token)
                                if not liquidity_data["has_sufficient_liquidity"]:
                                    logger.info(f"Skipping {token} due to insufficient liquidity")
                                    continue
                            
                            logger.info(f"🎯 Found futures arbitrage opportunity: {token} on {cex_name}")
                            logger.info(f"   DEX Price: ${str(dex_price).replace('.', ',')}")
                            logger.info(f"   CEX Price: ${str(futures_price).replace('.', ',')}")
                            logger.info(f"   Spread: {str(spread).replace('.', ',')}%")
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
            
            # Calculate potential profit on 1000 USDT trade (spread is already in percentage)
            potential_profit = (1000 * spread / 100)  # Convert percentage to decimal for calculation

            # Escape special characters in numbers and text
            spread_str = f"{spread:.4f}".replace('.', ',')
            price_diff_str = f"{price_diff_usd:.4f}".replace('.', ',')
            cex_price_str = f"{cex_price:.4f}".replace('.', ',')
            dex_price_str = f"{dex_price:.4f}".replace('.', ',')
            total_volume_str = f"{total_volume:,.2f}".replace('.', ',')
            dex_liquidity_str = f"{dex_data['liquidity']:,.2f}".replace('.', ',')
            potential_profit_str = f"{potential_profit:.4f}".replace('.', ',')
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
                
                f"📈 *Потенциальная прибыль \\(1000 USDT\\):* `${potential_profit_str} USDT`\n\n"
                
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
                                         liquidity_analysis,
                                         opportunity_id: Optional[int] = None):
        """Send notification for CEX-CEX arbitrage opportunity"""
        try:
            # Get deposit/withdraw info for both exchanges
            dw_info = await self.cex_manager.get_deposit_withdraw_info(token_symbol)
            high_cex_info = dw_info.get(high_cex, {})
            low_cex_info = dw_info.get(low_cex, {})

            # Build clickable links - escape special characters in URLs
            high_cex_link = f"https://www\\.{high_cex.lower()}\\.com/trade/{token_symbol}_USDT"
            low_cex_link = f"https://www\\.{low_cex.lower()}\\.com/trade/{token_symbol}_USDT"

            # Calculate price difference and potential profit
            price_diff = abs(high_price - low_price)
            potential_profit = (1000 * spread / 100)  # Convert percentage to decimal for calculation

            # Format current time
            current_time = time.strftime('%Y\\-%m\\-%d %H:%M:%S UTC')

            # Format message with proper escaping for MarkdownV2
            message = (
                f"🚨 *НОВЫЙ CEX\\-CEX АРБИТРАЖ\\!* 🚨\n\n"
                f"💎 *Токен:* `{token_symbol}`\n"
                f"📊 *Спред:* `{spread:.4f}%` _\\(${price_diff:.4f}\\)_\n\n"
                
                f"🔄 *Цены:*\n"
                f"• {high_cex} \\([Торговать]({high_cex_link})\\): `${high_price:.4f}`\n"
                f"• {low_cex} \\([Торговать]({low_cex_link})\\): `${low_price:.4f}`\n\n"
                
                f"💰 *Объем торгов:*\n"
                f"• Total CEX Volume 24h: `${str(liquidity_analysis.get('total_cex_volume', 0)).replace('.', ',')}`\n\n"
                
                f"📈 *Потенциальная прибыль \\(1000 USDT\\):* `${potential_profit:.4f} USDT`\n\n"
                
                f"🏦 *{high_cex} Информация:*\n"
                f"• Max Volume: `{high_cex_info.get('max_volume', 'N/A')}`\n"
                f"• Deposit: `{high_cex_info.get('deposit', 'N/A')}` {'✅' if high_cex_info.get('deposit') == 'Enabled' else '❌'}\n"
                f"• Withdraw: `{high_cex_info.get('withdraw', 'N/A')}` {'✅' if high_cex_info.get('withdraw') == 'Enabled' else '❌'}\n"
                f"• Withdraw Fee: `{high_cex_info.get('withdraw_fee', 'N/A')}`\n"
                f"• Chain: `{high_cex_info.get('chain', 'N/A')}`\n\n"
                
                f"🏦 *{low_cex} Информация:*\n"
                f"• Max Volume: `{low_cex_info.get('max_volume', 'N/A')}`\n"
                f"• Deposit: `{low_cex_info.get('deposit', 'N/A')}` {'✅' if low_cex_info.get('deposit') == 'Enabled' else '❌'}\n"
                f"• Withdraw: `{low_cex_info.get('withdraw', 'N/A')}` {'✅' if low_cex_info.get('withdraw') == 'Enabled' else '❌'}\n"
                f"• Withdraw Fee: `{low_cex_info.get('withdraw_fee', 'N/A')}`\n"
                f"• Chain: `{low_cex_info.get('chain', 'N/A')}`\n\n"
                
                f"• Тип: `FUTURES`\n"
                f"• Время: `{current_time}`\n"
            )
            
            success = await self.notifier.send_message(message)
            
            if success and opportunity_id:
                # Update notification status in database
                await self.db.execute(
                    "UPDATE opportunities SET notification_sent = 1 WHERE id = ?",
                    (opportunity_id,)
                )
            
        except Exception as e:
            logger.error(f"Error sending arbitrage notification: {e}")

    async def get_available_tokens(self):
        """
        Fetch available tokens by getting tokens from the exchange with most listings.
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
            return base_tokens

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
                          f"CEX Volume: ${str(liquidity_analysis['total_cex_volume']).replace('.', ',')}, " +
                          f"DEX Liquidity: ${str(liquidity_analysis['total_dex_liquidity']).replace('.', ',')}")
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
                    logger.info(f"• {cex}: ${str(price).replace('.', ',')}")
            else:
                logger.info("• No valid spot prices received")
                
            logger.info("Futures prices:")
            futures_prices = [(cex, price) for cex, price in prices["futures"].items() if price is not None]
            if futures_prices:
                for cex, price in futures_prices:
                    logger.info(f"• {cex}: ${str(price).replace('.', ',')}")
            else:
                logger.info("• No valid futures prices received")

            # Find best arbitrage opportunities for both spot and futures
            best_opportunities = []

            # Check spot prices
            for cex_name, spot_price in spot_prices:
                if spot_price is not None and dex_data["price"] is not None:
                    # Calculate spread both ways to ensure we don't miss opportunities
                    spread1 = (spot_price - dex_data["price"]) / dex_data["price"] * 100  # DEX -> CEX (in percentage)
                    spread2 = (dex_data["price"] - spot_price) / spot_price * 100  # CEX -> DEX (in percentage)
                    spread = max(abs(spread1), abs(spread2))  # Get max spread in percentage
                    
                    if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
                        best_opportunities.append({
                            "type": "spot",
                            "cex_name": cex_name,
                            "cex_price": spot_price,
                            "spread": spread
                        })

            # Check futures prices
            for cex_name, futures_price in futures_prices:
                if futures_price is not None and dex_data["price"] is not None:
                    # Calculate spread both ways to ensure we don't miss opportunities
                    spread1 = (futures_price - dex_data["price"]) / dex_data["price"] * 100  # DEX -> CEX (in percentage)
                    spread2 = (dex_data["price"] - futures_price) / futures_price * 100  # CEX -> DEX (in percentage)
                    spread = max(abs(spread1), abs(spread2))  # Get max spread in percentage
                    
                    if spread >= ARBITRAGE_THRESHOLD:  # Both values are in percentage now
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
        """Stop the arbitrage engine"""
        self._running = False
        self._shutdown_event.set()
        
        # Unsubscribe from all symbols
        unsubscribe_tasks = [self._unsubscribe_from_symbol(symbol) for symbol in self.active_symbols.copy()]
        if unsubscribe_tasks:
            await asyncio.gather(*unsubscribe_tasks)
        
        # Stop WebSocket connections
        await self.binance_ws.stop()
        await self.okx_ws.stop()
        
        await self._cleanup()

    async def _cleanup(self):
        """Cleanup resources"""
        try:
            # Close WebSocket manager
            await self.ws_manager.stop()
            
            # Close other connections
            await self.jupiter.close()
            await self.cex_manager.close()
            
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    async def close(self):
        """Close all connections"""
        await self.stop()

    async def run(self):
        """Main arbitrage loop with WebSocket integration"""
        try:
            # Initialize database
            await self.db.init()
            
            # Start WebSocket connections
            await self.binance_ws.start()
            await self.okx_ws.start()
            
            # Send test notification
            await self._test_notification()
            
            while self._running and not self._shutdown_event.is_set():
                try:
                    # Get available tokens
                    tokens = await self.get_available_tokens()
                    if not tokens:
                        logger.warning("No tokens available for arbitrage")
                        await asyncio.sleep(UPDATE_INTERVAL)
                        continue
                    
                    # Process tokens in batches
                    for i in range(0, len(tokens), BATCH_SIZE):
                        if not self._running or self._shutdown_event.is_set():
                            break
                        
                        batch = tokens[i:i + BATCH_SIZE]
                        await self.process_token_batch(batch)
                        
                        # Small delay between batches to avoid overwhelming the system
                        await asyncio.sleep(1)
                    
                    # Wait for the next update interval
                    await asyncio.sleep(UPDATE_INTERVAL)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in arbitrage loop: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                    
        except Exception as e:
            logger.error(f"Fatal error in arbitrage engine: {e}")
        finally:
            await self.stop()