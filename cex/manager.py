import asyncio
from typing import Dict, List, Optional
from utils.logger import logger
from .base import BaseCEX
from .mexc import MEXC
from .okx import OKX
from .bitget import BitGet
from .gateio import GateIO
from .kucoin import KuCoin
from .bybit import Bybit
from .binance import Binance
from config import (
    BINANCE_API_KEY, MEXC_API_KEY, KUCOIN_API_KEY, BYBIT_API_KEY,
    OKX_API_KEY, GATEIO_API_KEY, BITGET_API_KEY,
    MIN_CEX_24H_VOLUME, MIN_DEX_LIQUIDITY
)

# Import other CEX implementations as they are added

class CEXManager:
    def __init__(self):
        self.exchanges: List[BaseCEX] = []
        self.min_volume_threshold = MIN_CEX_24H_VOLUME
        self.min_liquidity_threshold = MIN_DEX_LIQUIDITY
        
        # Initialize only exchanges with valid API credentials
        self._initialize_exchanges()
        
        if not self.exchanges:
            raise ValueError("No exchanges configured! Please provide API credentials for at least one exchange.")
        
        logger.info(f"Initialized {len(self.exchanges)} exchanges: {', '.join(ex.name for ex in self.exchanges)}")

    def _initialize_exchanges(self):
        """Initialize only exchanges with valid API credentials"""
        exchange_configs = [
            (MEXC_API_KEY, MEXC, "MEXC"),
            (OKX_API_KEY, OKX, "OKX"),
            (BITGET_API_KEY, BitGet, "BitGet"),
            (GATEIO_API_KEY, GateIO, "Gate.io"),
            (KUCOIN_API_KEY, KuCoin, "KuCoin"),
            (BYBIT_API_KEY, Bybit, "Bybit"),
            (BINANCE_API_KEY, Binance, "Binance"),
        ]

        for api_key, exchange_class, exchange_name in exchange_configs:
            if api_key and api_key.strip():  # Check if API key is provided and not empty
                try:
                    exchange = exchange_class()
                    self.exchanges.append(exchange)
                    logger.info(f"Successfully initialized {exchange_name} exchange")
                except Exception as e:
                    logger.error(f"Failed to initialize {exchange_name} exchange: {e}")
            else:
                logger.warning(f"Skipping {exchange_name} exchange - no API credentials provided")

    async def get_all_futures_symbols(self) -> Dict[str, List[str]]:
        """
        Get all available futures symbols from all exchanges.
        Returns a dict mapping exchange names to their available symbols.
        """
        tasks = [exchange.get_futures_symbols() for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        symbols_by_exchange = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting futures symbols from {exchange.name}: {result}")
                symbols_by_exchange[exchange.name] = []
            else:
                symbols_by_exchange[exchange.name] = result
        
        return symbols_by_exchange

    async def get_all_spot_symbols(self) -> Dict[str, List[str]]:
        """
        Get all available spot symbols from all exchanges.
        Returns a dict mapping exchange names to their available symbols.
        """
        tasks = [exchange.get_spot_symbols() for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        symbols_by_exchange = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting spot symbols from {exchange.name}: {result}")
                symbols_by_exchange[exchange.name] = []
            else:
                symbols_by_exchange[exchange.name] = result
        
        return symbols_by_exchange

    async def get_available_symbols(self) -> List[str]:
        """Get all available symbols from all exchanges"""
        try:
            # Get symbols from each exchange
            exchange_symbols = {}
            for exchange in self.exchanges:
                try:
                    logger.info(f"Getting symbols from {exchange.name}...")
                    spot_symbols = await exchange.get_spot_symbols()
                    futures_symbols = await exchange.get_futures_symbols()
                    
                    # Log the results
                    logger.info(f"{exchange.name} symbols retrieved:")
                    logger.info(f"• Spot symbols: {len(spot_symbols)}")
                    logger.info(f"• Futures symbols: {len(futures_symbols)}")
                    
                    # Combine spot and futures symbols
                    combined_symbols = list(set(spot_symbols + futures_symbols))
                    exchange_symbols[exchange.name] = combined_symbols
                    logger.info(f"• Total unique symbols for {exchange.name}: {len(combined_symbols)}")
                except Exception as e:
                    logger.error(f"Error getting symbols from {exchange.name}: {e}")
                    exchange_symbols[exchange.name] = []

            # Find common symbols across exchanges
            if not exchange_symbols:
                logger.error("No symbols retrieved from any exchange")
                return []

            # Get symbols that exist on at least two exchanges
            all_symbols = set()
            symbol_count = {}
            
            for symbols in exchange_symbols.values():
                for symbol in symbols:
                    symbol_count[symbol] = symbol_count.get(symbol, 0) + 1
                    all_symbols.add(symbol)

            common_symbols = [symbol for symbol, count in symbol_count.items() if count >= 2]
            
            # Log summary
            logger.info("\nSymbol availability summary:")
            logger.info(f"• Total unique symbols across all exchanges: {len(all_symbols)}")
            logger.info(f"• Symbols available on 2+ exchanges: {len(common_symbols)}")
            for symbol, count in sorted(symbol_count.items(), key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"• {symbol}: available on {count} exchanges")

            return common_symbols
        except Exception as e:
            logger.error(f"Error in get_available_symbols: {e}")
            return []

    async def get_futures_prices(self, symbol: str) -> Dict[str, Optional[float]]:
        """Get futures prices for a symbol from all exchanges"""
        tasks = [exchange.get_futures_price(symbol) for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        prices = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.debug(f"Error getting futures price from {exchange.name}: {result}")
                prices[exchange.name] = None
            else:
                prices[exchange.name] = result
        
        return prices

    async def get_spot_prices(self, symbol: str) -> Dict[str, Optional[float]]:
        """Get spot prices for a symbol from all exchanges"""
        tasks = [exchange.get_spot_price(symbol) for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        prices = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.debug(f"Error getting spot price from {exchange.name}: {result}")
                prices[exchange.name] = None
            else:
                prices[exchange.name] = result
        
        return prices

    async def get_all_prices(self, symbol: str) -> Dict[str, Dict[str, Optional[float]]]:
        """Get both spot and futures prices for a symbol from all exchanges"""
        tasks = []
        
        # Create tasks that await both spot and futures prices concurrently
        for exchange in self.exchanges:
            tasks.append(asyncio.gather(
                exchange.get_spot_price(symbol),
                exchange.get_futures_price(symbol),
                return_exceptions=True
            ))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        spot_prices = {}
        futures_prices = {}
        
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting prices from {exchange.name}: {result}")
                spot_prices[exchange.name] = None
                futures_prices[exchange.name] = None
            else:
                spot_price, futures_price = result
                if isinstance(spot_price, Exception):
                    logger.error(f"Error getting spot price from {exchange.name}: {spot_price}")
                    spot_prices[exchange.name] = None
                else:
                    spot_prices[exchange.name] = spot_price
                    
                if isinstance(futures_price, Exception):
                    logger.error(f"Error getting futures price from {exchange.name}: {futures_price}")
                    futures_prices[exchange.name] = None
                else:
                    futures_prices[exchange.name] = futures_price
        
        # Log the prices we got
        logger.info(f"\nPrices for {symbol}:")
        logger.info("Spot prices:")
        for cex, price in spot_prices.items():
            if price is not None:
                logger.info(f"• {cex}: ${price:.6f}")
        
        logger.info("Futures prices:")
        for cex, price in futures_prices.items():
            if price is not None:
                logger.info(f"• {cex}: ${price:.6f}")
        
        return {
            "futures": futures_prices,
            "spot": spot_prices
        }

    async def get_24h_volumes(self, symbol: str) -> Dict[str, Optional[float]]:
        """Get 24h trading volumes for a symbol from all exchanges"""
        tasks = [exchange.get_24h_volume(symbol) for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        volumes = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting volume from {exchange.name}: {result}")
                volumes[exchange.name] = None
            else:
                volumes[exchange.name] = result
        
        return volumes

    async def get_total_cex_volume(self, symbol: str) -> float:
        """Get total 24h volume across all exchanges"""
        volumes = await self.get_24h_volumes(symbol)
        return sum(volume for volume in volumes.values() if volume is not None)

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict[str, Dict]:
        """Get deposit/withdraw info from all exchanges"""
        tasks = [exchange.get_deposit_withdraw_info(symbol) for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        info = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting deposit/withdraw info from {exchange.name}: {result}")
                info[exchange.name] = {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
            else:
                info[exchange.name] = result
        
        return info

    async def close(self):
        """Close all exchange connections"""
        await asyncio.gather(*[exchange.close() for exchange in self.exchanges])

    async def get_all_exchange_symbols(self) -> Dict[str, List[str]]:
        """Get all available symbols from each exchange"""
        if hasattr(self, '_cached_symbols'):
            return self._cached_symbols
            
        exchange_symbols = {}
        
        # Create tasks for all exchanges
        tasks = []
        for exchange in self.exchanges:
            tasks.append(asyncio.gather(
                exchange.get_spot_symbols(),
                exchange.get_futures_symbols(),
                return_exceptions=True
            ))
            
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for exchange, result in zip(self.exchanges, results):
            try:
                logger.info(f"Getting symbols from {exchange.name}...")
                if isinstance(result, Exception):
                    logger.error(f"Error getting symbols from {exchange.name}: {result}")
                    exchange_symbols[exchange.name] = []
                    continue
                    
                spot_symbols, futures_symbols = result
                
                if isinstance(spot_symbols, Exception):
                    logger.error(f"Failed to get {exchange.name} spot symbols: {spot_symbols}")
                    spot_symbols = []
                if isinstance(futures_symbols, Exception):
                    logger.error(f"Failed to get {exchange.name} futures symbols: {futures_symbols}")
                    futures_symbols = []
                
                # Log the results
                logger.info(f"Found {len(spot_symbols)} spot trading pairs on {exchange.name}")
                logger.info(f"Found {len(futures_symbols)} futures trading pairs on {exchange.name}")
                
                # Combine spot and futures symbols
                combined_symbols = list(set(spot_symbols + futures_symbols))
                exchange_symbols[exchange.name] = combined_symbols
                
                logger.info(f"Found {len(combined_symbols)} total symbols on {exchange.name}")
            except Exception as e:
                logger.error(f"Error processing symbols from {exchange.name}: {e}")
                exchange_symbols[exchange.name] = []
        
        # Cache the results
        self._cached_symbols = exchange_symbols
        return exchange_symbols

    async def check_tokens_availability(self, tokens: List[str]) -> List[str]:
        """Check which tokens are available on multiple exchanges"""
        # Use cached symbols if available
        exchange_symbols = await self.get_all_exchange_symbols()
        
        # Count on how many exchanges each token appears
        token_count = {}
        for token in tokens:
            count = sum(1 for symbols in exchange_symbols.values() if token in symbols)
            token_count[token] = count
        
        # Return tokens available on at least 2 exchanges
        available_tokens = [token for token, count in token_count.items() if count >= 2]
        logger.info(f"Found {len(available_tokens)} tokens available on multiple exchanges")
        
        return available_tokens 