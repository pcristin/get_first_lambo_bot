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
            (BYBIT_API_KEY, Bybit, "Bybit")
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
                logger.error(f"Error getting symbols from {exchange.name}: {result}")
                symbols_by_exchange[exchange.name] = []
            else:
                symbols_by_exchange[exchange.name] = result
        
        return symbols_by_exchange

    async def get_common_symbols(self) -> List[str]:
        """Get symbols that are available on all exchanges"""
        symbols_by_exchange = await self.get_all_futures_symbols()
        if not symbols_by_exchange:
            return []

        # Start with symbols from first exchange
        common_symbols = set(next(iter(symbols_by_exchange.values())))
        
        # Find intersection with all other exchanges
        for symbols in symbols_by_exchange.values():
            common_symbols &= set(symbols)

        logger.info(f"Found {len(common_symbols)} symbols common to all {len(self.exchanges)} active exchanges")
        return list(common_symbols)

    async def get_futures_prices(self, symbol: str) -> Dict[str, Optional[float]]:
        """Get futures prices for a symbol from all exchanges"""
        tasks = [exchange.get_futures_price(symbol) for exchange in self.exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        prices = {}
        for exchange, result in zip(self.exchanges, results):
            if isinstance(result, Exception):
                logger.error(f"Error getting price from {exchange.name}: {result}")
                prices[exchange.name] = None
            else:
                prices[exchange.name] = result
        
        return prices

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