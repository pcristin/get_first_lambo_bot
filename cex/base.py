from abc import ABC, abstractmethod
import asyncio
from typing import Dict, List, Optional
import aiohttp
from utils.rate_limiter import RateLimiter

class BaseCEX(ABC):
    """Base class for all CEX implementations"""
    
    def __init__(self):
        self.session = None
        self.rate_limiter = RateLimiter()
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the exchange"""
        pass

    @property
    @abstractmethod
    def market_rate_limit_key(self) -> str:
        """Return the rate limit key for market data endpoints"""
        pass

    @property
    @abstractmethod
    def private_rate_limit_key(self) -> str:
        """Return the rate limit key for private endpoints"""
        pass

    async def _acquire_market_rate_limit(self):
        """Acquire market rate limit"""
        await self.rate_limiter.acquire(self.market_rate_limit_key)

    async def _acquire_private_rate_limit(self):
        """Acquire private rate limit"""
        await self.rate_limiter.acquire(self.private_rate_limit_key)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),  # 10 second timeout
                raise_for_status=False  # Don't raise exceptions for non-200 status codes
            )
        return self.session

    async def close(self):
        """Close the exchange connection and cleanup resources"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
        except Exception as e:
            # Just log the error but don't raise it
            from utils.logger import logger
            logger.error(f"Error closing {self.name} session: {str(e)}")

    async def __aenter__(self):
        """Support for async context manager"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure resources are cleaned up"""
        await self.close()

    @abstractmethod
    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        pass

    @abstractmethod
    async def get_spot_symbols(self) -> List[str]:
        """Get all available spot trading pairs"""
        pass

    @abstractmethod
    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        pass

    @abstractmethod
    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        pass

    @abstractmethod
    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """Get deposit/withdrawal information for a symbol"""
        pass

    @abstractmethod
    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Get 24h trading volume for a symbol"""
        pass 