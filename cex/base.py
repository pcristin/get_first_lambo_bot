from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from utils.rate_limiter import RateLimiter

class BaseCEX(ABC):
    """Base class for all CEX implementations"""
    
    def __init__(self):
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

    async def _acquire_market_rate_limit(self, weight: int = 1):
        """Acquire market data rate limit"""
        await self.rate_limiter.acquire(self.market_rate_limit_key, weight)

    async def _acquire_private_rate_limit(self, weight: int = 1):
        """Acquire private endpoint rate limit"""
        await self.rate_limiter.acquire(self.private_rate_limit_key, weight)

    @abstractmethod
    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
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

    @abstractmethod
    async def close(self):
        """Close any open connections"""
        pass 