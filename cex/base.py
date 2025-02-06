from abc import ABC, abstractmethod
import asyncio
from typing import Dict, List, Optional
import aiohttp
from utils.rate_limiter import RateLimiter
import random

class BaseCEX(ABC):
    """Base class for all CEX implementations"""
    
    def __init__(self):
        self.session = None
        self.rate_limiter = RateLimiter()
        self.max_retries = 3
        self.retry_delay = 1  # Base delay in seconds
        self.retry_exceptions = (
            aiohttp.ClientError,  # Network-related errors
            asyncio.TimeoutError,  # Timeout errors
            KeyError,  # Missing data in response
            ValueError,  # Invalid data in response
        )
    
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

    async def _handle_response(self, response: aiohttp.ClientResponse, error_msg: str) -> dict:
        """Handle API response with proper error handling"""
        try:
            if response.status == 429:  # Rate limit hit
                retry_after = int(response.headers.get('Retry-After', '5'))
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message="Rate limit exceeded"
                )
            
            data = await response.json()
            
            if response.status >= 400:
                error_details = str(data) if data else f"HTTP {response.status}"
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"{error_msg}: {error_details}"
                )
                
            return data
        except aiohttp.ContentTypeError:
            text = await response.text()
            raise ValueError(f"{error_msg}: Invalid JSON response: {text}")

    async def _retry_request(self, func, *args, **kwargs):
        """Execute a request with retries and exponential backoff"""
        from utils.logger import logger
        
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except self.retry_exceptions as e:
                if attempt == self.max_retries - 1:  # Last attempt
                    logger.error(f"{self.name}: Max retries ({self.max_retries}) exceeded: {str(e)}")
                    raise  # Re-raise the last exception
                
                # Calculate exponential backoff with jitter
                delay = min(30, self.retry_delay * (2 ** attempt) + random.uniform(0, 1))
                logger.warning(f"{self.name}: Request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                logger.info(f"{self.name}: Retrying in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                continue

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session with improved configuration"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,      # Total timeout
                connect=10,    # Connection timeout
                sock_read=10,  # Socket read timeout
                sock_connect=10  # Socket connect timeout
            )
            
            # Common headers for all requests
            headers = {
                'User-Agent': 'ArbitrageBot/1.0',
                'Accept': 'application/json',
            }
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                raise_for_status=False,
                headers=headers,
                connector=aiohttp.TCPConnector(
                    limit=100,  # Max concurrent connections
                    ttl_dns_cache=300,  # DNS cache TTL in seconds
                    enable_cleanup_closed=True
                )
            )
        return self.session

    async def close(self):
        """Close the exchange connection and cleanup resources with improved error handling"""
        from utils.logger import logger
        if self.session:
            try:
                if not self.session.closed:
                    await asyncio.wait_for(self.session.close(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(f"{self.name}: Timeout while closing session")
            except Exception as e:
                logger.error(f"{self.name}: Error closing session: {str(e)}")
            finally:
                self.session = None

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

    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """
        Get order book for a symbol
        Returns: {
            'bids': [(price, amount), ...],
            'asks': [(price, amount), ...],
            'timestamp': int  # Unix timestamp in milliseconds
        }
        """
        pass

    @abstractmethod
    async def get_ticker(self, symbol: str) -> Dict:
        """
        Get 24h ticker data for a symbol
        Returns: {
            'last': float,  # Last price
            'bid': float,   # Best bid
            'ask': float,   # Best ask
            'volume': float,  # 24h volume
            'timestamp': int  # Unix timestamp in milliseconds
        }
        """
        pass 