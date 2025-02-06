import asyncio
import time
from typing import Dict, Optional
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class RateLimit:
    max_requests: int
    time_window: float  # in seconds
    weight: int = 1

class RateLimiter:
    def __init__(self):
        self.requests: Dict[str, list] = defaultdict(list)
        self.rate_limits: Dict[str, RateLimit] = {
            # MEXC rate limits
            'mexc_market': RateLimit(max_requests=20, time_window=1),    # 20 requests per second for market data
            'mexc_private': RateLimit(max_requests=60, time_window=60),  # 60 requests per minute for private endpoints
            
            # Bybit rate limits
            'bybit_market': RateLimit(max_requests=50, time_window=1),   # 50 requests per second for market data
            'bybit_private': RateLimit(max_requests=600, time_window=60), # 600 requests per minute for private endpoints
            
            # OKX rate limits
            'okx_market': RateLimit(max_requests=20, time_window=2),     # 20 requests per 2 seconds
            'okx_private': RateLimit(max_requests=300, time_window=60),  # 300 requests per minute
            
            # KuCoin rate limits
            'kucoin_market': RateLimit(max_requests=30, time_window=1),  # 30 requests per second
            'kucoin_private': RateLimit(max_requests=180, time_window=60), # 180 requests per minute
            
            # Gate.io rate limits
            'gateio_market': RateLimit(max_requests=300, time_window=60),  # 300 requests per minute
            'gateio_private': RateLimit(max_requests=180, time_window=60), # 180 requests per minute
            
            # BitGet rate limits
            'bitget_market': RateLimit(max_requests=20, time_window=1),   # 20 requests per second
            'bitget_private': RateLimit(max_requests=300, time_window=60), # 300 requests per minute

            # Binance rate limits
            'binance_market': RateLimit(max_requests=1200, time_window=60),  # 1200 requests per minute
            'binance_private': RateLimit(max_requests=60, time_window=60),   # 60 requests per minute
            
            # DexScreener rate limits (based on their documentation)
            'dexscreener': RateLimit(max_requests=30, time_window=60),  # 30 requests per minute
            
            # General fallback limits for unknown exchanges
            'default_market': RateLimit(max_requests=10, time_window=1),   # Conservative default
            'default_private': RateLimit(max_requests=30, time_window=60)  # Conservative default
        }

        # IP-based rate limits (shared across all instances)
        self.ip_rate_limits: Dict[str, RateLimit] = {
            'binance_ip': RateLimit(max_requests=2400, time_window=60),   # 2400 requests per minute per IP
            'okx_ip': RateLimit(max_requests=500, time_window=60),        # 500 requests per minute per IP
            'bybit_ip': RateLimit(max_requests=1200, time_window=60),     # 1200 requests per minute per IP
            'kucoin_ip': RateLimit(max_requests=1800, time_window=60),    # 1800 requests per minute per IP
            'gateio_ip': RateLimit(max_requests=900, time_window=60),     # 900 requests per minute per IP
            'mexc_ip': RateLimit(max_requests=1800, time_window=60),      # 1800 requests per minute per IP
            'dexscreener_ip': RateLimit(max_requests=60, time_window=60), # 60 requests per minute per IP
        }

    async def acquire(self, key: str, weight: int = 1, check_ip: bool = True) -> None:
        """
        Acquire permission to make an API request.
        Waits if necessary to comply with rate limits.
        
        Args:
            key: The rate limit key to use
            weight: The weight of the request
            check_ip: Whether to also check IP-based rate limits
        """
        # First check the endpoint-specific rate limit
        await self._acquire_limit(key, weight)
        
        # Then check IP-based rate limit if applicable
        if check_ip:
            ip_key = f"{key.split('_')[0]}_ip"  # Convert 'binance_market' to 'binance_ip'
            if ip_key in self.ip_rate_limits:
                await self._acquire_limit(ip_key, weight)

    async def _acquire_limit(self, key: str, weight: int = 1) -> None:
        """Internal method to acquire a specific rate limit"""
        rate_limit = self.rate_limits.get(key) or self.ip_rate_limits.get(key)
        if not rate_limit:
            if 'market' in key:
                rate_limit = self.rate_limits['default_market']
            else:
                rate_limit = self.rate_limits['default_private']

        current_time = time.time()
        
        # Remove old requests outside the time window
        self.requests[key] = [
            req_time for req_time in self.requests[key]
            if current_time - req_time <= rate_limit.time_window
        ]
        
        # Calculate current request weight
        current_weight = len(self.requests[key]) * weight
        
        # If we would exceed the rate limit, wait until we can proceed
        if current_weight + weight > rate_limit.max_requests:
            oldest_request = self.requests[key][0]
            wait_time = oldest_request + rate_limit.time_window - current_time
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                # Recursively check again after waiting
                await self._acquire_limit(key, weight)
                return
        
        # Add the new request timestamp
        self.requests[key].extend([current_time] * weight)

    def get_remaining_requests(self, key: str) -> int:
        """Get the number of remaining requests allowed in the current time window"""
        rate_limit = self.rate_limits.get(key) or self.ip_rate_limits.get(key)
        if not rate_limit:
            return 0
            
        current_time = time.time()
        
        # Clean up old requests
        self.requests[key] = [
            req_time for req_time in self.requests[key]
            if current_time - req_time <= rate_limit.time_window
        ]
        
        return rate_limit.max_requests - len(self.requests[key]) 