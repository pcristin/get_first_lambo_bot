# dex/dexscreener.py
import aiohttp
import asyncio
from typing import Dict, Optional
from utils.logger import logger
from utils.rate_limiter import RateLimiter

class DexScreener:
    BASE_URL = "https://api.dexscreener.com/latest/dex/search/"
    
    def __init__(self):
        self.rate_limiter = RateLimiter()
        self.session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def get_token_data(self, token_symbol: str) -> Optional[Dict]:
        """
        Searches Dexscreener for the token.
        Returns a dict with:
          - price: (float) price in USD
          - contract: (str) contract address
          - network: (str) chain/network (e.g. SOLANA)
          - dex_url: (str) direct URL to the token on Dexscreener
          - liquidity: (float) liquidity in USD
        """
        await self.rate_limiter.acquire('dexscreener')
        
        params = {"q": token_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.BASE_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("pairs"):
                        # Choose the first matching pair with highest liquidity
                        valid_pairs = [
                            pair for pair in data["pairs"]
                            if pair.get("baseToken", {}).get("symbol", "").upper() == token_symbol.upper()
                        ]
                        
                        if not valid_pairs:
                            logger.error(f"No matching pairs found for {token_symbol}")
                            return None
                            
                        # Sort by liquidity (USD)
                        sorted_pairs = sorted(
                            valid_pairs,
                            key=lambda x: float(x.get("liquidity", {}).get("usd", 0)),
                            reverse=True
                        )
                        
                        pair = sorted_pairs[0]
                        token_data = {
                            "price": float(pair.get("priceUsd", 0)),
                            "contract": pair.get("baseToken", {}).get("address", ""),
                            "network": pair.get("chainId", "").upper(),
                            "dex_url": f"https://dexscreener.com/{pair.get('chainId', '').lower()}/{pair.get('pairAddress', '')}",
                            "liquidity": float(pair.get("liquidity", {}).get("usd", 0))
                        }
                        logger.info(f"DexScreener data for {token_symbol}: {token_data}")
                        return token_data
                    else:
                        logger.error(f"No DexScreener results for {token_symbol}")
                        return None
                else:
                    logger.error(f"DexScreener API error: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error in DexScreener.get_token_data: {e}")
            return None

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()