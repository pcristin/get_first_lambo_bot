import requests
import hmac
import hashlib
import time
import aiohttp
import base64
from typing import Dict, List, Optional
from utils.logger import logger
from config import GATEIO_API_KEY, GATEIO_API_SECRET
from .base import BaseCEX

class GateIO(BaseCEX):
    SPOT_API_URL = "https://api.gateio.ws/api/v4/spot/tickers"
    FUTURES_API_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
    CURRENCY_API_URL = "https://api.gateio.ws/api/v4/spot/currencies"
    PRIVATE_API_URL = "https://api.gateio.ws/api/v4"

    def __init__(self):
        super().__init__()
        self.api_key = GATEIO_API_KEY
        self.api_secret = GATEIO_API_SECRET
        self.session = None

    @property
    def name(self) -> str:
        return "Gate.io"

    @property
    def market_rate_limit_key(self) -> str:
        return "gateio_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "gateio_private"

    def _generate_signature(self, method, url, query_string='', body=''):
        t = time.time()
        m = hashlib.sha512()
        m.update((query_string + body).encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string, hashed_payload, t)
        sign = hmac.new(self.api_secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': self.api_key, 'Timestamp': str(t), 'SIGN': sign}

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        await self._acquire_market_rate_limit()
        currency_pair = f"{symbol}_USDT"
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        if ticker.get("currency_pair") == currency_pair:
                            price = float(ticker.get("last", 0))
                            logger.info(f"Gate.io Spot Price for {symbol}: {price}")
                            return price
                    logger.error(f"Gate.io Spot: Ticker for {symbol} not found.")
                    return None
                logger.error(f"Failed to get Gate.io spot price: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in GateIO.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        currency_pair = f"{symbol}_USDT"
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        if ticker.get("currency_pair") == currency_pair:
                            price = float(ticker.get("last", 0))
                            logger.info(f"Gate.io Futures Price for {symbol}: {price}")
                            return price
                    logger.error(f"Gate.io Futures: Ticker for {symbol} not found.")
                    return None
                logger.error(f"Failed to get Gate.io futures price: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in GateIO.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using Gate.io's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            await self._acquire_private_rate_limit()
            url = f"/api/v4/spot/currencies/{symbol}"
            headers = self._generate_signature("GET", url)
            session = await self._get_session()
            
            async with session.get(
                f"{self.CURRENCY_API_URL}/{symbol}",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict):
                        chains = data.get("chains", [])
                        
                        # Try to find BSC chain first, fall back to first available chain
                        chain_info = next((chain for chain in chains if chain.get("chain") == "BSC"), None)
                        if not chain_info and chains:
                            chain_info = chains[0]
                        
                        if chain_info:
                            return {
                                "max_volume": chain_info.get("withdraw_max", "N/A"),
                                "deposit": "Enabled" if chain_info.get("deposit_status") == "enable" else "Disabled",
                                "withdraw": "Enabled" if chain_info.get("withdraw_status") == "enable" else "Disabled"
                            }
                
                logger.error(f"Gate.io: Failed to get currency info for {symbol}")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
                
        except Exception as e:
            logger.error(f"Exception in GateIO.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.FUTURES_API_URL}") as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for ticker in data:
                        pair = ticker.get("currency_pair", "")
                        if pair.endswith("_USDT"):
                            symbol = pair.replace("_USDT", "")
                            symbols.append(symbol)
                    logger.info(f"Found {len(symbols)} futures trading pairs on Gate.io")
                    return symbols
                logger.error("Failed to get Gate.io futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in GateIO.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Get 24h trading volume for a symbol"""
        await self._acquire_market_rate_limit()
        currency_pair = f"{symbol}_USDT"
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        if ticker.get("currency_pair") == currency_pair:
                            volume = float(ticker.get("volume_24h_usd", 0))
                            logger.info(f"Gate.io 24h Volume for {symbol}: ${volume:,.2f}")
                            return volume
                    logger.error(f"Gate.io: Ticker for {symbol} not found")
                    return None
                logger.error(f"Gate.io Volume API error for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Exception in GateIO.get_24h_volume: {e}")
            return None

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def get_spot_symbols(self) -> List[str]:
        """Get all available spot trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for ticker in data:
                        pair = ticker.get("currency_pair", "")
                        if pair.endswith("_USDT"):
                            symbol = pair.replace("_USDT", "")
                            symbols.append(symbol)
                    logger.info(f"Found {len(symbols)} spot trading pairs on Gate.io")
                    return symbols
                logger.error("Failed to get Gate.io spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in GateIO.get_spot_symbols: {e}")
            return []