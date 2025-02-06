import aiohttp
import time
import hashlib
import hmac
from typing import Dict, List, Optional
from utils.logger import logger
from config import MEXC_API_KEY, MEXC_API_SECRET
from .base import BaseCEX

class MEXC(BaseCEX):
    SPOT_API_URL = "https://www.mexc.com/open/api/v2/market/ticker"
    FUTURES_API_URL = "https://contract.mexc.com/api/v1/contract/ticker"
    PRIVATE_API_URL = "https://api.mexc.com"
    FUTURES_SYMBOLS_URL = "https://contract.mexc.com/api/v1/contract/detail"

    def __init__(self):
        super().__init__()
        self.api_key = MEXC_API_KEY
        self.api_secret = MEXC_API_SECRET
        self.session = None

    @property
    def name(self) -> str:
        return "MEXC"

    @property
    def market_rate_limit_key(self) -> str:
        return "mexc_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "mexc_private"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _generate_signature(self, params: Dict) -> str:
        """Generate signature for private API calls"""
        sorted_params = sorted(params.items())
        signature_string = '&'.join([f"{key}={value}" for key, value in sorted_params])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            signature_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success") and data.get("data"):
                        ticker = data["data"][0]
                        price = float(ticker.get("last", 0))
                        logger.info(f"MEXC Spot Price for {symbol}: {price}")
                        return price
                logger.error(f"MEXC Spot API error for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success") and data.get("data"):
                        ticker = data["data"][0]
                        price = float(ticker.get("last", 0))
                        logger.info(f"MEXC Futures Price for {symbol}: {price}")
                        return price
                logger.error(f"MEXC Futures API error for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        await self._acquire_private_rate_limit()
        endpoint = "/api/v3/capital/config/getall"
        timestamp = int(time.time() * 1000)
        
        params = {
            "timestamp": timestamp,
            "recvWindow": 5000
        }
        
        signature = self._generate_signature(params)
        headers = {
            "X-MEXC-APIKEY": self.api_key,
            "Content-Type": "application/json"
        }
        
        params["signature"] = signature
        session = await self._get_session()
        
        try:
            async with session.get(
                f"{self.PRIVATE_API_URL}{endpoint}",
                params=params,
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    coin_info = next((coin for coin in data if coin.get("coin") == symbol), None)
                    
                    if coin_info:
                        network_info = max(
                            coin_info.get("networkList", []),
                            key=lambda x: float(x.get("withdrawMax", 0)) if x.get("withdrawMax", "0").replace(".", "").isdigit() else 0,
                            default={}
                        )
                        
                        info = {
                            "max_volume": network_info.get("withdrawMax", "N/A"),
                            "deposit": "Enabled" if network_info.get("depositEnable", False) else "Disabled",
                            "withdraw": "Enabled" if network_info.get("withdrawEnable", False) else "Disabled"
                        }
                        logger.info(f"MEXC deposit/withdraw info for {symbol}: {info}")
                        return info
                    
                    logger.error(f"Could not find {symbol} in MEXC capital config")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
        except Exception as e:
            logger.error(f"Exception in MEXC.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        try:
            async with session.get(self.FUTURES_SYMBOLS_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success") and data.get("data"):
                        symbols = []
                        for contract in data["data"]:
                            symbol = contract.get("symbol", "")
                            if symbol.endswith("_USDT"):
                                base_symbol = symbol.replace("_USDT", "")
                                symbols.append(base_symbol)
                        
                        logger.info(f"Found {len(symbols)} futures trading pairs on MEXC")
                        return symbols
                logger.error("Failed to parse MEXC futures symbols response")
                return []
        except Exception as e:
            logger.error(f"Exception in MEXC.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success") and data.get("data"):
                        ticker = data["data"][0]
                        volume = float(ticker.get("volume", 0)) * float(ticker.get("last", 0))
                        logger.info(f"MEXC 24h Volume for {symbol}: ${volume:,.2f}")
                        return volume
                logger.error(f"MEXC Volume API error for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_24h_volume: {e}")
            return None

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()