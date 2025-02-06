import requests
import hmac
import hashlib
import time
import json
import aiohttp
from utils.logger import logger
from config import BYBIT_API_KEY, BYBIT_API_SECRET
from typing import Dict, List, Optional
from .base import BaseCEX

class Bybit(BaseCEX):
    SPOT_API_URL = "https://api.bybit.com/v5/market/tickers"
    FUTURES_API_URL = "https://api.bybit.com/v5/market/tickers"
    COIN_INFO_API_URL = "https://api.bybit.com/v5/asset/coin/query-info"
    PRIVATE_API_URL = "https://api.bybit.com"

    def __init__(self):
        super().__init__()
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self.session = None

    @property
    def name(self) -> str:
        return "Bybit"

    @property
    def market_rate_limit_key(self) -> str:
        return "bybit_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "bybit_private"

    def _generate_signature(self, params):
        timestamp = str(int(time.time() * 1000))
        param_str = timestamp + self.api_key + str(params)
        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return timestamp, signature

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {
            "category": "spot",
            "symbol": formatted_symbol
        }
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        price = float(data["result"]["list"][0].get("lastPrice", 0))
                        logger.info(f"Bybit Spot Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"Bybit Spot API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get Bybit spot price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bybit.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {
            "category": "linear",
            "symbol": formatted_symbol
        }
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        price = float(data["result"]["list"][0].get("lastPrice", 0))
                        logger.info(f"Bybit Futures Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"Bybit Futures API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get Bybit futures price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bybit.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using Bybit's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            await self._acquire_private_rate_limit()
            params = {
                "coin": symbol
            }
            
            timestamp, signature = self._generate_signature(params)
            
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000"
            }
            
            session = await self._get_session()
            
            async with session.get(
                self.COIN_INFO_API_URL,
                params=params,
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("rows"):
                        coin_info = data["result"]["rows"][0]
                        chains = coin_info.get("chains", [])
                        
                        # Try to find BSC chain first, fall back to first available chain
                        chain_info = next(
                            (chain for chain in chains if chain.get("chain") == "BSC"),
                            None
                        )
                        if not chain_info and chains:
                            chain_info = chains[0]
                        
                        if chain_info:
                            return {
                                "max_volume": chain_info.get("withdrawLimit", "N/A"),
                                "deposit": "Enabled" if chain_info.get("depositStatus") else "Disabled",
                                "withdraw": "Enabled" if chain_info.get("withdrawStatus") else "Disabled"
                            }
                
                logger.error(f"Bybit: Failed to get currency info for {symbol}")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
                
        except Exception as e:
            logger.error(f"Exception in Bybit.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            params = {"category": "linear"}
            async with session.get(f"{self.FUTURES_API_URL}", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        symbols = []
                        for ticker in data["result"]["list"]:
                            symbol = ticker.get("symbol", "")
                            if symbol.endswith("USDT"):
                                base_symbol = symbol.replace("USDT", "")
                                symbols.append(base_symbol)
                        logger.info(f"Found {len(symbols)} futures trading pairs on Bybit")
                        return symbols
                logger.error("Failed to get Bybit futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in Bybit.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Get 24h trading volume for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {
            "category": "spot",
            "symbol": formatted_symbol
        }
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.SPOT_API_URL}", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        ticker = data["result"]["list"][0]
                        if "volume24h" in ticker and "lastPrice" in ticker:
                            volume = float(ticker["volume24h"]) * float(ticker["lastPrice"])
                            logger.info(f"Bybit 24h Volume for {symbol}: ${volume:,.2f}")
                            return volume
                    logger.error(f"Bybit Volume API error for {symbol}: {data.get('retMsg', 'Invalid response format')}")
                    return None
                logger.error(f"Bybit Volume API error for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bybit.get_24h_volume: {e}")
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
            params = {"category": "spot"}
            async with session.get(f"{self.SPOT_API_URL}", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        symbols = []
                        for ticker in data["result"]["list"]:
                            symbol = ticker.get("symbol", "")
                            if symbol.endswith("USDT"):
                                base_symbol = symbol.replace("USDT", "")
                                symbols.append(base_symbol)
                        logger.info(f"Found {len(symbols)} spot trading pairs on Bybit")
                        return symbols
                logger.error("Failed to get Bybit spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in Bybit.get_spot_symbols: {e}")
            return []