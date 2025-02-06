import requests
import hmac
import base64
import time
import json
import aiohttp
from utils.logger import logger
from config import BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
from typing import Dict, List, Optional
from .base import BaseCEX

class BitGet(BaseCEX):
    SPOT_API_URL = "https://api.bitget.com/api/spot/v1/market/ticker"
    FUTURES_API_URL = "https://api.bitget.com/api/mix/v1/market/ticker"
    COIN_INFO_API_URL = "https://api.bitget.com/api/spot/v1/public/currencies"
    PRIVATE_API_URL = "https://api.bitget.com"

    def __init__(self):
        super().__init__()
        self.api_key = BITGET_API_KEY
        self.api_secret = BITGET_API_SECRET
        self.api_passphrase = BITGET_API_PASSPHRASE
        self.session = None

    @property
    def name(self) -> str:
        return "BitGet"

    @property
    def market_rate_limit_key(self) -> str:
        return "bitget_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "bitget_private"

    def _generate_signature(self, timestamp, method, request_path, body=''):
        message = str(timestamp) + str.upper(method) + request_path + (body or '')
        mac = hmac.new(
            bytes(self.api_secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        d = mac.digest()
        return base64.b64encode(d).decode()

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        ticker = data["data"][0]
                        price = float(ticker.get("last", 0))
                        logger.info(f"Bitget Spot Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"Bitget Spot API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get Bitget spot price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bitget.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        # For futures, using USDT perpetual contract.
        # Format: "ALPHAOFSOLUSDT_UMCBL" (the suffix may vary by contract type)
        formatted_symbol = f"{symbol}USDT_UMCBL"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        ticker = data["data"]
                        if isinstance(ticker, list):
                            ticker = ticker[0]
                        if "last" in ticker:
                            price = float(ticker["last"])
                            logger.info(f"Bitget Futures Price for {symbol}: {price}")
                            return price
                        else:
                            logger.error(f"Bitget Futures API error for {symbol}: No 'last' price in response")
                            return None
                    else:
                        logger.error(f"Bitget Futures API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get Bitget futures price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bitget.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using BitGet's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            await self._acquire_private_rate_limit()
            timestamp = str(int(time.time() * 1000))
            request_path = "/api/spot/v1/public/currencies"
            
            signature = self._generate_signature(timestamp, "GET", request_path)
            
            headers = {
                "ACCESS-KEY": self.api_key,
                "ACCESS-SIGN": signature,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-PASSPHRASE": self.api_passphrase,
                "Content-Type": "application/json"
            }
            
            session = await self._get_session()
            
            async with session.get(
                self.COIN_INFO_API_URL,
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        # Find the currency info
                        currency_info = next(
                            (curr for curr in data["data"] if curr.get("coinName") == symbol),
                            None
                        )
                        
                        if currency_info:
                            # Try to find BSC chain first, fall back to first available chain
                            chains = currency_info.get("chains", [])
                            chain_info = next(
                                (chain for chain in chains if chain.get("chain") == "BSC"),
                                None
                            )
                            if not chain_info and chains:
                                chain_info = chains[0]
                            
                            if chain_info:
                                return {
                                    "max_volume": chain_info.get("withdrawMax", "N/A"),
                                    "deposit": "Enabled" if chain_info.get("depositEnable") else "Disabled",
                                    "withdraw": "Enabled" if chain_info.get("withdrawEnable") else "Disabled"
                                }
                
                logger.error(f"BitGet: Failed to get currency info for {symbol}")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
            
        except Exception as e:
            logger.error(f"Exception in BitGet.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.FUTURES_API_URL}/instruments") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        symbols = []
                        for instrument in data["data"]:
                            if instrument.get("quoteCoin") == "USDT":
                                symbol = instrument.get("baseCoin")
                                if symbol:
                                    symbols.append(symbol)
                        logger.info(f"Found {len(symbols)} futures trading pairs on BitGet")
                        return symbols
                logger.error("Failed to get BitGet futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in BitGet.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Get 24h trading volume for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.SPOT_API_URL}", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        ticker = data["data"][0]
                        if "volume" in ticker and "close" in ticker:
                            volume = float(ticker["volume"]) * float(ticker["close"])
                            logger.info(f"BitGet 24h Volume for {symbol}: ${volume:,.2f}")
                            return volume
                    logger.error(f"BitGet Volume API error for {symbol}: {data.get('msg', 'Invalid response format')}")
                    return None
                logger.error(f"BitGet Volume API error for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in BitGet.get_24h_volume: {e}")
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
            async with session.get(f"{self.SPOT_API_URL}/tickers") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        symbols = []
                        for ticker in data["data"]:
                            symbol = ticker.get("symbol", "")
                            if symbol.endswith("USDT"):
                                base_symbol = symbol.replace("USDT", "")
                                symbols.append(base_symbol)
                        logger.info(f"Found {len(symbols)} spot trading pairs on BitGet")
                        return symbols
                logger.error("Failed to get BitGet spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in BitGet.get_spot_symbols: {e}")
            return []

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Get order book for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol, "limit": limit}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.PRIVATE_API_URL}/api/spot/v1/market/depth", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        book = data["data"]
                        return {
                            'bids': [(float(price), float(amount)) for price, amount in book.get("bids", [])],
                            'asks': [(float(price), float(amount)) for price, amount in book.get("asks", [])],
                            'timestamp': int(time.time() * 1000)
                        }
                logger.error(f"BitGet Orderbook API error for {symbol}")
                return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.error(f"Exception in BitGet.get_orderbook: {e}")
            return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}

    async def get_ticker(self, symbol: str) -> Dict:
        """Get 24h ticker data for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "00000" and data.get("data"):
                        ticker = data["data"]
                        return {
                            'last': float(ticker.get("close", 0)),
                            'bid': float(ticker.get("bestBid", 0)),
                            'ask': float(ticker.get("bestAsk", 0)),
                            'volume': float(ticker.get("baseVolume", 0)),
                            'timestamp': int(time.time() * 1000)
                        }
                logger.error(f"BitGet Ticker API error for {symbol}")
                return {
                    'last': 0,
                    'bid': 0,
                    'ask': 0,
                    'volume': 0,
                    'timestamp': int(time.time() * 1000)
                }
        except Exception as e:
            logger.error(f"Exception in BitGet.get_ticker: {e}")
            return {
                'last': 0,
                'bid': 0,
                'ask': 0,
                'volume': 0,
                'timestamp': int(time.time() * 1000)
            }