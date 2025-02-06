import requests
import hmac
import hashlib
import time
import aiohttp
from typing import Dict, List, Optional
from utils.logger import logger
from config import BINANCE_API_KEY, BINANCE_API_SECRET
from .base import BaseCEX

class Binance(BaseCEX):
    SPOT_API_URL = "https://api.binance.com/api/v3"
    FUTURES_API_URL = "https://fapi.binance.com/fapi/v1"
    PRIVATE_API_URL = "https://api.binance.com"
    CAPITAL_API_URL = "https://api.binance.com/sapi/v1/capital/config/getall"

    def __init__(self):
        super().__init__()
        self.api_key = BINANCE_API_KEY
        self.api_secret = BINANCE_API_SECRET
        self.session = None

    @property
    def name(self) -> str:
        return "Binance"

    @property
    def market_rate_limit_key(self) -> str:
        return "binance_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "binance_private"

    def _generate_signature(self, params):
        query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.SPOT_API_URL}/ticker/price", params={"symbol": formatted_symbol}) as response:
                if response.status == 200:
                    data = await response.json()
                    price = float(data.get("price", 0))
                    logger.info(f"Binance Spot Price for {symbol}: {price}")
                    return price
                logger.error(f"Failed to get Binance spot price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Binance.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.FUTURES_API_URL}/ticker/price", params={"symbol": formatted_symbol}) as response:
                if response.status == 200:
                    data = await response.json()
                    price = float(data.get("price", 0))
                    logger.info(f"Binance Futures Price for {symbol}: {price}")
                    return price
                logger.error(f"Failed to get Binance futures price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in Binance.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using Binance's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            timestamp = int(time.time() * 1000)
            params = {
                "timestamp": timestamp,
            }
            
            signature = self._generate_signature(params)
            headers = {
                "X-MBX-APIKEY": self.api_key
            }
            
            params["signature"] = signature
            session = await self._get_session()
            
            async with session.get(
                self.CAPITAL_API_URL,
                params=params,
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    coin_info = next((coin for coin in data if coin["coin"] == symbol), None)
                    
                    if coin_info:
                        network_info = next((network for network in coin_info.get("networkList", []) 
                                      if network.get("network") == "BSC"), None)
                        if not network_info:
                            network_info = coin_info.get("networkList", [{}])[0]
                        
                        return {
                            "max_volume": network_info.get("withdrawMax", "N/A"),
                            "deposit": "Enabled" if network_info.get("depositEnable", False) else "Disabled",
                            "withdraw": "Enabled" if network_info.get("withdrawEnable", False) else "Disabled"
                        }
                    else:
                        logger.error(f"Binance: Token {symbol} not found in capital config")
                        return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
                else:
                    logger.error(f"Binance API error: {response.status}")
                    return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
                    
        except Exception as e:
            logger.error(f"Exception in Binance.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.FUTURES_API_URL}/exchangeInfo") as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for symbol in data.get("symbols", []):
                        if symbol.get("quoteAsset") == "USDT" and symbol.get("status") == "TRADING":
                            base_symbol = symbol.get("baseAsset")
                            if base_symbol:
                                symbols.append(base_symbol)
                    logger.info(f"Found {len(symbols)} futures trading pairs on Binance")
                    return symbols
                logger.error("Failed to get Binance futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in Binance.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """
        Get 24h trading volume for a symbol (combines spot and futures volume)
        Returns the total volume in USD
        """
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        session = await self._get_session()
        total_volume = 0.0
        
        try:
            # Get spot volume
            try:
                async with session.get(f"{self.SPOT_API_URL}/ticker/24hr", params={"symbol": formatted_symbol}) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, dict) and "quoteVolume" in data:
                            spot_volume = float(data["quoteVolume"])  # Already in USDT
                            total_volume += spot_volume
                            logger.info(f"Binance Spot 24h Volume for {symbol}: ${spot_volume:,.2f}")
                        else:
                            logger.debug(f"Binance Spot Volume API warning for {symbol}: Invalid response format")
                    elif response.status == 400:
                        logger.debug(f"Binance Spot Volume API: {symbol} not available")
                    else:
                        logger.error(f"Binance Spot Volume API error for {symbol}: Status {response.status}")
            except Exception as e:
                logger.error(f"Error getting Binance spot volume for {symbol}: {str(e)}")

            # Get futures volume
            try:
                async with session.get(f"{self.FUTURES_API_URL}/ticker/24hr", params={"symbol": formatted_symbol}) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, dict) and "quoteVolume" in data:
                            futures_volume = float(data["quoteVolume"])  # Already in USDT
                            total_volume += futures_volume
                            logger.info(f"Binance Futures 24h Volume for {symbol}: ${futures_volume:,.2f}")
                        else:
                            logger.debug(f"Binance Futures Volume API warning for {symbol}: Invalid response format")
                    elif response.status == 400:
                        logger.debug(f"Binance Futures Volume API: {symbol} not available")
                    else:
                        logger.error(f"Binance Futures Volume API error for {symbol}: Status {response.status}")
            except Exception as e:
                logger.error(f"Error getting Binance futures volume for {symbol}: {str(e)}")

            if total_volume > 0:
                logger.info(f"Binance Total 24h Volume for {symbol}: ${total_volume:,.2f}")
                return total_volume
            return None

        except aiohttp.ClientError as e:
            logger.error(f"Network error in Binance.get_24h_volume for {symbol}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in Binance.get_24h_volume for {symbol}: {str(e)}")
            return None
        finally:
            pass  # Don't close the session here as it's managed by the class

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
            async with session.get(f"{self.SPOT_API_URL}/exchangeInfo") as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for symbol in data.get("symbols", []):
                        if symbol.get("quoteAsset") == "USDT" and symbol.get("status") == "TRADING":
                            base_symbol = symbol.get("baseAsset")
                            if base_symbol:
                                symbols.append(base_symbol)
                    logger.info(f"Found {len(symbols)} spot trading pairs on Binance")
                    return symbols
                logger.error("Failed to get Binance spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in Binance.get_spot_symbols: {e}")
            return []

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Get order book for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol, "limit": limit}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.SPOT_API_URL}/depth", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("bids") and data.get("asks"):
                        return {
                            'bids': [(float(price), float(amount)) for price, amount in data["bids"]],
                            'asks': [(float(price), float(amount)) for price, amount in data["asks"]],
                            'timestamp': data.get("lastUpdateId", int(time.time() * 1000))
                        }
                logger.error(f"Binance Orderbook API error for {symbol}")
                return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.error(f"Exception in Binance.get_orderbook: {e}")
            return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}

    async def get_ticker(self, symbol: str) -> Dict:
        """Get 24h ticker data for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.SPOT_API_URL}/ticker/24hr", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'last': float(data.get("lastPrice", 0)),
                        'bid': float(data.get("bidPrice", 0)),
                        'ask': float(data.get("askPrice", 0)),
                        'volume': float(data.get("volume", 0)),
                        'timestamp': data.get("closeTime", int(time.time() * 1000))
                    }
                logger.error(f"Binance Ticker API error for {symbol}")
                return {
                    'last': 0,
                    'bid': 0,
                    'ask': 0,
                    'volume': 0,
                    'timestamp': int(time.time() * 1000)
                }
        except Exception as e:
            logger.error(f"Exception in Binance.get_ticker: {e}")
            return {
                'last': 0,
                'bid': 0,
                'ask': 0,
                'volume': 0,
                'timestamp': int(time.time() * 1000)
            }