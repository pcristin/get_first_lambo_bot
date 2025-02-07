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

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using Binance's API.
        Returns a dictionary containing max withdrawal amount, deposit/withdrawal status,
        withdrawal fees and chain information.
        
        API Docs: 
        - https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints#exchange-information
        - https://binance-docs.github.io/apidocs/spot/en/#all-coins-39-information-user_data
        """
        try:
            await self._acquire_private_rate_limit()
            timestamp = str(int(time.time() * 1000))
            
            session = await self._get_session()
            
            # First get the symbol info from exchange information
            async with session.get(
                f"{self.SPOT_API_URL}/exchangeInfo",
                params={"symbol": f"{symbol}USDT"}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if "symbols" in data:
                        symbol_info = next(
                            (s for s in data["symbols"] if s.get("baseAsset") == symbol),
                            None
                        )
                        
                        if symbol_info:
                            # Now get withdrawal info from capital config
                            params = {
                                "timestamp": timestamp,
                                "recvWindow": 5000
                            }
                            
                            signature = self._generate_signature(params)
                            headers = {
                                "X-MBX-APIKEY": self.api_key
                            }
                            
                            params["signature"] = signature
                            
                            async with session.get(
                                self.CAPITAL_API_URL,
                                params=params,
                                headers=headers
                            ) as capital_response:
                                if capital_response.status == 200:
                                    capital_data = await capital_response.json()
                                    coin_info = next(
                                        (coin for coin in capital_data if coin.get("coin") == symbol),
                                        None
                                    )
                                    
                                    if coin_info:
                                        networks = coin_info.get("networkList", [])
                                        
                                        # Try to find BSC chain first, fall back to first available chain
                                        network_info = next(
                                            (net for net in networks if "BSC" in net.get("network", "").upper()),
                                            next((net for net in networks if net.get("depositEnable")), networks[0] if networks else None)
                                        )
                                        
                                        if network_info:
                                            # Get withdrawal limits
                                            min_withdraw = network_info.get("withdrawMin", "N/A")
                                            max_withdraw = network_info.get("withdrawMax", "N/A")
                                            
                                            # Format max volume as range if both min and max are available
                                            max_volume = f"{min_withdraw}-{max_withdraw}" if min_withdraw != "N/A" and max_withdraw != "N/A" else max_withdraw
                                            
                                            # Get withdrawal fee
                                            withdraw_fee = network_info.get("withdrawFee", "N/A")
                                            withdraw_fee_percent = network_info.get("withdrawIntegerMultiple", "0")
                                            
                                            # Format withdrawal fee string
                                            if withdraw_fee != "N/A" and float(withdraw_fee_percent) > 0:
                                                fee_str = f"{withdraw_fee} + {float(withdraw_fee_percent) * 100}%"
                                            else:
                                                fee_str = withdraw_fee
                                            
                                            return {
                                                "max_volume": max_volume,
                                                "deposit": "Enabled" if network_info.get("depositEnable") else "Disabled",
                                                "withdraw": "Enabled" if network_info.get("withdrawEnable") else "Disabled",
                                                "withdraw_fee": fee_str,
                                                "chain": network_info.get("network", "N/A")
                                            }
                
                logger.error(f"Binance: Failed to get currency info for {symbol}")
                return {
                    "max_volume": "N/A",
                    "deposit": "N/A",
                    "withdraw": "N/A",
                    "withdraw_fee": "N/A",
                    "chain": "N/A"
                }
                
        except Exception as e:
            logger.error(f"Exception in Binance.get_deposit_withdraw_info: {e}")
            return {
                "max_volume": "N/A",
                "deposit": "N/A",
                "withdraw": "N/A",
                "withdraw_fee": "N/A",
                "chain": "N/A"
            }

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