import aiohttp
import time
import hashlib
import hmac
from typing import Dict, List, Optional
from utils.logger import logger
from config import MEXC_API_KEY, MEXC_API_SECRET
from .base import BaseCEX

class MEXC(BaseCEX):
    SPOT_API_URL = "https://api.mexc.com/api/v3/ticker/24hr"
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
        """
        Gets deposit and withdrawal information for a token using MEXC's API.
        Returns a dictionary containing max withdrawal amount, deposit/withdrawal status,
        withdrawal fees and chain information.
        
        API Docs: https://mexcdevelop.github.io/apidocs/spot_v3_en/#exchange-information
        """
        try:
            await self._acquire_private_rate_limit()
            
            session = await self._get_session()
            
            # First get the coin info from exchange information endpoint
            async with session.get(
                "https://api.mexc.com/api/v3/exchangeInfo",
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
                            # Get withdrawal info from capital config endpoint
                            timestamp = str(int(time.time() * 1000))
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
                            
                            async with session.get(
                                f"{self.PRIVATE_API_URL}/api/v3/capital/config/getall",
                                params=params,
                                headers=headers
                            ) as withdraw_response:
                                if withdraw_response.status == 200:
                                    withdraw_data = await withdraw_response.json()
                                    coin_info = next(
                                        (coin for coin in withdraw_data if coin.get("coin") == symbol),
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
                
                logger.error(f"MEXC: Failed to get currency info for {symbol}")
                return {
                    "max_volume": "N/A",
                    "deposit": "N/A",
                    "withdraw": "N/A",
                    "withdraw_fee": "N/A",
                    "chain": "N/A"
                }
                
        except Exception as e:
            logger.error(f"Exception in MEXC.get_deposit_withdraw_info: {e}")
            return {
                "max_volume": "N/A",
                "deposit": "N/A",
                "withdraw": "N/A",
                "withdraw_fee": "N/A",
                "chain": "N/A"
            }

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
        """Get 24h trading volume for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDT"  # MEXC v3 uses no underscore
        params = {"symbol": formatted_symbol}
        
        try:
            session = await self._get_session()
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Handle both single object and array response formats
                    ticker_data = data[0] if isinstance(data, list) else data
                    
                    if "volume" in ticker_data:
                        # Get volume in base currency and multiply by last price for USDT volume
                        base_volume = float(ticker_data["volume"] or 0)
                        last_price = float(ticker_data.get("lastPrice", 0))
                        volume = base_volume * last_price
                        logger.info(f"MEXC 24h Volume for {symbol}: ${volume:,.2f}")
                        return volume
                    
                    logger.error(f"MEXC Volume API error for {symbol}: Invalid response format")
                    return None
                logger.error(f"MEXC Volume API error for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_24h_volume: {e}")
            return None

    async def get_spot_symbols(self) -> List[str]:
        """Get all available spot trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    
                    # Handle response as a dict if available
                    if isinstance(data, dict) and data.get("success") and data.get("data"):
                        for ticker in data["data"]:
                            symbol = ticker.get("symbol", "")
                            if symbol.endswith("_USDT"):
                                base_symbol = symbol.replace("_USDT", "")
                                symbols.append(base_symbol)
                    # Otherwise assume response is a list of tickers
                    elif isinstance(data, list):
                        for ticker in data:
                            symbol = ticker.get("symbol", "")
                            if symbol.endswith("USDT"):  # Note: MEXC v3 uses no underscore
                                base_symbol = symbol.replace("USDT", "")
                                symbols.append(base_symbol)
                                
                    logger.info(f"Found {len(symbols)} spot trading pairs on MEXC")
                    return symbols
                    
                logger.error(f"Failed to get MEXC spot symbols: Status {response.status}")
                return []
        except Exception as e:
            logger.error(f"Exception in MEXC.get_spot_symbols: {e}")
            return []

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Get order book for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol, "limit": limit}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.PRIVATE_API_URL}/api/v3/depth", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("bids") and data.get("asks"):
                        return {
                            'bids': [(float(price), float(amount)) for price, amount in data["bids"]],
                            'asks': [(float(price), float(amount)) for price, amount in data["asks"]],
                            'timestamp': int(time.time() * 1000)
                        }
                logger.error(f"MEXC Orderbook API error for {symbol}")
                return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.error(f"Exception in MEXC.get_orderbook: {e}")
            return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}

    async def get_ticker(self, symbol: str) -> Dict:
        """Get 24h ticker data for a symbol"""
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
                        return {
                            'last': float(ticker.get("last", 0)),
                            'bid': float(ticker.get("bid", 0)),
                            'ask': float(ticker.get("ask", 0)),
                            'volume': float(ticker.get("volume", 0)),
                            'timestamp': int(time.time() * 1000)
                        }
                logger.error(f"MEXC Ticker API error for {symbol}")
                return {
                    'last': 0,
                    'bid': 0,
                    'ask': 0,
                    'volume': 0,
                    'timestamp': int(time.time() * 1000)
                }
        except Exception as e:
            logger.error(f"Exception in MEXC.get_ticker: {e}")
            return {
                'last': 0,
                'bid': 0,
                'ask': 0,
                'volume': 0,
                'timestamp': int(time.time() * 1000)
            }