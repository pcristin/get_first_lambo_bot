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
        Gets deposit and withdrawal information for a token using Gate.io's API.
        Returns a dictionary containing max withdrawal amount, deposit/withdrawal status,
        withdrawal fees and chain information.
        
        API Docs: https://www.gate.io/docs/developers/apiv4/#get-details-of-a-specific-currency
        """
        try:
            await self._acquire_private_rate_limit()
            url = f"/api/v4/currencies/{symbol}"
            headers = self._generate_signature("GET", url)
            session = await self._get_session()
            
            async with session.get(
                f"{self.PRIVATE_API_URL}{url}",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict):
                        chains = data.get("chains", [])
                        
                        # Try to find BSC chain first, fall back to first available chain
                        chain_info = next(
                            (chain for chain in chains if chain.get("chain_name", "").upper() == "BSC"),
                            next((chain for chain in chains if chain.get("is_deposit_enabled")), None)
                        )
                        
                        if chain_info:
                            # Get withdrawal limits
                            min_withdraw = chain_info.get("withdraw_limit_min", "N/A")
                            max_withdraw = chain_info.get("withdraw_limit_max", "N/A")
                            
                            # Format max volume as range if both min and max are available
                            max_volume = f"{min_withdraw}-{max_withdraw}" if min_withdraw != "N/A" and max_withdraw != "N/A" else max_withdraw
                            
                            # Get withdrawal fee
                            withdraw_fee = chain_info.get("withdraw_fix_fee", "N/A")
                            withdraw_fee_percent = chain_info.get("withdraw_percent_fee", "0")
                            
                            # Format withdrawal fee string
                            if withdraw_fee != "N/A" and float(withdraw_fee_percent) > 0:
                                fee_str = f"{withdraw_fee} + {float(withdraw_fee_percent) * 100}%"
                            else:
                                fee_str = withdraw_fee
                            
                            return {
                                "max_volume": max_volume,
                                "deposit": "Enabled" if chain_info.get("is_deposit_enabled") else "Disabled",
                                "withdraw": "Enabled" if chain_info.get("is_withdraw_enabled") else "Disabled",
                                "withdraw_fee": fee_str,
                                "chain": chain_info.get("chain_name", "N/A")
                            }
                
                logger.error(f"Gate.io: Failed to get currency info for {symbol}")
                return {
                    "max_volume": "N/A",
                    "deposit": "N/A",
                    "withdraw": "N/A",
                    "withdraw_fee": "N/A",
                    "chain": "N/A"
                }
                
        except Exception as e:
            logger.error(f"Exception in GateIO.get_deposit_withdraw_info: {e}")
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

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Get order book for a symbol"""
        await self._acquire_market_rate_limit()
        currency_pair = f"{symbol}_USDT"
        params = {"currency_pair": currency_pair, "limit": limit}
        session = await self._get_session()
        
        try:
            async with session.get("https://api.gateio.ws/api/v4/spot/order_book", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("bids") and data.get("asks"):
                        return {
                            'bids': [(float(price), float(amount)) for price, amount in data["bids"]],
                            'asks': [(float(price), float(amount)) for price, amount in data["asks"]],
                            'timestamp': int(time.time() * 1000)
                        }
                logger.error(f"Gate.io Orderbook API error for {symbol}")
                return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.error(f"Exception in GateIO.get_orderbook: {e}")
            return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}

    async def get_ticker(self, symbol: str) -> Dict:
        """Get 24h ticker data for a symbol"""
        await self._acquire_market_rate_limit()
        currency_pair = f"{symbol}_USDT"
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        if ticker.get("currency_pair") == currency_pair:
                            return {
                                'last': float(ticker.get("last", 0)),
                                'bid': float(ticker.get("highest_bid", 0)),
                                'ask': float(ticker.get("lowest_ask", 0)),
                                'volume': float(ticker.get("base_volume", 0)),
                                'timestamp': int(time.time() * 1000)
                            }
                    logger.error(f"Gate.io: Ticker for {symbol} not found")
                    return {
                        'last': 0,
                        'bid': 0,
                        'ask': 0,
                        'volume': 0,
                        'timestamp': int(time.time() * 1000)
                    }
                logger.error(f"Gate.io Ticker API error for {symbol}")
                return {
                    'last': 0,
                    'bid': 0,
                    'ask': 0,
                    'volume': 0,
                    'timestamp': int(time.time() * 1000)
                }
        except Exception as e:
            logger.error(f"Exception in GateIO.get_ticker: {e}")
            return {
                'last': 0,
                'bid': 0,
                'ask': 0,
                'volume': 0,
                'timestamp': int(time.time() * 1000)
            }