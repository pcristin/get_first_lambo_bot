import requests
import hmac
import hashlib
import base64
import time
from utils.logger import logger
from config import KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE
from .base import BaseCEX
import aiohttp
from typing import List, Optional, Dict

class KuCoin(BaseCEX):
    SPOT_API_URL = "https://api.kucoin.com/api/v1/market/orderbook/level1"
    FUTURES_API_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
    CURRENCIES_API_URL = "https://api.kucoin.com/api/v1/currencies"
    PRIVATE_API_URL = "https://api.kucoin.com"

    def __init__(self):
        super().__init__()
        self.api_key = KUCOIN_API_KEY
        self.api_secret = KUCOIN_API_SECRET
        self.api_passphrase = KUCOIN_API_PASSPHRASE
        self.session = None

    @property
    def name(self) -> str:
        return "KuCoin"

    @property
    def market_rate_limit_key(self) -> str:
        return "kucoin_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "kucoin_private"

    def _generate_signature(self, timestamp, method, endpoint, body=''):
        str_to_sign = f"{timestamp}{method}{endpoint}{body}"
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
        passphrase = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                self.api_passphrase.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
        return signature, passphrase

    async def get_spot_price(self, symbol: str) -> Optional[float]:
        """Get spot price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}-USDT"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data"):
                        price = float(data["data"].get("price", 0))
                        logger.info(f"KuCoin Spot Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"KuCoin Spot API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get KuCoin spot price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}USDTM"
        params = {"symbol": formatted_symbol}
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data"):
                        contract = next(
                            (item for item in data["data"] 
                             if item.get("symbol") == f"XBT{formatted_symbol}"), None
                        )
                        if contract:
                            price = float(contract.get("markPrice", 0))
                            logger.info(f"KuCoin Futures Price for {symbol}: {price}")
                            return price
                    logger.error(f"KuCoin Futures: Contract for {formatted_symbol} not found")
                    return None
                logger.error(f"Failed to get KuCoin futures price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using KuCoin's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            await self._acquire_private_rate_limit()
            timestamp = str(int(time.time() * 1000))
            endpoint = f"/api/v1/currencies/{symbol}"
            signature, passphrase = self._generate_signature(timestamp, "GET", endpoint)
            
            headers = {
                "KC-API-KEY": self.api_key,
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase,
                "KC-API-KEY-VERSION": "2"
            }
            
            session = await self._get_session()
            
            async with session.get(
                f"{self.CURRENCIES_API_URL}/{symbol}",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data"):
                        currency_data = data["data"]
                        chains = currency_data.get("chains", [])
                        
                        # Try to find BSC chain first, fall back to first available chain
                        chain_info = next((chain for chain in chains if chain.get("chainName") == "BSC"), None)
                        if not chain_info and chains:
                            chain_info = chains[0]
                        
                        if chain_info:
                            return {
                                "max_volume": chain_info.get("withdrawalMinSize", "N/A"),
                                "deposit": "Enabled" if chain_info.get("isDepositEnabled", False) else "Disabled",
                                "withdraw": "Enabled" if chain_info.get("isWithdrawEnabled", False) else "Disabled"
                            }
                
                logger.error(f"KuCoin: Failed to get currency info for {symbol}")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
                
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data"):
                        symbols = []
                        for contract in data["data"]:
                            if contract.get("quoteCoin") == "USDT":
                                symbol = contract.get("baseCoin")
                                if symbol:
                                    symbols.append(symbol)
                        logger.info(f"Found {len(symbols)} futures trading pairs on KuCoin")
                        return symbols
                logger.error("Failed to get KuCoin futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """Get 24h trading volume for a symbol"""
        await self._acquire_market_rate_limit()
        formatted_symbol = f"{symbol}-USDT"
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.PRIVATE_API_URL}/api/v1/market/allTickers") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data", {}).get("ticker"):
                        # Find the ticker for our symbol
                        ticker = next(
                            (t for t in data["data"]["ticker"] if t.get("symbol") == formatted_symbol),
                            None
                        )
                        if ticker and "volValue" in ticker:
                            volume = float(ticker["volValue"])  # Already in USDT
                            logger.info(f"KuCoin 24h Volume for {symbol}: ${volume:,.2f}")
                            return volume
                    logger.error(f"KuCoin Volume API error for {symbol}: Ticker not found")
                    return None
                logger.error(f"KuCoin Volume API error for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_24h_volume: {e}")
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
            async with session.get(f"{self.SPOT_API_URL}/symbols") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "200000" and data.get("data"):
                        symbols = []
                        for symbol_data in data["data"]:
                            if symbol_data.get("quoteCurrency") == "USDT" and symbol_data.get("enableTrading"):
                                base_symbol = symbol_data.get("baseCurrency")
                                if base_symbol:
                                    symbols.append(base_symbol)
                        logger.info(f"Found {len(symbols)} spot trading pairs on KuCoin")
                        return symbols
                logger.error("Failed to get KuCoin spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_spot_symbols: {e}")
            return []