import requests
import hmac
import base64
import json
import time
import aiohttp
from typing import Dict, List, Optional
from utils.logger import logger
from config import OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE
from .base import BaseCEX

class OKX(BaseCEX):
    SPOT_API_URL = "https://www.okx.com/api/v5/market/ticker"
    FUTURES_API_URL = "https://www.okx.com/api/v5/public/mark-price"
    CURRENCIES_API_URL = "https://www.okx.com/api/v5/asset/currencies"
    PRIVATE_API_URL = "https://www.okx.com/api/v5"

    def __init__(self):
        super().__init__()
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_API_SECRET
        self.api_passphrase = OKX_API_PASSPHRASE
        self.session = None

    @property
    def name(self) -> str:
        return "OKX"

    @property
    def market_rate_limit_key(self) -> str:
        return "okx_market"

    @property
    def private_rate_limit_key(self) -> str:
        return "okx_private"

    def _generate_signature(self, timestamp, method, request_path, body=''):
        if str(body) == '{}' or str(body) == 'None':
            body = ''
        message = str(timestamp) + str.upper(method) + request_path + str(body)
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
        instId = f"{symbol}-USDT"
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params={"instId": instId}) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        price = float(ticker.get("last", 0))
                        logger.info(f"OKX Spot Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"OKX Spot API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get OKX spot price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in OKX.get_spot_price: {e}")
            return None

    async def get_futures_price(self, symbol: str) -> Optional[float]:
        """Get futures price for a symbol"""
        await self._acquire_market_rate_limit()
        instId = f"{symbol}-USDT-SWAP"
        session = await self._get_session()
        
        try:
            async with session.get(self.FUTURES_API_URL, params={"instId": instId}) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        price = float(ticker.get("markPx", 0))
                        logger.info(f"OKX Futures Price for {symbol}: {price}")
                        return price
                    else:
                        logger.error(f"OKX Futures API error for {symbol}: {data}")
                        return None
                logger.error(f"Failed to get OKX futures price for {symbol}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Exception in OKX.get_futures_price: {e}")
            return None

    async def get_deposit_withdraw_info(self, symbol: str) -> Dict:
        """
        Gets deposit and withdrawal information for a token using OKX's private API.
        Returns a dictionary containing max withdrawal amount, deposit/withdrawal status, and withdrawal fees.
        """
        try:
            await self._acquire_private_rate_limit()
            timestamp = str(int(time.time() * 1000))
            
            # Get currency info
            currencies_path = "/api/v5/asset/currencies"
            
            signature = self._generate_signature(timestamp, "GET", currencies_path)
            headers = {
                "OK-ACCESS-KEY": self.api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.api_passphrase
            }
            
            session = await self._get_session()
            
            # Get currency info including withdrawal limits and chain info
            async with session.get(
                f"{self.PRIVATE_API_URL}{currencies_path}",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        # Find the currency info
                        currency_info = None
                        for curr in data["data"]:
                            if curr.get("ccy") == symbol:
                                currency_info = curr
                                break
                        
                        if currency_info:
                            # Get chain info
                            chains = currency_info.get("chains", [])
                            chain_info = next(
                                (chain for chain in chains if chain.get("chain") == "BSC"),
                                next(iter(chains), None) if chains else None
                            )
                            
                            if chain_info:
                                # Get withdrawal fee info
                                min_fee = chain_info.get("minFee", "N/A")
                                max_fee = chain_info.get("maxFee", min_fee)
                                fee_info = f"{min_fee}"
                                if max_fee != min_fee:
                                    fee_info += f"-{max_fee}"
                                
                                return {
                                    "max_volume": chain_info.get("maxWd", "N/A"),
                                    "deposit": "Enabled" if chain_info.get("canDep") == "1" else "Disabled",
                                    "withdraw": "Enabled" if chain_info.get("canWd") == "1" else "Disabled",
                                    "withdraw_fee": fee_info
                                }
                            else:
                                # If no chain info, use main currency status
                                return {
                                    "max_volume": currency_info.get("maxWd", "N/A"),
                                    "deposit": "Enabled" if currency_info.get("canDep") == "1" else "Disabled",
                                    "withdraw": "Enabled" if currency_info.get("canWd") == "1" else "Disabled",
                                    "withdraw_fee": "N/A"
                                }
                        
                        logger.error(f"OKX: Currency {symbol} not found in response")
                    else:
                        logger.error(f"OKX: Invalid response format for {symbol}: {data}")
                else:
                    logger.error(f"OKX API error: Status {response.status}")
            
            return {
                "max_volume": "N/A", 
                "deposit": "N/A", 
                "withdraw": "N/A",
                "withdraw_fee": "N/A"
            }
                
        except Exception as e:
            logger.error(f"Exception in OKX.get_deposit_withdraw_info: {e}")
            return {
                "max_volume": "N/A", 
                "deposit": "N/A", 
                "withdraw": "N/A",
                "withdraw_fee": "N/A"
            }

    async def get_futures_symbols(self) -> List[str]:
        """Get all available futures trading pairs"""
        await self._acquire_market_rate_limit()
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.FUTURES_API_URL}/instruments?instType=SWAP") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        symbols = []
                        for instrument in data["data"]:
                            if instrument.get("quoteCcy") == "USDT":
                                symbol = instrument.get("baseCcy")
                                if symbol:
                                    symbols.append(symbol)
                        logger.info(f"Found {len(symbols)} futures trading pairs on OKX")
                        return symbols
                logger.error("Failed to get OKX futures symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in OKX.get_futures_symbols: {e}")
            return []

    async def get_24h_volume(self, symbol: str) -> Optional[float]:
        """
        Get 24h trading volume for a symbol (combines spot and futures volume)
        Returns the total volume in USD
        """
        await self._acquire_market_rate_limit()
        spot_instId = f"{symbol}-USDT"
        futures_instId = f"{symbol}-USDT-SWAP"
        
        params_spot = {"instId": spot_instId, "instType": "SPOT"}
        params_futures = {"instId": futures_instId, "instType": "SWAP"}
        
        session = await self._get_session()
        total_volume = 0.0
        
        try:
            # Get spot volume
            async with session.get(f"{self.PRIVATE_API_URL}/market/ticker", params=params_spot) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        if "vol24h" in ticker and "last" in ticker:
                            spot_volume = float(ticker["vol24h"]) * float(ticker["last"])
                            total_volume += spot_volume
                            logger.info(f"OKX Spot 24h Volume for {symbol}: ${spot_volume:,.2f}")
                    else:
                        logger.warning(f"OKX Spot Volume API warning for {symbol}: {data.get('msg', 'Invalid response format')}")
                else:
                    logger.error(f"OKX Spot Volume API error for {symbol}: Status {response.status}")

            # Get futures volume
            async with session.get(f"{self.PRIVATE_API_URL}/market/ticker", params=params_futures) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        if "vol24h" in ticker and "last" in ticker:
                            futures_volume = float(ticker["vol24h"]) * float(ticker["last"])
                            total_volume += futures_volume
                            logger.info(f"OKX Futures 24h Volume for {symbol}: ${futures_volume:,.2f}")
                    else:
                        logger.warning(f"OKX Futures Volume API warning for {symbol}: {data.get('msg', 'Invalid response format')}")
                else:
                    logger.error(f"OKX Futures Volume API error for {symbol}: Status {response.status}")

            if total_volume > 0:
                logger.info(f"OKX Total 24h Volume for {symbol}: ${total_volume:,.2f}")
                return total_volume
            return None

        except aiohttp.ClientError as e:
            logger.error(f"Network error in OKX.get_24h_volume for {symbol}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in OKX.get_24h_volume for {symbol}: {str(e)}")
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
            async with session.get(f"{self.SPOT_API_URL}/instruments?instType=SPOT") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        symbols = []
                        for instrument in data["data"]:
                            if instrument.get("quoteCcy") == "USDT":
                                symbol = instrument.get("baseCcy")
                                if symbol:
                                    symbols.append(symbol)
                        logger.info(f"Found {len(symbols)} spot trading pairs on OKX")
                        return symbols
                logger.error("Failed to get OKX spot symbols")
                return []
        except Exception as e:
            logger.error(f"Exception in OKX.get_spot_symbols: {e}")
            return []

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Get order book for a symbol"""
        await self._acquire_market_rate_limit()
        instId = f"{symbol}-USDT"
        params = {"instId": instId, "sz": limit}
        session = await self._get_session()
        
        try:
            async with session.get(f"{self.PRIVATE_API_URL}/market/books", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        book = data["data"][0]
                        return {
                            'bids': [(float(price), float(amount)) for price, amount, *_ in book.get("bids", [])],
                            'asks': [(float(price), float(amount)) for price, amount, *_ in book.get("asks", [])],
                            'timestamp': int(book.get("ts", time.time() * 1000))
                        }
                logger.error(f"OKX Orderbook API error for {symbol}")
                return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.error(f"Exception in OKX.get_orderbook: {e}")
            return {'bids': [], 'asks': [], 'timestamp': int(time.time() * 1000)}

    async def get_ticker(self, symbol: str) -> Dict:
        """Get 24h ticker data for a symbol"""
        await self._acquire_market_rate_limit()
        instId = f"{symbol}-USDT"
        params = {"instId": instId}
        session = await self._get_session()
        
        try:
            async with session.get(self.SPOT_API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        return {
                            'last': float(ticker.get("last", 0)),
                            'bid': float(ticker.get("bidPx", 0)),
                            'ask': float(ticker.get("askPx", 0)),
                            'volume': float(ticker.get("vol24h", 0)),
                            'timestamp': int(ticker.get("ts", time.time() * 1000))
                        }
                logger.error(f"OKX Ticker API error for {symbol}")
                return {
                    'last': 0,
                    'bid': 0,
                    'ask': 0,
                    'volume': 0,
                    'timestamp': int(time.time() * 1000)
                }
        except Exception as e:
            logger.error(f"Exception in OKX.get_ticker: {e}")
            return {
                'last': 0,
                'bid': 0,
                'ask': 0,
                'volume': 0,
                'timestamp': int(time.time() * 1000)
            }