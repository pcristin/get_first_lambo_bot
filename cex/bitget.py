import requests
import hmac
import base64
import time
import json
from utils.logger import logger
from config import BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE

class BitGet:
    SPOT_API_URL = "https://api.bitget.com/api/spot/v1/market/ticker"
    FUTURES_API_URL = "https://api.bitget.com/api/mix/v1/market/ticker"
    COIN_INFO_API_URL = "https://api.bitget.com/api/spot/v1/public/currencies"

    def __init__(self):
        self.api_key = BITGET_API_KEY
        self.api_secret = BITGET_API_SECRET
        self.passphrase = BITGET_API_PASSPHRASE

    def _generate_signature(self, timestamp, method, request_path, body=''):
        message = str(timestamp) + str.upper(method) + request_path + (body or '')
        mac = hmac.new(
            bytes(self.api_secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        d = mac.digest()
        return base64.b64encode(d).decode()

    def get_spot_price(self, symbol):
        # Bitget symbol: e.g. "ALPHAOFSOLUSDT"
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("code") == "00000" and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("last", 0))
                logger.info(f"Bitget Spot Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"Bitget Spot API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bitget.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        # For futures, using USDT perpetual contract.
        # Format: "ALPHAOFSOLUSDT_UMCBL" (the suffix may vary by contract type)
        formatted_symbol = f"{symbol}USDT_UMCBL"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("code") == "00000" and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("last", 0))
                logger.info(f"Bitget Futures Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"Bitget Futures API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bitget.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using BitGet's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            timestamp = str(int(time.time() * 1000))
            request_path = "/api/spot/v1/public/currencies"
            
            signature = self._generate_signature(timestamp, "GET", request_path)
            
            headers = {
                "ACCESS-KEY": self.api_key,
                "ACCESS-SIGN": signature,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-PASSPHRASE": self.passphrase,
                "Content-Type": "application/json"
            }
            
            response = requests.get(
                self.COIN_INFO_API_URL,
                headers=headers,
                timeout=10
            )
            
            data = response.json()
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