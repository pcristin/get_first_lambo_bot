import requests
import hmac
import base64
import json
import time
from utils.logger import logger
from config import OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE

class OKX:
    SPOT_API_URL = "https://www.okx.com/api/v5/market/ticker"
    FUTURES_API_URL = "https://www.okx.com/api/v5/public/mark-price"
    CURRENCIES_API_URL = "https://www.okx.com/api/v5/asset/currencies"

    def __init__(self):
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_API_SECRET
        self.passphrase = OKX_API_PASSPHRASE

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

    def get_spot_price(self, symbol):
        # OKX spot instId: e.g. "ALPHAOFSOL-USDT"
        instId = f"{symbol}-USDT"
        params = {"instId": instId}
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("code") == "0" and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("last", 0))
                logger.info(f"OKX Spot Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"OKX Spot API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in OKX.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        # OKX perpetual swap instId: e.g. "ALPHAOFSOL-USDT-SWAP"
        instId = f"{symbol}-USDT-SWAP"
        params = {"instId": instId}
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("code") == "0" and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("markPx", 0))
                logger.info(f"OKX Futures Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"OKX Futures API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in OKX.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using OKX's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            timestamp = str(int(time.time() * 1000))
            request_path = "/api/v5/asset/currencies"
            
            signature = self._generate_signature(timestamp, "GET", request_path)
            
            headers = {
                "OK-ACCESS-KEY": self.api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.passphrase,
                "x-simulated-trading": "0"
            }
            
            response = requests.get(
                self.CURRENCIES_API_URL,
                headers=headers,
                timeout=10
            )
            
            data = response.json()
            if data.get("code") == "0" and data.get("data"):
                # Find the currency info
                currency_info = next(
                    (curr for curr in data["data"] if curr.get("ccy") == symbol),
                    None
                )
                
                if currency_info:
                    # Try to find BSC chain first, fall back to first available chain
                    chain_info = next(
                        (chain for chain in currency_info.get("chains", [])
                         if chain.get("chain") == "BSC"),
                        None
                    )
                    if not chain_info and currency_info.get("chains"):
                        chain_info = currency_info["chains"][0]
                    
                    if chain_info:
                        return {
                            "max_volume": chain_info.get("maxWd", "N/A"),
                            "deposit": "Enabled" if chain_info.get("canDep") == "1" else "Disabled",
                            "withdraw": "Enabled" if chain_info.get("canWd") == "1" else "Disabled"
                        }
            
            logger.error(f"OKX: Failed to get currency info for {symbol}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
            
        except Exception as e:
            logger.error(f"Exception in OKX.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}