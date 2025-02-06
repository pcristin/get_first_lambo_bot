import requests
import hmac
import hashlib
import time
from utils.logger import logger
from config import BINANCE_API_KEY, BINANCE_API_SECRET

class Binance:
    SPOT_API_URL = "https://api.binance.com/api/v3/ticker/price"
    FUTURES_API_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
    CAPITAL_API_URL = "https://api.binance.com/sapi/v1/capital/config/getall"

    def __init__(self):
        self.api_key = BINANCE_API_KEY
        self.api_secret = BINANCE_API_SECRET

    def _generate_signature(self, params):
        query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def get_spot_price(self, symbol):
        # Binance symbol format is e.g. BTCUSDT (no underscore)
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            price = float(data.get("price", 0))
            logger.info(f"Binance Spot Price for {symbol}: {price}")
            return price
        except Exception as e:
            logger.error(f"Exception in Binance.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        formatted_symbol = f"{symbol}USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
            price = float(data.get("price", 0))
            logger.info(f"Binance Futures Price for {symbol}: {price}")
            return price
        except Exception as e:
            logger.error(f"Exception in Binance.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
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
            response = requests.get(
                self.CAPITAL_API_URL,
                params=params,
                headers=headers,
                timeout=10
            )
            
            data = response.json()
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
                
        except Exception as e:
            logger.error(f"Exception in Binance.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}