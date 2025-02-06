import requests
import hmac
import hashlib
import time
import json
from utils.logger import logger
from config import BYBIT_API_KEY, BYBIT_API_SECRET

class Bybit:
    SPOT_API_URL = "https://api.bybit.com/v5/market/tickers"
    FUTURES_API_URL = "https://api.bybit.com/v5/market/tickers"
    COIN_INFO_API_URL = "https://api.bybit.com/v5/asset/coin/query-info"

    def __init__(self):
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET

    def _generate_signature(self, params):
        timestamp = str(int(time.time() * 1000))
        param_str = timestamp + self.api_key + str(params)
        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return timestamp, signature

    def get_spot_price(self, symbol):
        formatted_symbol = f"{symbol}USDT"
        params = {
            "category": "spot",
            "symbol": formatted_symbol
        }
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                price = float(data["result"]["list"][0].get("lastPrice", 0))
                logger.info(f"Bybit Spot Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"Bybit Spot API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bybit.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        formatted_symbol = f"{symbol}USDT"
        params = {
            "category": "linear",
            "symbol": formatted_symbol
        }
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                price = float(data["result"]["list"][0].get("lastPrice", 0))
                logger.info(f"Bybit Futures Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"Bybit Futures API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in Bybit.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using Bybit's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            params = {
                "coin": symbol
            }
            
            timestamp, signature = self._generate_signature(params)
            
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": "5000"
            }
            
            response = requests.get(
                self.COIN_INFO_API_URL,
                params=params,
                headers=headers,
                timeout=10
            )
            
            data = response.json()
            if data.get("retCode") == 0 and data.get("result", {}).get("rows"):
                coin_info = data["result"]["rows"][0]
                chains = coin_info.get("chains", [])
                
                # Try to find BSC chain first, fall back to first available chain
                chain_info = next(
                    (chain for chain in chains if chain.get("chain") == "BSC"),
                    None
                )
                if not chain_info and chains:
                    chain_info = chains[0]
                
                if chain_info:
                    return {
                        "max_volume": chain_info.get("withdrawLimit", "N/A"),
                        "deposit": "Enabled" if chain_info.get("depositStatus") else "Disabled",
                        "withdraw": "Enabled" if chain_info.get("withdrawStatus") else "Disabled"
                    }
            
            logger.error(f"Bybit: Failed to get currency info for {symbol}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
            
        except Exception as e:
            logger.error(f"Exception in Bybit.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}