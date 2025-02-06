import requests
import hmac
import hashlib
import base64
import time
from utils.logger import logger
from config import KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE

class KuCoin:
    SPOT_API_URL = "https://api.kucoin.com/api/v1/market/orderbook/level1"
    FUTURES_API_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
    CURRENCIES_API_URL = "https://api.kucoin.com/api/v1/currencies"

    def __init__(self):
        self.api_key = KUCOIN_API_KEY
        self.api_secret = KUCOIN_API_SECRET
        self.api_passphrase = KUCOIN_API_PASSPHRASE

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

    def get_spot_price(self, symbol):
        # KuCoin spot symbol format: "ALPHAOFSOL-USDT"
        formatted_symbol = f"{symbol}-USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("code") == "200000" and data.get("data"):
                price = float(data["data"].get("price", 0))
                logger.info(f"KuCoin Spot Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"KuCoin Spot API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        # KuCoin futures symbol format: e.g. "ALPHAOFSOLUSDTM" (no dash)
        formatted_symbol = f"{symbol}USDTM"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
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
        except Exception as e:
            logger.error(f"Exception in KuCoin.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using KuCoin's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
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
            
            response = requests.get(
                f"{self.CURRENCIES_API_URL}/{symbol}",
                headers=headers,
                timeout=10
            )
            
            data = response.json()
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