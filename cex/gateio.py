import requests
import hmac
import hashlib
import time
from utils.logger import logger
from config import GATEIO_API_KEY, GATEIO_API_SECRET

class GateIO:
    SPOT_API_URL = "https://api.gateio.ws/api/v4/spot/tickers"
    FUTURES_API_URL = "https://fx-api.gateio.ws/api/v4/futures/tickers"
    CURRENCY_API_URL = "https://api.gateio.ws/api/v4/spot/currencies"

    def __init__(self):
        self.api_key = GATEIO_API_KEY
        self.api_secret = GATEIO_API_SECRET

    def _generate_signature(self, method, url, query_string='', body=''):
        t = time.time()
        m = hashlib.sha512()
        m.update((query_string + body).encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string, hashed_payload, t)
        sign = hmac.new(self.api_secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': self.api_key, 'Timestamp': str(t), 'SIGN': sign}

    def get_spot_price(self, symbol):
        # Gate.io currency pair: e.g. "ALPHAOFSOL_USDT"
        currency_pair = f"{symbol}_USDT"
        try:
            response = requests.get(self.SPOT_API_URL, timeout=10)
            data = response.json()
            for ticker in data:
                if ticker.get("currency_pair") == currency_pair:
                    price = float(ticker.get("last", 0))
                    logger.info(f"Gate.io Spot Price for {symbol}: {price}")
                    return price
            logger.error(f"Gate.io Spot: Ticker for {symbol} not found.")
            return None
        except Exception as e:
            logger.error(f"Exception in GateIO.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        # Futures ticker for Gate.io: using the same currency pair format.
        currency_pair = f"{symbol}_USDT"
        try:
            response = requests.get(self.FUTURES_API_URL, timeout=10)
            data = response.json()
            for ticker in data:
                if ticker.get("currency_pair") == currency_pair:
                    price = float(ticker.get("last", 0))
                    logger.info(f"Gate.io Futures Price for {symbol}: {price}")
                    return price
            logger.error(f"Gate.io Futures: Ticker for {symbol} not found.")
            return None
        except Exception as e:
            logger.error(f"Exception in GateIO.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Gets deposit and withdrawal information for a token using Gate.io's private API.
        Returns a dictionary containing max withdrawal amount and deposit/withdrawal status.
        """
        try:
            url = f"/api/v4/spot/currencies/{symbol}"
            headers = self._generate_signature("GET", url)
            
            response = requests.get(
                f"{self.CURRENCY_API_URL}/{symbol}",
                headers=headers,
                timeout=10
            )
            
            data = response.json()
            if isinstance(data, dict):
                chains = data.get("chains", [])
                
                # Try to find BSC chain first, fall back to first available chain
                chain_info = next((chain for chain in chains if chain.get("chain") == "BSC"), None)
                if not chain_info and chains:
                    chain_info = chains[0]
                
                if chain_info:
                    return {
                        "max_volume": chain_info.get("withdraw_max", "N/A"),
                        "deposit": "Enabled" if chain_info.get("deposit_status") == "enable" else "Disabled",
                        "withdraw": "Enabled" if chain_info.get("withdraw_status") == "enable" else "Disabled"
                    }
            
            logger.error(f"Gate.io: Failed to get currency info for {symbol}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
            
        except Exception as e:
            logger.error(f"Exception in GateIO.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}