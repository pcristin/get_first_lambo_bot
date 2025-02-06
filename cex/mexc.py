import requests
from bs4 import BeautifulSoup
from utils.logger import logger

class MEXC:
    SPOT_API_URL = "https://www.mexc.com/open/api/v2/market/ticker"
    FUTURES_API_URL = "https://contract.mexc.com/api/v1/contract/ticker"

    def get_spot_price(self, symbol):
        # MEXC expects the symbol in the format: ALPHAOFSOL_USDT
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.SPOT_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("success") and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("last", 0))
                logger.info(f"MEXC Spot Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"MEXC Spot API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_spot_price: {e}")
            return None

    def get_futures_price(self, symbol):
        # Futures symbol format: ALPHAOFSOL_USDT
        formatted_symbol = f"{symbol}_USDT"
        params = {"symbol": formatted_symbol}
        try:
            response = requests.get(self.FUTURES_API_URL, params=params, timeout=10)
            data = response.json()
            if data.get("success") and data.get("data"):
                ticker = data["data"][0]
                price = float(ticker.get("last", 0))
                logger.info(f"MEXC Futures Price for {symbol}: {price}")
                return price
            else:
                logger.error(f"MEXC Futures API error for {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"Exception in MEXC.get_futures_price: {e}")
            return None

    def get_deposit_withdraw_info(self, symbol):
        """
        Scrapes the MEXC website for deposit/withdrawal info.
        Adjust the HTML selectors below according to the actual page structure.
        """
        url = f"https://www.mexc.com/exchange/coin-detail/{symbol}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                max_volume_elem = soup.find("div", class_="max-volume")
                deposit_elem = soup.find("span", class_="deposit-status")
                withdraw_elem = soup.find("span", class_="withdraw-status")
                max_volume = max_volume_elem.text.strip() if max_volume_elem else "N/A"
                deposit_status = deposit_elem.text.strip() if deposit_elem else "N/A"
                withdraw_status = withdraw_elem.text.strip() if withdraw_elem else "N/A"
                info = {
                    "max_volume": max_volume,
                    "deposit": deposit_status,
                    "withdraw": withdraw_status,
                }
                logger.info(f"MEXC deposit/withdraw info for {symbol}: {info}")
                return info
            else:
                logger.error(f"Failed to fetch deposit/withdraw page for {symbol} on MEXC")
                return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}
        except Exception as e:
            logger.error(f"Exception in MEXC.get_deposit_withdraw_info: {e}")
            return {"max_volume": "N/A", "deposit": "N/A", "withdraw": "N/A"}