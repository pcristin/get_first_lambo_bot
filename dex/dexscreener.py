# dex/dexscreener.py
import requests
from utils.logger import logger

class DexScreener:
    BASE_URL = "https://api.dexscreener.com/latest/dex/search/"

    def get_token_data(self, token_symbol):
        """
        Searches Dexscreener for the token.
        Returns a dict with:
          - price: (float) price in USD
          - contract: (str) contract address
          - network: (str) chain/network (e.g. SOLANA)
          - dex_url: (str) direct URL to the token on Dexscreener
        """
        params = {"q": token_symbol}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            data = response.json()
            if data.get("pairs"):
                # Choose the first matching pair.
                pair = data["pairs"][0]
                token_data = {
                    "price": float(pair.get("priceUsd", 0)),
                    "contract": pair.get("baseToken", {}).get("address", ""),
                    "network": pair.get("baseToken", {}).get("chainId", "").upper(),
                    "dex_url": f"https://dexscreener.com/{pair.get('chainId', '').lower()}/{pair.get('id', '')}"
                }
                logger.info(f"DexScreener data for {token_symbol}: {token_data}")
                return token_data
            else:
                logger.error(f"No DexScreener results for {token_symbol}")
                return None
        except Exception as e:
            logger.error(f"Error in DexScreener.get_token_data: {e}")
            return None