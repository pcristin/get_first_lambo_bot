import aiohttp
from typing import Optional
from utils.logger import logger

class JupiterAPI:
    BASE_URL = "https://api.jup.ag/swap/v1/quote"
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC SPL token mint
    
    def __init__(self):
        self.session = None
        
    async def get_token_price(self, token_mint: str, amount: int = 1000000) -> Optional[float]:
        """
        Get token price from Jupiter API.
        Args:
            token_mint: The Solana token mint address
            amount: Amount in lamports/smallest units (default 1M = 1 USDC)
        Returns:
            Price in USDC or None if error
        """
        try:
            params = {
                "inputMint": token_mint,
                "outputMint": self.USDC_MINT,
                "amount": str(amount),
                "slippageBps": 50,  # 0.5% slippage
                "restrictIntermediateTokens": True
            }
            
            session = await self._get_session()
            async with session.get(self.BASE_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if "outAmount" in data:
                        # Convert from USDC smallest units (6 decimals) to USDC
                        price = float(data["outAmount"]) / 1_000_000
                        # Since we're quoting 1 token worth, this is the price per token
                        logger.info(f"Jupiter price for {token_mint}: ${price:.4f}")
                        return price
                    else:
                        logger.error(f"Jupiter API invalid response format for {token_mint}")
                else:
                    logger.error(f"Jupiter API error for {token_mint}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error getting Jupiter price for {token_mint}: {e}")
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close() 