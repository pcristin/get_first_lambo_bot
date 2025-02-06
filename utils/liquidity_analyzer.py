import requests
from typing import Dict, List, Optional
from utils.logger import logger
from cex.binance import Binance
from cex.kucoin import Kucoin
from cex.bybit import Bybit
from cex.okx import OKX
from cex.manager import CEXManager
from dex.dexscreener import DexScreener

class LiquidityAnalyzer:
    def __init__(self):
        self.binance = Binance()
        self.kucoin = Kucoin()
        self.bybit = Bybit()
        self.okx = OKX()
        self.cex_manager = CEXManager()
        self.dexscreener = DexScreener()
        
        # Minimum liquidity thresholds in USD
        self.MIN_CEX_24H_VOLUME = 1_000_000  # $1M daily volume on CEX
        self.MIN_DEX_LIQUIDITY = 500_000     # $500K liquidity on DEX
        
    async def get_cex_volume(self, symbol: str) -> Dict[str, float]:
        """Get 24h trading volume across major CEXes"""
        volumes = {}
        
        # Get Binance volume
        try:
            response = requests.get(
                f"{self.binance.SPOT_API_URL}/24hr",
                params={"symbol": f"{symbol}USDT"},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                volumes["binance"] = float(data.get("volume", 0)) * float(data.get("weightedAvgPrice", 0))
        except Exception as e:
            logger.error(f"Error getting Binance volume: {e}")
            
        # Add similar implementations for other CEXes
        # This is a basic implementation that can be expanded
            
        return volumes
    
    async def get_dex_liquidity(self, symbol: str) -> Dict[str, float]:
        """Get DEX liquidity data"""
        liquidity = {}
        
        try:
            token_data = await self.dexscreener.get_token_data(symbol)
            if token_data:
                # DexScreener returns data for the most liquid pair
                liquidity["dexscreener"] = token_data.get("liquidity", 0)
        except Exception as e:
            logger.error(f"Error getting DEX liquidity: {e}")
            
        return liquidity
    
    async def analyze_token_liquidity(self, symbol: str) -> Dict:
        """
        Analyze token liquidity across exchanges.
        Returns a dict with liquidity metrics and whether it meets thresholds.
        """
        # Get CEX volumes and DEX liquidity in parallel
        total_cex_volume = await self.cex_manager.get_total_cex_volume(symbol)
        dex_liquidity = await self.get_dex_liquidity(symbol)
        
        total_dex_liquidity = sum(dex_liquidity.values())
        
        has_sufficient_liquidity = (
            total_cex_volume >= self.cex_manager.min_volume_threshold or
            total_dex_liquidity >= self.cex_manager.min_liquidity_threshold
        )
        
        return {
            "symbol": symbol,
            "total_cex_volume": total_cex_volume,
            "dex_liquidity": dex_liquidity,
            "total_dex_liquidity": total_dex_liquidity,
            "has_sufficient_liquidity": has_sufficient_liquidity,
            "metrics": {
                "cex_volume_threshold": self.cex_manager.min_volume_threshold,
                "dex_liquidity_threshold": self.cex_manager.min_liquidity_threshold
            }
        }
    
    async def get_high_liquidity_tokens(self, symbols: List[str]) -> List[Dict]:
        """
        Filter and return only tokens with high liquidity from a list of symbols.
        """
        high_liquidity_tokens = []
        
        for symbol in symbols:
            analysis = await self.analyze_token_liquidity(symbol)
            if analysis["has_sufficient_liquidity"]:
                high_liquidity_tokens.append(analysis)
                
        return high_liquidity_tokens

    async def close(self):
        """Close all connections"""
        await self.cex_manager.close()
        await self.dexscreener.close() 