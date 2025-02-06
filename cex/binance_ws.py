from typing import Dict, Optional
import json
from utils.logger import logger
from .websocket_manager import WebSocketManager

class BinanceWebSocket:
    """Binance WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://stream.binance.com:9443/ws"
        self._price_cache: Dict[str, float] = {}
    
    async def start(self):
        """Start the Binance WebSocket connection"""
        await self.ws_manager.start({
            "binance": self.base_url
        })
    
    async def stop(self):
        """Stop the Binance WebSocket connection"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str) -> str:
        """Format symbol for Binance WebSocket (e.g., 'BTC/USDT' -> 'btcusdt')"""
        return symbol.replace("/", "").lower()
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            symbol = data.get("s", "").upper()  # BTCUSDT -> BTC/USDT
            if not symbol:
                return
                
            # Format symbol back to standard format
            formatted_symbol = f"{symbol[:-4]}/{symbol[-4:]}" if symbol.endswith(("USDT", "BUSD")) else symbol
            
            price = float(data.get("c", 0))  # "c" is the close price
            if price > 0:
                self._price_cache[formatted_symbol] = price
                logger.debug(f"Updated Binance price for {formatted_symbol}: ${str(price).replace('.', ',')}")
        except Exception as e:
            logger.error(f"Error processing Binance price update: {e}")
    
    async def subscribe_to_price(self, symbol: str):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol)
        stream_name = f"{formatted_symbol}@ticker"
        
        await self.ws_manager.subscribe(
            exchange="binance",
            symbol=formatted_symbol,
            callback=self._price_callback
        )
        
        logger.info(f"Subscribed to Binance price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol)
        
        await self.ws_manager.unsubscribe(
            exchange="binance",
            symbol=formatted_symbol
        )
        
        # Clear cached price
        self._price_cache.pop(symbol, None)
        logger.info(f"Unsubscribed from Binance price updates for {symbol}")
    
    def get_cached_price(self, symbol: str) -> Optional[float]:
        """Get the most recent price from cache"""
        return self._price_cache.get(symbol)
    
    @property
    def subscribed_symbols(self):
        """Get list of currently subscribed symbols"""
        return list(self._price_cache.keys()) 