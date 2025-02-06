from typing import Dict, Optional
import json
from utils.logger import logger
from .websocket_manager import WebSocketManager

class BybitWebSocket:
    """Bybit WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://stream.bybit.com/v5/public/spot"
        self.base_url_futures = "wss://stream.bybit.com/v5/public/linear"
        self._price_cache: Dict[str, Dict[str, float]] = {
            "spot": {},
            "futures": {}
        }
    
    async def start(self):
        """Start the Bybit WebSocket connections"""
        await self.ws_manager.start({
            "bybit_spot": self.base_url,
            "bybit_futures": self.base_url_futures
        })
    
    async def stop(self):
        """Stop the Bybit WebSocket connections"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str, market_type: str = "SPOT") -> str:
        """Format symbol for Bybit WebSocket (e.g., 'BTC/USDT' -> 'BTCUSDT')"""
        return symbol.replace("/", "")
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            # Extract data based on market type
            topic = data.get("topic", "")
            if not topic:
                return
            
            # Determine market type from topic
            market_type = "futures" if "linear" in topic else "spot"
            
            # Extract symbol and price
            symbol = data.get("data", {}).get("symbol", "")
            if not symbol:
                return
            
            # Format symbol back to standard format (e.g., 'BTCUSDT' -> 'BTC/USDT')
            if symbol.endswith("USDT"):
                formatted_symbol = f"{symbol[:-4]}/USDT"
            else:
                formatted_symbol = symbol  # Handle other quote currencies if needed
            
            # Get last price
            price = float(data.get("data", {}).get("lastPrice", 0))
            if price > 0:
                self._price_cache[market_type][formatted_symbol] = price
                logger.debug(
                    f"Updated Bybit {market_type} price for {formatted_symbol}: "
                    f"${str(price).replace('.', ',')}"
                )
        except Exception as e:
            logger.error(f"Error processing Bybit price update: {e}")
    
    async def subscribe_to_price(self, symbol: str, market_type: str = "SPOT"):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"bybit_{market_type.lower()}"
        
        # Bybit-specific subscription message
        subscription_msg = {
            "op": "subscribe",
            "args": [f"tickers.{formatted_symbol}"]
        }
        
        await self.ws_manager.subscribe(
            exchange=exchange,
            symbol=formatted_symbol,
            callback=self._price_callback
        )
        
        # Send subscription message
        if exchange in self.ws_manager.connections:
            ws = self.ws_manager.connections[exchange]
            if not ws.closed:
                await ws.send_json(subscription_msg)
        
        logger.info(f"Subscribed to Bybit {market_type} price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str, market_type: str = "SPOT"):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"bybit_{market_type.lower()}"
        
        # Bybit-specific unsubscription message
        unsubscription_msg = {
            "op": "unsubscribe",
            "args": [f"tickers.{formatted_symbol}"]
        }
        
        await self.ws_manager.unsubscribe(
            exchange=exchange,
            symbol=formatted_symbol
        )
        
        # Send unsubscription message
        if exchange in self.ws_manager.connections:
            ws = self.ws_manager.connections[exchange]
            if not ws.closed:
                await ws.send_json(unsubscription_msg)
        
        # Clear cached price
        self._price_cache[market_type.lower()].pop(symbol, None)
        logger.info(f"Unsubscribed from Bybit {market_type} price updates for {symbol}")
    
    def get_cached_price(self, symbol: str, market_type: str = "SPOT") -> Optional[float]:
        """Get the most recent price from cache"""
        return self._price_cache[market_type.lower()].get(symbol)
    
    @property
    def subscribed_symbols(self):
        """Get list of currently subscribed symbols by market type"""
        return {
            "spot": list(self._price_cache["spot"].keys()),
            "futures": list(self._price_cache["futures"].keys())
        } 