from typing import Dict, Optional
import json
from utils.logger import logger
from .websocket_manager import WebSocketManager

class OKXWebSocket:
    """OKX WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://ws.okx.com:8443/ws/v5/public"
        self._price_cache: Dict[str, Dict[str, float]] = {
            "spot": {},
            "futures": {}
        }
    
    async def start(self):
        """Start the OKX WebSocket connection"""
        await self.ws_manager.start({
            "okx": self.base_url
        })
    
    async def stop(self):
        """Stop the OKX WebSocket connection"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str, market_type: str = "SPOT") -> str:
        """Format symbol for OKX WebSocket (e.g., 'BTC/USDT' -> 'BTC-USDT')"""
        base, quote = symbol.split("/")
        if market_type.upper() == "SPOT":
            return f"{base}-{quote}"
        else:  # FUTURES
            return f"{base}-{quote}-SWAP"
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            inst_id = data.get("instId", "")
            if not inst_id:
                return
            
            # Determine market type and format symbol
            is_swap = inst_id.endswith("-SWAP")
            market_type = "futures" if is_swap else "spot"
            
            # Format symbol back to standard format
            if is_swap:
                base, quote, _ = inst_id.split("-")
            else:
                base, quote = inst_id.split("-")
            symbol = f"{base}/{quote}"
            
            price = float(data.get("last", 0))
            if price > 0:
                self._price_cache[market_type][symbol] = price
                logger.debug(
                    f"Updated OKX {market_type} price for {symbol}: "
                    f"${str(price).replace('.', ',')}"
                )
        except Exception as e:
            logger.error(f"Error processing OKX price update: {e}")
    
    async def subscribe_to_price(self, symbol: str, market_type: str = "SPOT"):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        
        # OKX-specific subscription message
        subscription_msg = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": formatted_symbol
            }]
        }
        
        await self.ws_manager.subscribe(
            exchange="okx",
            symbol=formatted_symbol,
            callback=self._price_callback
        )
        
        # Send subscription message
        if "okx" in self.ws_manager.connections:
            ws = self.ws_manager.connections["okx"]
            if not ws.closed:
                await ws.send_json(subscription_msg)
        
        logger.info(f"Subscribed to OKX {market_type} price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str, market_type: str = "SPOT"):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        
        # OKX-specific unsubscription message
        unsubscription_msg = {
            "op": "unsubscribe",
            "args": [{
                "channel": "tickers",
                "instId": formatted_symbol
            }]
        }
        
        await self.ws_manager.unsubscribe(
            exchange="okx",
            symbol=formatted_symbol
        )
        
        # Send unsubscription message
        if "okx" in self.ws_manager.connections:
            ws = self.ws_manager.connections["okx"]
            if not ws.closed:
                await ws.send_json(unsubscription_msg)
        
        # Clear cached price
        self._price_cache[market_type.lower()].pop(symbol, None)
        logger.info(f"Unsubscribed from OKX {market_type} price updates for {symbol}")
    
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