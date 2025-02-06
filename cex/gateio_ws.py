from typing import Dict, Optional
import json
from utils.logger import logger
from .websocket_manager import WebSocketManager

class GateioWebSocket:
    """Gate.io WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://api.gateio.ws/ws/v4/"
        self.base_url_futures = "wss://fx-ws.gateio.ws/v4/ws/usdt"
        self._price_cache: Dict[str, Dict[str, float]] = {
            "spot": {},
            "futures": {}
        }
    
    async def start(self):
        """Start the Gate.io WebSocket connections"""
        await self.ws_manager.start({
            "gateio_spot": self.base_url,
            "gateio_futures": self.base_url_futures
        })
    
    async def stop(self):
        """Stop the Gate.io WebSocket connections"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str, market_type: str = "SPOT") -> str:
        """Format symbol for Gate.io WebSocket (e.g., 'BTC/USDT' -> 'BTC_USDT')"""
        return symbol.replace("/", "_")
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            # Extract data based on market type
            channel = data.get("channel", "")
            if not channel:
                return
            
            # Determine market type from channel
            market_type = "futures" if "futures" in channel else "spot"
            
            # Extract symbol and price
            if market_type == "spot":
                symbol = data.get("result", {}).get("currency_pair", "")
                price = float(data.get("result", {}).get("last", 0))
            else:
                symbol = data.get("result", {}).get("contract", "")
                price = float(data.get("result", {}).get("last", 0))
            
            if not symbol:
                return
            
            # Format symbol back to standard format (e.g., 'BTC_USDT' -> 'BTC/USDT')
            formatted_symbol = symbol.replace("_", "/")
            
            if price > 0:
                self._price_cache[market_type][formatted_symbol] = price
                logger.debug(
                    f"Updated Gate.io {market_type} price for {formatted_symbol}: "
                    f"${str(price).replace('.', ',')}"
                )
        except Exception as e:
            logger.error(f"Error processing Gate.io price update: {e}")
    
    async def subscribe_to_price(self, symbol: str, market_type: str = "SPOT"):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"gateio_{market_type.lower()}"
        
        # Gate.io-specific subscription message
        if market_type.upper() == "SPOT":
            subscription_msg = {
                "time": int(time.time()),
                "channel": "spot.tickers",
                "event": "subscribe",
                "payload": [formatted_symbol]
            }
        else:
            subscription_msg = {
                "time": int(time.time()),
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": [formatted_symbol]
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
        
        logger.info(f"Subscribed to Gate.io {market_type} price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str, market_type: str = "SPOT"):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"gateio_{market_type.lower()}"
        
        # Gate.io-specific unsubscription message
        if market_type.upper() == "SPOT":
            unsubscription_msg = {
                "time": int(time.time()),
                "channel": "spot.tickers",
                "event": "unsubscribe",
                "payload": [formatted_symbol]
            }
        else:
            unsubscription_msg = {
                "time": int(time.time()),
                "channel": "futures.tickers",
                "event": "unsubscribe",
                "payload": [formatted_symbol]
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
        logger.info(f"Unsubscribed from Gate.io {market_type} price updates for {symbol}")
    
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