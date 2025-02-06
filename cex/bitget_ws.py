from typing import Dict, Optional
import json
import time
from utils.logger import logger
from .websocket_manager import WebSocketManager

class BitgetWebSocket:
    """Bitget WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://ws.bitget.com/spot/v1/stream"
        self.base_url_futures = "wss://ws.bitget.com/mix/v1/stream"
        self._price_cache: Dict[str, Dict[str, float]] = {
            "spot": {},
            "futures": {}
        }
    
    async def start(self):
        """Start the Bitget WebSocket connections"""
        await self.ws_manager.start({
            "bitget_spot": self.base_url,
            "bitget_futures": self.base_url_futures
        })
    
    async def stop(self):
        """Stop the Bitget WebSocket connections"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str, market_type: str = "SPOT") -> str:
        """Format symbol for Bitget WebSocket (e.g., 'BTC/USDT' -> 'BTCUSDT')"""
        formatted = symbol.replace("/", "")
        if market_type.upper() == "FUTURES":
            return f"{formatted}_UMCBL"
        return formatted
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            # Extract data based on market type
            arg = data.get("arg", {})
            channel = arg.get("channel", "")
            if not channel:
                return
            
            # Determine market type from channel
            market_type = "futures" if "mix" in channel else "spot"
            
            # Extract symbol and price
            if market_type == "spot":
                symbol = arg.get("instId", "")
                price = float(data.get("data", [{}])[0].get("close", 0))
            else:
                symbol = arg.get("instId", "").replace("_UMCBL", "")
                price = float(data.get("data", [{}])[0].get("last", 0))
            
            if not symbol:
                return
            
            # Format symbol back to standard format (e.g., 'BTCUSDT' -> 'BTC/USDT')
            if symbol.endswith("USDT"):
                formatted_symbol = f"{symbol[:-4]}/USDT"
            else:
                formatted_symbol = symbol  # Handle other quote currencies if needed
            
            if price > 0:
                self._price_cache[market_type][formatted_symbol] = price
                logger.debug(
                    f"Updated Bitget {market_type} price for {formatted_symbol}: "
                    f"${str(price).replace('.', ',')}"
                )
        except Exception as e:
            logger.error(f"Error processing Bitget price update: {e}")
    
    async def subscribe_to_price(self, symbol: str, market_type: str = "SPOT"):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"bitget_{market_type.lower()}"
        
        # Bitget-specific subscription message
        if market_type.upper() == "SPOT":
            subscription_msg = {
                "op": "subscribe",
                "args": [{
                    "instType": "sp",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
            }
        else:
            subscription_msg = {
                "op": "subscribe",
                "args": [{
                    "instType": "mc",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
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
        
        logger.info(f"Subscribed to Bitget {market_type} price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str, market_type: str = "SPOT"):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"bitget_{market_type.lower()}"
        
        # Bitget-specific unsubscription message
        if market_type.upper() == "SPOT":
            unsubscription_msg = {
                "op": "unsubscribe",
                "args": [{
                    "instType": "sp",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
            }
        else:
            unsubscription_msg = {
                "op": "unsubscribe",
                "args": [{
                    "instType": "mc",
                    "channel": "ticker",
                    "instId": formatted_symbol
                }]
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
        logger.info(f"Unsubscribed from Bitget {market_type} price updates for {symbol}")
    
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