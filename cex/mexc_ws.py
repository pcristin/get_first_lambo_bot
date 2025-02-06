from typing import Dict, Optional
import json
import time
from utils.logger import logger
from .websocket_manager import WebSocketManager

class MEXCWebSocket:
    """MEXC WebSocket client implementation"""
    
    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager
        self.base_url = "wss://wbs.mexc.com/ws"
        self.base_url_futures = "wss://contract.mexc.com/ws"
        self._price_cache: Dict[str, Dict[str, float]] = {
            "spot": {},
            "futures": {}
        }
    
    async def start(self):
        """Start the MEXC WebSocket connections"""
        await self.ws_manager.start({
            "mexc_spot": self.base_url,
            "mexc_futures": self.base_url_futures
        })
    
    async def stop(self):
        """Stop the MEXC WebSocket connections"""
        await self.ws_manager.stop()
    
    def _format_symbol(self, symbol: str, market_type: str = "SPOT") -> str:
        """Format symbol for MEXC WebSocket (e.g., 'BTC/USDT' -> 'BTC_USDT')"""
        return symbol.replace("/", "")
    
    async def _price_callback(self, data: dict):
        """Handle price update from WebSocket"""
        try:
            # Extract data based on market type
            channel = data.get("c", "")  # channel
            if not channel:
                return
            
            # Determine market type from channel
            market_type = "futures" if "contract" in channel else "spot"
            
            # Extract symbol and price based on market type
            if market_type == "spot":
                symbol = data.get("s", "")  # symbol
                price = float(data.get("p", 0))  # price
            else:
                symbol = data.get("symbol", "")
                price = float(data.get("lastPrice", 0))
            
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
                    f"Updated MEXC {market_type} price for {formatted_symbol}: "
                    f"${str(price).replace('.', ',')}"
                )
        except Exception as e:
            logger.error(f"Error processing MEXC price update: {e}")
    
    async def subscribe_to_price(self, symbol: str, market_type: str = "SPOT"):
        """Subscribe to real-time price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"mexc_{market_type.lower()}"
        
        # MEXC-specific subscription message
        if market_type.upper() == "SPOT":
            subscription_msg = {
                "method": "SUBSCRIPTION",
                "params": [
                    f"spot@public.deals.v3.api@{formatted_symbol}"
                ]
            }
        else:
            subscription_msg = {
                "method": "sub.deal",
                "param": {
                    "symbol": formatted_symbol
                }
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
        
        logger.info(f"Subscribed to MEXC {market_type} price updates for {symbol}")
    
    async def unsubscribe_from_price(self, symbol: str, market_type: str = "SPOT"):
        """Unsubscribe from price updates for a symbol"""
        formatted_symbol = self._format_symbol(symbol, market_type)
        exchange = f"mexc_{market_type.lower()}"
        
        # MEXC-specific unsubscription message
        if market_type.upper() == "SPOT":
            unsubscription_msg = {
                "method": "UNSUBSCRIPTION",
                "params": [
                    f"spot@public.deals.v3.api@{formatted_symbol}"
                ]
            }
        else:
            unsubscription_msg = {
                "method": "unsub.deal",
                "param": {
                    "symbol": formatted_symbol
                }
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
        logger.info(f"Unsubscribed from MEXC {market_type} price updates for {symbol}")
    
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