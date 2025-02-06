import asyncio
import json
import logging
from typing import Dict, Set, Callable, Optional, List
import aiohttp
from utils.logger import logger

class WebSocketManager:
    def __init__(self):
        self.connections: Dict[str, aiohttp.ClientWebSocketResponse] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # exchange -> set of symbols
        self.callbacks: Dict[str, Dict[str, List[Callable]]] = {}  # exchange -> symbol -> list of callbacks
        self.running = False
        self.tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

    async def subscribe(self, exchange: str, symbol: str, callback: Callable):
        """Subscribe to real-time updates for a symbol on an exchange"""
        if exchange not in self.callbacks:
            self.callbacks[exchange] = {}
        if symbol not in self.callbacks[exchange]:
            self.callbacks[exchange][symbol] = []
        
        self.callbacks[exchange][symbol].append(callback)
        
        if exchange not in self.subscriptions:
            self.subscriptions[exchange] = set()
        
        if symbol not in self.subscriptions[exchange]:
            self.subscriptions[exchange].add(symbol)
            if exchange in self.connections and not self.connections[exchange].closed:
                await self._subscribe_symbol(exchange, symbol)

    async def unsubscribe(self, exchange: str, symbol: str, callback: Optional[Callable] = None):
        """Unsubscribe from updates for a symbol"""
        if callback:
            if (exchange in self.callbacks and 
                symbol in self.callbacks[exchange]):
                self.callbacks[exchange][symbol].remove(callback)
                if not self.callbacks[exchange][symbol]:
                    del self.callbacks[exchange][symbol]
                    if not self.callbacks[exchange]:
                        del self.callbacks[exchange]
        else:
            # Remove all callbacks for this symbol
            if exchange in self.callbacks:
                self.callbacks[exchange].pop(symbol, None)
                if not self.callbacks[exchange]:
                    del self.callbacks[exchange]

        # If no more callbacks for this symbol, unsubscribe from exchange
        if (not callback or 
            exchange not in self.callbacks or 
            symbol not in self.callbacks[exchange]):
            if exchange in self.subscriptions:
                self.subscriptions[exchange].discard(symbol)
                if exchange in self.connections and not self.connections[exchange].closed:
                    await self._unsubscribe_symbol(exchange, symbol)

    async def _subscribe_symbol(self, exchange: str, symbol: str):
        """Send subscription message to exchange"""
        if exchange not in self.connections or self.connections[exchange].closed:
            return
        
        try:
            # Example subscription message (customize per exchange)
            message = {
                "method": "subscribe",
                "params": [f"{symbol.lower()}@ticker"],
                "id": 1
            }
            await self.connections[exchange].send_json(message)
            logger.info(f"Subscribed to {symbol} on {exchange}")
        except Exception as e:
            logger.error(f"Error subscribing to {symbol} on {exchange}: {e}")

    async def _unsubscribe_symbol(self, exchange: str, symbol: str):
        """Send unsubscription message to exchange"""
        if exchange not in self.connections or self.connections[exchange].closed:
            return
        
        try:
            # Example unsubscription message (customize per exchange)
            message = {
                "method": "unsubscribe",
                "params": [f"{symbol.lower()}@ticker"],
                "id": 1
            }
            await self.connections[exchange].send_json(message)
            logger.info(f"Unsubscribed from {symbol} on {exchange}")
        except Exception as e:
            logger.error(f"Error unsubscribing from {symbol} on {exchange}: {e}")

    async def _handle_message(self, exchange: str, message: str):
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            # Example message handling (customize per exchange)
            if "data" in data:
                symbol = data["data"].get("s")  # symbol
                if (exchange in self.callbacks and 
                    symbol in self.callbacks[exchange]):
                    for callback in self.callbacks[exchange][symbol]:
                        try:
                            await callback(data["data"])
                        except Exception as e:
                            logger.error(f"Error in callback for {symbol} on {exchange}: {e}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON from {exchange}: {message}")
        except Exception as e:
            logger.error(f"Error handling message from {exchange}: {e}")

    async def _maintain_connection(self, exchange: str, url: str):
        """Maintain WebSocket connection with reconnection logic"""
        backoff = 1
        max_backoff = 60
        
        while not self._shutdown_event.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        self.connections[exchange] = ws
                        logger.info(f"Connected to {exchange} WebSocket")
                        
                        # Resubscribe to all symbols
                        if exchange in self.subscriptions:
                            for symbol in self.subscriptions[exchange]:
                                await self._subscribe_symbol(exchange, symbol)
                        
                        backoff = 1  # Reset backoff on successful connection
                        
                        while not self._shutdown_event.is_set():
                            try:
                                msg = await ws.receive()
                                
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self._handle_message(exchange, msg.data)
                                elif msg.type == aiohttp.WSMsgType.CLOSED:
                                    logger.warning(f"{exchange} WebSocket connection closed")
                                    break
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    logger.error(f"{exchange} WebSocket connection error: {ws.exception()}")
                                    break
                            except Exception as e:
                                logger.error(f"Error processing {exchange} WebSocket message: {e}")
                                if not ws.closed:
                                    await ws.close()
                                break
                            
            except Exception as e:
                logger.error(f"Error in {exchange} WebSocket connection: {e}")
            
            if self._shutdown_event.is_set():
                break
                
            # Exponential backoff with jitter
            delay = min(max_backoff, backoff + (backoff * 0.1 * (asyncio.get_event_loop().time() % 1)))
            logger.info(f"Reconnecting to {exchange} WebSocket in {delay:.1f} seconds...")
            await asyncio.sleep(delay)
            backoff = min(max_backoff, backoff * 2)

    async def start(self, exchange_urls: Dict[str, str]):
        """Start WebSocket connections to exchanges"""
        self.running = True
        self._shutdown_event.clear()
        
        for exchange, url in exchange_urls.items():
            task = asyncio.create_task(
                self._maintain_connection(exchange, url)
            )
            self.tasks.append(task)
        
        logger.info("WebSocket manager started")

    async def stop(self):
        """Stop all WebSocket connections"""
        self.running = False
        self._shutdown_event.set()
        
        # Close all connections
        for exchange, ws in self.connections.items():
            if not ws.closed:
                await ws.close()
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        self.tasks.clear()
        self.connections.clear()
        self.subscriptions.clear()
        self.callbacks.clear()
        
        logger.info("WebSocket manager stopped") 