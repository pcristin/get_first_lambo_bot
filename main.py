import asyncio
import signal
from arbitrage.arbitrage_engine import ArbitrageEngine
from utils.logger import logger

async def shutdown_signal_handler(engine, loop):
    """Handle shutdown signals"""
    logger.info("\nReceived shutdown signal. Starting graceful shutdown...")
    try:
        # Stop the engine first
        await engine.stop()
        
        # Cancel all other tasks
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
            
        # Give tasks a moment to respond to cancellation
        await asyncio.sleep(0.1)
        
        # Wait for all tasks to complete with a timeout
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Some tasks did not complete within timeout")
                
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        loop.stop()

async def main():
    # Create the arbitrage engine
    engine = ArbitrageEngine()
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown_signal_handler(engine, loop))
        )
    
    try:
        logger.info("Starting arbitrage bot...")
        # Run the main loop
        await engine.run()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        # Ensure engine is properly closed
        logger.info("Cleaning up resources...")
        await engine.close()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nShutdown complete.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise  # Re-raise to show the full traceback