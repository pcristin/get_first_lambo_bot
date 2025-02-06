import asyncio
import signal
from arbitrage.arbitrage_engine import ArbitrageEngine
from utils.logger import logger

def handle_exception(loop, context):
    """Handle exceptions that occur in the event loop"""
    msg = context.get("exception", context["message"])
    logger.error(f"Caught exception: {msg}")

async def shutdown_signal_handler(engine):
    """Handle shutdown signals"""
    logger.info("\nShutdown signal received. Stopping engine...")
    await engine.stop()

    # Cancel all pending tasks except this one
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    if tasks:
        logger.info(f"Cancelling {len(tasks)} pending task(s)")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    # Create the arbitrage engine
    engine = ArbitrageEngine()
    loop = asyncio.get_running_loop()

    # Define a simple signal handler wrapper
    def _handle_signal():
        asyncio.create_task(shutdown_signal_handler(engine))

    # Set up signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)
    
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
        await engine.close()
        logger.info("Engine closed, exiting.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected. Exiting.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise  # Re-raise to show the full traceback