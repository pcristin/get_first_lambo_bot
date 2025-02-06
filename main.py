import asyncio
from arbitrage.arbitrage_engine import ArbitrageEngine

async def main():
    engine = ArbitrageEngine()
    await engine.run()

if __name__ == '__main__':
    asyncio.run(main())