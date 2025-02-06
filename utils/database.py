from datetime import datetime
from typing import Dict, List, Optional, Union
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, func
from sqlalchemy.sql import text
from utils.logger import logger
from utils.models import Base, Opportunity, Trade, PriceHistory, Analytics
from config.database import (
    DATABASE_URL, DB_ECHO, DB_POOL_SIZE, DB_MAX_OVERFLOW,
    DB_POOL_TIMEOUT, DB_POOL_RECYCLE, DB_TYPE
)

class Database:
    """Async SQLAlchemy database service"""
    
    def __init__(self, db_url: str = DATABASE_URL):
        # Create engine with appropriate configuration based on database type
        engine_kwargs = {
            "echo": DB_ECHO,
        }
        
        if DB_TYPE == "postgresql":
            # Add PostgreSQL-specific settings
            engine_kwargs.update({
                "pool_size": DB_POOL_SIZE,
                "max_overflow": DB_MAX_OVERFLOW,
                "pool_timeout": DB_POOL_TIMEOUT,
                "pool_recycle": DB_POOL_RECYCLE
            })
        
        self.engine = create_async_engine(
            db_url,
            **engine_kwargs
        )
        
        self.async_session = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info(f"Initialized database connection using {DB_TYPE}")
    
    async def init(self):
        """Initialize database schema"""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database schema: {e}")
            raise
    
    async def log_opportunity(self,
                            token: str,
                            spread: float,
                            high_exchange: str,
                            high_price: float,
                            low_exchange: str,
                            low_price: float,
                            market_type: str,
                            volume_24h: Optional[float] = None,
                            liquidity_score: Optional[float] = None) -> int:
        """Log an arbitrage opportunity"""
        async with self.async_session() as session:
            opportunity = Opportunity(
                token=token,
                spread=spread,
                high_exchange=high_exchange,
                high_price=high_price,
                low_exchange=low_exchange,
                low_price=low_price,
                market_type=market_type,
                volume_24h=volume_24h,
                liquidity_score=liquidity_score
            )
            session.add(opportunity)
            await session.commit()
            return opportunity.id
    
    async def log_price(self,
                       token: str,
                       exchange: str,
                       market_type: str,
                       price: float):
        """Log a price update"""
        async with self.async_session() as session:
            price_history = PriceHistory(
                token=token,
                exchange=exchange,
                market_type=market_type,
                price=price
            )
            session.add(price_history)
            await session.commit()
    
    async def log_trade(self,
                       opportunity_id: int,
                       token: str,
                       buy_exchange: str,
                       buy_price: float,
                       buy_amount: float,
                       sell_exchange: str,
                       sell_price: float,
                       sell_amount: float,
                       profit_usd: float,
                       profit_percent: float,
                       status: str,
                       error: Optional[str] = None) -> int:
        """Log a completed trade"""
        async with self.async_session() as session:
            trade = Trade(
                opportunity_id=opportunity_id,
                token=token,
                buy_exchange=buy_exchange,
                buy_price=buy_price,
                buy_amount=buy_amount,
                sell_exchange=sell_exchange,
                sell_price=sell_price,
                sell_amount=sell_amount,
                profit_usd=profit_usd,
                profit_percent=profit_percent,
                status=status,
                error=error
            )
            session.add(trade)
            await session.commit()
            return trade.id
    
    async def log_metric(self,
                        metric: str,
                        value: float,
                        metadata: Optional[Dict] = None):
        """Log an analytics metric"""
        async with self.async_session() as session:
            analytics = Analytics(
                metric=metric,
                value=value,
                meta_data=metadata
            )
            session.add(analytics)
            await session.commit()
    
    async def get_recent_opportunities(self,
                                    limit: int = 100,
                                    min_spread: Optional[float] = None,
                                    token: Optional[str] = None,
                                    market_type: Optional[str] = None) -> List[Dict]:
        """Get recent arbitrage opportunities"""
        async with self.async_session() as session:
            query = select(Opportunity).order_by(Opportunity.timestamp.desc())
            
            if min_spread is not None:
                query = query.where(Opportunity.spread >= min_spread)
            if token:
                query = query.where(Opportunity.token == token)
            if market_type:
                query = query.where(Opportunity.market_type == market_type)
            
            query = query.limit(limit)
            result = await session.execute(query)
            opportunities = result.scalars().all()
            
            return [
                {
                    "id": opp.id,
                    "timestamp": opp.timestamp.isoformat(),
                    "token": opp.token,
                    "spread": opp.spread,
                    "high_exchange": opp.high_exchange,
                    "high_price": opp.high_price,
                    "low_exchange": opp.low_exchange,
                    "low_price": opp.low_price,
                    "market_type": opp.market_type,
                    "volume_24h": opp.volume_24h,
                    "liquidity_score": opp.liquidity_score,
                    "notification_sent": opp.notification_sent,
                    "executed": opp.executed
                }
                for opp in opportunities
            ]
    
    async def get_trade_history(self,
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None,
                              token: Optional[str] = None,
                              status: Optional[str] = None) -> List[Dict]:
        """Get trade history with filters"""
        async with self.async_session() as session:
            query = select(Trade).order_by(Trade.timestamp.desc())
            
            if start_date:
                query = query.where(Trade.timestamp >= start_date)
            if end_date:
                query = query.where(Trade.timestamp <= end_date)
            if token:
                query = query.where(Trade.token == token)
            if status:
                query = query.where(Trade.status == status)
            
            result = await session.execute(query)
            trades = result.scalars().all()
            
            return [
                {
                    "id": trade.id,
                    "opportunity_id": trade.opportunity_id,
                    "timestamp": trade.timestamp.isoformat(),
                    "token": trade.token,
                    "buy_exchange": trade.buy_exchange,
                    "buy_price": trade.buy_price,
                    "buy_amount": trade.buy_amount,
                    "sell_exchange": trade.sell_exchange,
                    "sell_price": trade.sell_price,
                    "sell_amount": trade.sell_amount,
                    "profit_usd": trade.profit_usd,
                    "profit_percent": trade.profit_percent,
                    "status": trade.status,
                    "error": trade.error
                }
                for trade in trades
            ]
    
    async def get_price_history(self,
                              token: str,
                              exchange: Optional[str] = None,
                              market_type: Optional[str] = None,
                              limit: int = 1000) -> List[Dict]:
        """Get price history for a token"""
        async with self.async_session() as session:
            query = select(PriceHistory).where(PriceHistory.token == token)
            
            if exchange:
                query = query.where(PriceHistory.exchange == exchange)
            if market_type:
                query = query.where(PriceHistory.market_type == market_type)
            
            query = query.order_by(PriceHistory.timestamp.desc()).limit(limit)
            result = await session.execute(query)
            prices = result.scalars().all()
            
            return [
                {
                    "id": price.id,
                    "timestamp": price.timestamp.isoformat(),
                    "token": price.token,
                    "exchange": price.exchange,
                    "market_type": price.market_type,
                    "price": price.price
                }
                for price in prices
            ]
    
    async def get_analytics(self,
                          metric: Optional[str] = None,
                          start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None) -> List[Dict]:
        """Get analytics data with filters"""
        async with self.async_session() as session:
            query = select(Analytics).order_by(Analytics.timestamp.desc())
            
            if metric:
                query = query.where(Analytics.metric == metric)
            if start_date:
                query = query.where(Analytics.timestamp >= start_date)
            if end_date:
                query = query.where(Analytics.timestamp <= end_date)
            
            result = await session.execute(query)
            analytics = result.scalars().all()
            
            return [
                {
                    "id": analytic.id,
                    "timestamp": analytic.timestamp.isoformat(),
                    "metric": analytic.metric,
                    "value": analytic.value,
                    "metadata": analytic.meta_data
                }
                for analytic in analytics
            ]
    
    async def get_summary_stats(self) -> Dict[str, Union[float, int]]:
        """Get summary statistics"""
        async with self.async_session() as session:
            stats = {}
            
            # Total opportunities found
            result = await session.execute(select(func.count()).select_from(Opportunity))
            stats['total_opportunities'] = result.scalar()
            
            # Total successful trades
            result = await session.execute(
                select(func.count())
                .select_from(Trade)
                .where(Trade.status == 'completed')
            )
            stats['total_trades'] = result.scalar()
            
            # Total profit
            result = await session.execute(
                select(func.sum(Trade.profit_usd))
                .select_from(Trade)
                .where(Trade.status == 'completed')
            )
            stats['total_profit_usd'] = result.scalar() or 0.0
            
            # Average spread
            result = await session.execute(
                select(func.avg(Opportunity.spread))
                .select_from(Opportunity)
            )
            stats['avg_spread'] = result.scalar() or 0.0
            
            # Most profitable token
            result = await session.execute(
                select(Trade.token, func.sum(Trade.profit_usd).label('total_profit'))
                .select_from(Trade)
                .where(Trade.status == 'completed')
                .group_by(Trade.token)
                .order_by(text('total_profit DESC'))
                .limit(1)
            )
            row = result.first()
            if row:
                stats['most_profitable_token'] = {
                    'token': row[0],
                    'profit_usd': row[1]
                }
            
            return stats
    
    async def close(self):
        """Close database connections"""
        await self.engine.dispose() 