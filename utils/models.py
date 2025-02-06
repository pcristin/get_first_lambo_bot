from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Opportunity(Base):
    """Model for arbitrage opportunities"""
    __tablename__ = "opportunities"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    token = Column(String, nullable=False)
    spread = Column(Float, nullable=False)
    high_exchange = Column(String, nullable=False)
    high_price = Column(Float, nullable=False)
    low_exchange = Column(String, nullable=False)
    low_price = Column(Float, nullable=False)
    market_type = Column(String, nullable=False)
    volume_24h = Column(Float)
    liquidity_score = Column(Float)
    notification_sent = Column(Boolean, default=False)
    executed = Column(Boolean, default=False)
    
    # Relationships
    trades = relationship("Trade", back_populates="opportunity")

class Trade(Base):
    """Model for executed trades"""
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True)
    opportunity_id = Column(Integer, ForeignKey("opportunities.id"))
    timestamp = Column(DateTime, default=datetime.utcnow)
    token = Column(String, nullable=False)
    buy_exchange = Column(String, nullable=False)
    buy_price = Column(Float, nullable=False)
    buy_amount = Column(Float, nullable=False)
    sell_exchange = Column(String, nullable=False)
    sell_price = Column(Float, nullable=False)
    sell_amount = Column(Float, nullable=False)
    profit_usd = Column(Float, nullable=False)
    profit_percent = Column(Float, nullable=False)
    status = Column(String, nullable=False)
    error = Column(String)
    
    # Relationships
    opportunity = relationship("Opportunity", back_populates="trades")

class PriceHistory(Base):
    """Model for price history"""
    __tablename__ = "price_history"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    token = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    market_type = Column(String, nullable=False)
    price = Column(Float, nullable=False)

class Analytics(Base):
    """Model for analytics data"""
    __tablename__ = "analytics"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    metric = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    meta_data = Column(JSON) 