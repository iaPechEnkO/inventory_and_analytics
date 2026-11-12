from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    func,
    Integer,
    MetaData,
    String,
    Table,
)

metadata = MetaData()

stock_events = Table(
    "stock_events",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("sku", String(64), nullable=False),
    Column("qty", Integer, nullable=False),
    Column("warehouse", String(64), nullable=False),
    Column("event_type", String(16), nullable=False),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
)

stock_agg = Table(
    "stock_agg",
    metadata,
    Column("warehouse", String(64), primary_key=True),
    Column("sku", String(64), primary_key=True),
    Column("qty", Integer, nullable=False, default=0),
)
