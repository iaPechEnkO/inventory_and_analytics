import json

from sqlalchemy import select, func, insert

from database.tables import stock_events
from database.queries import stock_subq
from settings.config import RECEIPT_CHANNEL, ISSUE_CHANNEL
from settings.logger import setup_logger

logger = setup_logger(__name__)


async def record_event(
    db, redis_conn, event_type: str, sku: str, qty: int, warehouse: str
):
    async with db.begin() as conn:
        await conn.execute(
            insert(stock_events).values(
                sku=sku,
                qty=qty,
                warehouse=warehouse,
                event_type=event_type,
            )
        )
    channel = RECEIPT_CHANNEL if event_type == "receipt" else ISSUE_CHANNEL
    await redis_conn.publish_with_retry(
        channel, json.dumps({"sku": sku, "qty": qty, "warehouse": warehouse})
    )


async def get_stock(db, warehouse: str | None, sku: str | None):
    q = select(stock_subq.c.warehouse, stock_subq.c.sku, stock_subq.c.qty)
    if warehouse:
        q = q.where(stock_subq.c.warehouse == warehouse)
    if sku:
        q = q.where(stock_subq.c.sku == sku)
    async with db.connect() as conn:
        rows = (await conn.execute(q)).mappings().all()
    return [
        {
            "warehouse": r["warehouse"],
            "sku": r["sku"],
            "qty": int(r["qty"]),
        }
        for r in rows
    ]


async def get_summary(db, top_n: int = 5):
    top_skus_subq = (
        select(stock_subq.c.sku, func.sum(stock_subq.c.qty).label("total_qty"))
        .group_by(stock_subq.c.sku)
        .order_by(func.sum(stock_subq.c.qty).desc())
        .limit(top_n)
        .subquery()
    )
    wh_stmt = select(
        stock_subq.c.warehouse,
        func.sum(stock_subq.c.qty).label("total_qty"),
        func.count(stock_subq.c.sku).label("sku_count"),
    ).group_by(stock_subq.c.warehouse)
    async with db.connect() as conn:
        wh_rows = (await conn.execute(wh_stmt)).all()
        top_rows = (await conn.execute(select(top_skus_subq))).all()

    return {
        "warehouses": [
            {
                "warehouse": r.warehouse,
                "total_qty": int(r.total_qty),
                "sku_count": int(r.sku_count),
            }
            for r in wh_rows
        ],
        "top_skus": [
            {
                "sku": r.sku,
                "total_qty": int(r.total_qty),
            }
            for r in top_rows
        ],
    }
