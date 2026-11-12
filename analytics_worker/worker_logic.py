import asyncio
import json

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.tables import stock_agg, stock_events
from settings.logger import setup_logger

logger = setup_logger(__name__)

CHANNELS = ("inventory.receipt", "inventory.issue")


async def seed_agg(db):
    signed = case(
        (stock_events.c.event_type == "receipt", stock_events.c.qty),
        else_=-stock_events.c.qty,
    )
    subq = (
        select(
            stock_events.c.warehouse,
            stock_events.c.sku,
            func.sum(signed).label("qty"),
        )
        .group_by(stock_events.c.warehouse, stock_events.c.sku)
        .subquery()
    )
    async with db.begin() as conn:
        await conn.execute(
            pg_insert(stock_agg)
            .from_select(
                ["warehouse", "sku", "qty"],
                select(subq.c.warehouse, subq.c.sku, subq.c.qty),
            )
            .on_conflict_do_update(
                index_elements=["warehouse", "sku"],
                set_={"qty": pg_insert(stock_agg).excluded.qty},
            )
        )


async def handle_event(db, event_type: str, payload: dict):
    delta = payload["qty"] if event_type == "receipt" else -payload["qty"]
    async with db.begin() as conn:
        stmt = (
            pg_insert(stock_agg)
            .values(
                warehouse=payload["warehouse"],
                sku=payload["sku"],
                qty=delta,
            )
            .on_conflict_do_update(
                index_elements=["warehouse", "sku"],
                set_={"qty": stock_agg.c.qty + delta},
            )
        )
        await conn.execute(stmt)


async def _process_message(db, msg: dict):
    if msg["type"] != "message":
        return False
    try:
        payload = json.loads(msg["data"])
    except json.JSONDecodeError:
        logger.warning(f"[worker] not a json: {msg['data']}")
        return False
    if not all(k in payload for k in ("sku", "qty", "warehouse")):
        logger.warning(f"[worker] invalid payload: {payload}")
        return False
    if not isinstance(payload["qty"], int) or payload["qty"] <= 0:
        logger.warning(f'[worker] invalid qty: {payload["qty"]}')
        return False
    if not isinstance(payload["sku"], str) or not payload["sku"]:
        logger.warning(f'[worker] invalid sku: {payload["sku"]}')
        return False
    if not isinstance(payload["warehouse"], str) or not payload["warehouse"]:
        logger.warning(f'[worker] invalid warehouse: {payload["warehouse"]}')
        return False
    if msg["channel"] == "inventory.receipt":
        event_type = "receipt"
    elif msg["channel"] == "inventory.issue":
        event_type = "issue"
    else:
        logger.warning(f"[worker] unknown channel: {msg['channel']}")
        return False
    await handle_event(db, event_type, payload)
    return True


async def run_worker(db, redis_conn, ready_event: asyncio.Event | None = None):
    await seed_agg(db)
    pubsub = redis_conn.pubsub()
    await pubsub.subscribe(*CHANNELS)
    if ready_event:
        ready_event.set()
    logger.info(f"[worker] listening on {', '.join(CHANNELS)}")
    try:
        async for msg in pubsub.listen():
            try:
                await _process_message(db, msg)
            except Exception as err:
                error_message = "An error occurred during message processing"
                logger.error(f"{error_message}: {err}")
    finally:
        await pubsub.aclose()
