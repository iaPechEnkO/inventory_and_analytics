import asyncio
import json

import pytest
from sqlalchemy import select

from analytics_worker.worker_logic import run_worker, seed_agg
from database.tables import stock_events, stock_agg
from settings.config import RECEIPT_CHANNEL, ISSUE_CHANNEL


async def _wait_for_qty(
    db_conn,
    warehouse: str,
    sku: str,
    expected: int,
    timeout: float = 5.0,
    interval: float = 0.1,
):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last = None
    while loop.time() < deadline:
        async with db_conn.connect() as conn:
            last = (
                await conn.execute(
                    select(stock_agg.c.qty).where(
                        stock_agg.c.warehouse == warehouse,
                        stock_agg.c.sku == sku,
                    )
                )
            ).scalar_one_or_none()
        if last == expected:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(
        f"timed out waiting for {warehouse}/{sku} qty={expected}, last={last}"
    )


@pytest.mark.asyncio
async def test_receipt_via_redis_pipeline(db_conn, redis_conn):
    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert().values(
                sku="A-100",
                qty=0,
                warehouse="WH-1",
                event_type="receipt",
            )
        )
        await conn.execute(
            stock_agg.insert().values(warehouse="WH-1", sku="A-100", qty=0)
        )

    ready = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(db_conn, redis_conn, ready))
    try:
        await asyncio.wait_for(ready.wait(), timeout=5)
        await redis_conn.publish_with_retry(
            RECEIPT_CHANNEL,
            json.dumps({"sku": "A-100", "qty": 50, "warehouse": "WH-1"}),
        )

        await _wait_for_qty(db_conn, "WH-1", "A-100", expected=50)
    finally:
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task


@pytest.mark.asyncio
async def test_issue_via_redis_pipeline(db_conn, redis_conn):
    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert().values(
                sku="A-100",
                qty=50,
                warehouse="WH-1",
                event_type="receipt",
            )
        )
        await conn.execute(
            stock_agg.insert().values(warehouse="WH-1", sku="A-100", qty=50)
        )

    ready = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(db_conn, redis_conn, ready))
    try:
        await asyncio.wait_for(ready.wait(), timeout=5)
        await redis_conn.publish_with_retry(
            ISSUE_CHANNEL,
            json.dumps({"sku": "A-100", "qty": 20, "warehouse": "WH-1"}),
        )
        await _wait_for_qty(db_conn, "WH-1", "A-100", expected=30)
    finally:
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task


@pytest.mark.asyncio
async def test_seed_agg_from_events(db_conn):
    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert(),
            [
                {
                    "sku": "A-100",
                    "qty": 50,
                    "warehouse": "WH-1",
                    "event_type": "receipt",
                },
                {"sku": "A-100", "qty": 20, "warehouse": "WH-1", "event_type": "issue"},
                {
                    "sku": "A-100",
                    "qty": 10,
                    "warehouse": "WH-1",
                    "event_type": "receipt",
                },
                {
                    "sku": "B-200",
                    "qty": 5,
                    "warehouse": "WH-1",
                    "event_type": "receipt",
                },
                {"sku": "B-200", "qty": 3, "warehouse": "WH-1", "event_type": "issue"},
                {
                    "sku": "A-100",
                    "qty": 7,
                    "warehouse": "WH-2",
                    "event_type": "receipt",
                },
            ],
        )

    await seed_agg(db_conn)

    async with db_conn.connect() as conn:
        rows = (
            await conn.execute(
                select(
                    stock_agg.c.warehouse, stock_agg.c.sku, stock_agg.c.qty
                ).order_by(stock_agg.c.warehouse, stock_agg.c.sku)
            )
        ).all()

    assert rows == [
        ("WH-1", "A-100", 40),
        ("WH-1", "B-200", 2),
        ("WH-2", "A-100", 7),
    ]


@pytest.mark.asyncio
async def test_seed_agg_idempotent_and_updates(db_conn):
    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert().values(
                sku="A-100", qty=50, warehouse="WH-1", event_type="receipt"
            )
        )
    await seed_agg(db_conn)

    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert().values(
                sku="A-100", qty=20, warehouse="WH-1", event_type="issue"
            )
        )
    await seed_agg(db_conn)

    async with db_conn.connect() as conn:
        rows = (
            await conn.execute(
                select(stock_agg.c.warehouse, stock_agg.c.sku, stock_agg.c.qty)
            )
        ).all()

    assert rows == [("WH-1", "A-100", 30)]


@pytest.mark.asyncio
async def test_event_for_new_pair_creates_row(db_conn, redis_conn):
    ready = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(db_conn, redis_conn, ready))
    try:
        await asyncio.wait_for(ready.wait(), timeout=5)
        await redis_conn.publish_with_retry(
            RECEIPT_CHANNEL,
            json.dumps({"sku": "NEW-1", "qty": 42, "warehouse": "WH-9"}),
        )
        await _wait_for_qty(db_conn, "WH-9", "NEW-1", expected=42)
    finally:
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
