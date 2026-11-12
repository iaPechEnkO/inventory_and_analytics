import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy import select

from database.tables import stock_events
from inventory_api.api import receipt, issue, stock, summary


@pytest.fixture
async def api_client(db_conn, redis_conn):
    app = web.Application()
    app["db"] = db_conn
    app["redis"] = redis_conn
    app.router.add_post("/receipts", receipt)
    app.router.add_post("/issues", issue)
    app.router.add_get("/stock", stock)
    app.router.add_get("/stock/summary", summary)

    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_receipt_creates_event_and_publishes(api_client, db_conn, redis_conn):
    pubsub = redis_conn.pubsub()
    try:
        await pubsub.subscribe("inventory.receipt")
        await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

        resp = await api_client.post(
            "/receipts", json={"sku": "A-100", "qty": 50, "warehouse": "WH-1"}
        )
        assert resp.status == 201
        assert await resp.json() == {"status": "ok"}

        async with db_conn.connect() as conn:
            rows = (await conn.execute(select(stock_events))).all()
        assert len(rows) == 1
        assert (
            rows[0].sku == "A-100"
            and rows[0].qty == 50
            and rows[0].event_type == "receipt"
        )

        msg = await asyncio.wait_for(
            pubsub.get_message(ignore_subscribe_messages=True), timeout=2.0
        )
        assert msg is not None and msg["channel"] == "inventory.receipt"
    finally:
        await pubsub.aclose()


@pytest.mark.asyncio
async def test_issue_creates_event(api_client, db_conn):
    resp = await api_client.post(
        "/issues", json={"sku": "A-100", "qty": 20, "warehouse": "WH-1"}
    )
    assert resp.status == 201
    async with db_conn.connect() as conn:
        row = (await conn.execute(select(stock_events))).one()
    assert row.event_type == "issue" and row.qty == 20


@pytest.mark.asyncio
async def test_receipt_validation_errors(api_client):
    resp = await api_client.post("/receipts", json={"sku": "A-100", "qty": 50})
    assert resp.status == 422
    body = await resp.json()
    assert body["status"] == "error"
    resp = await api_client.post(
        "/receipts", json={"sku": "A-100", "qty": "50", "warehouse": "WH-1"}
    )
    assert resp.status == 422
    resp = await api_client.post(
        "/receipts", json={"sku": "A-100", "qty": -5, "warehouse": "WH-1"}
    )
    assert resp.status == 422


@pytest.mark.asyncio
async def test_stock_returns_aggregates(api_client, db_conn):
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
                    "sku": "B-200",
                    "qty": 10,
                    "warehouse": "WH-2",
                    "event_type": "receipt",
                },
            ],
        )

    resp = await api_client.get("/stock?warehouse=WH-1")
    assert resp.status == 200
    body = await resp.json()
    assert body["items"] == [{"warehouse": "WH-1", "sku": "A-100", "qty": 30}]

    resp = await api_client.get("/stock?warehouse=WH-1&sku=A-100")
    body = await resp.json()
    assert body["items"] == [{"warehouse": "WH-1", "sku": "A-100", "qty": 30}]

    resp = await api_client.get("/stock")
    body = await resp.json()
    assert len(body["items"]) == 2


@pytest.mark.asyncio
async def test_summary_top_n(api_client, db_conn):
    async with db_conn.begin() as conn:
        await conn.execute(
            stock_events.insert(),
            [
                {
                    "sku": "A-100",
                    "qty": 100,
                    "warehouse": "WH-1",
                    "event_type": "receipt",
                },
                {
                    "sku": "B-200",
                    "qty": 50,
                    "warehouse": "WH-1",
                    "event_type": "receipt",
                },
                {
                    "sku": "C-300",
                    "qty": 30,
                    "warehouse": "WH-2",
                    "event_type": "receipt",
                },
            ],
        )

    resp = await api_client.get("/stock/summary?top_n=2")
    assert resp.status == 200
    body = await resp.json()
    assert "warehouses" in body
    assert "top_skus" in body
    whs = {w["warehouse"]: w for w in body["warehouses"]}
    assert whs["WH-1"]["total_qty"] == 150
    assert whs["WH-1"]["sku_count"] == 2
    assert whs["WH-2"]["total_qty"] == 30
    assert len(body["top_skus"]) == 2
    assert body["top_skus"][0]["sku"] == "A-100"


@pytest.mark.asyncio
async def test_summary_invalid_top_n(api_client):
    resp = await api_client.get("/stock/summary?top_n=0")
    assert resp.status == 422
    resp = await api_client.get("/stock/summary?top_n=abc")
    assert resp.status == 422
