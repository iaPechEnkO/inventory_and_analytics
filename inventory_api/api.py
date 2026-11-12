from aiohttp import web

from inventory_api.inventory_logic import record_event, get_stock, get_summary
from settings.logger import setup_logger

logger = setup_logger(__name__)


async def _handle_event(request, event_type: str):
    try:
        data = await request.json()
        sku = data["sku"]
        qty = data["qty"]
        warehouse = data["warehouse"]

        if not isinstance(qty, int) or qty <= 0:
            raise ValueError("qty must be a positive integer")
        if not isinstance(sku, str) or not sku:
            raise ValueError("sku must be a non-empty string")
        if not isinstance(warehouse, str) or not warehouse:
            raise ValueError("warehouse must be a non-empty string")

        await record_event(
            db=request.app["db"],
            redis_conn=request.app["redis"],
            event_type=event_type,
            sku=sku,
            qty=qty,
            warehouse=warehouse,
        )
    except (KeyError, ValueError) as k_err:
        error_message = "An error occurred during unpacking request to json"
        logger.error(f"{error_message}: {k_err}")
        return web.json_response(
            {"status": "error", "error": error_message},
            status=422,
        )
    except Exception as err:
        error_message = "An error occurred during recording an event"
        logger.error(f"{error_message}: {err}")
        return web.json_response(
            {"status": "error", "error": error_message},
            status=500,
        )

    return web.json_response({"status": "ok"}, status=201)


async def receipt(request):
    return await _handle_event(request, "receipt")


async def issue(request):
    return await _handle_event(request, "issue")


async def stock(request):
    try:
        warehouse = request.query.get("warehouse")
        sku = request.query.get("sku")

        items = await get_stock(
            db=request.app["db"],
            warehouse=warehouse,
            sku=sku,
        )
    except Exception as err:
        error_message = "An error occurred during stocking"
        logger.error(f"{error_message}: {err}")
        return web.json_response(
            {"items": None, "error": error_message},
            status=500,
        )

    return web.json_response({"items": items})


async def summary(request):
    try:
        top_n = int(request.query.get("top_n", 5))
        if top_n <= 0:
            raise ValueError
        warehouses = await get_summary(db=request.app["db"], top_n=top_n)
    except ValueError as k_err:
        error_message = "<top_n> incorrect value"
        logger.error(f"{error_message}: {k_err}")
        return web.json_response(
            {"warehouses": None, "top_skus": None, "error": error_message},
            status=422,
        )
    except Exception as err:
        error_message = "An error occurred during summary"
        logger.error(f"{error_message}: {err}")
        return web.json_response(
            {"warehouses": None, "top_skus": None, "error": error_message},
            status=500,
        )

    return web.json_response(warehouses)
