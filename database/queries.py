from sqlalchemy import case, func, select
from database.tables import stock_events

signed_qty = case(
    (stock_events.c.event_type == "receipt", stock_events.c.qty),
    else_=-stock_events.c.qty,
).label("signed_qty")

stock_subq = (
    select(
        stock_events.c.warehouse,
        stock_events.c.sku,
        func.sum(signed_qty).label("qty"),
    )
    .group_by(stock_events.c.warehouse, stock_events.c.sku)
    .subquery()
)
