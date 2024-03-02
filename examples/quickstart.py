"""The README quickstart: a durable checkout with parallel activities, a human approval
signal, a durable timer and an idempotent side effect.

Run: ``uv run python examples/quickstart.py``
"""

from __future__ import annotations

from typing import Any

from replayagent import (
    App,
    Client,
    RetryPolicy,
    Store,
    Worker,
    WorkflowContext,
    activity,
    activity_info,
    workflow,
)


@activity(retry=RetryPolicy(initial_interval=0.1, maximum_attempts=5))
def fetch_price(sku: str) -> int:
    return {"apple": 3, "pear": 4}[sku]  # any I/O is fine here: it runs once per attempt


@activity
def send_invoice(total: int) -> str:
    key = activity_info().idempotency_key  # stable across retries and crashes
    return f"invoice {key}: {total}"


@workflow
def checkout(ctx: WorkflowContext, order: dict[str, Any]) -> dict[str, Any]:
    prices = ctx.gather(*(ctx.start_activity(fetch_price, sku) for sku in order["items"]))
    if not ctx.wait_signal("approve", timeout=3600):  # human in the loop, durable
        return {"status": "rejected"}
    ctx.sleep(1)  # durable timer: survives restarts
    return {"status": "sent", "receipt": ctx.activity(send_invoice, sum(prices))}


def main() -> None:
    app = App(workflows=[checkout], activities=[fetch_price, send_invoice])
    store = Store("quickstart.db")
    client = Client(store, app)
    wid = client.start(checkout, {"items": ["apple", "pear"]}, workflow_id="order-1")
    client.signal(wid, "approve", True)
    with Worker(store, app) as worker:
        worker.run_until_complete(wid)
    print(client.result(wid))
    store.close()


if __name__ == "__main__":
    main()
