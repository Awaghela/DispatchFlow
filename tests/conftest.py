"""Integration-test fixtures.

Needs PostgreSQL and Redis on localhost (python scripts/stackctl.py up postgres redis, or docker compose up -d
postgres redis). SQS is provided by an in-process moto server, so no AWS account or LocalStack is required.
"""
import asyncio
import os
import socket

import pytest

# Must be set before any `app.*` import: settings and the DB engine are created at import time.
os.environ.update(
    DATABASE_URL=os.getenv("TEST_DATABASE_URL", "postgresql+asyncpg://dispatch:dispatch@localhost:5432/dispatchflow_test"),
    REDIS_URL=os.getenv("TEST_REDIS_URL", "redis://localhost:6379/9"),
    AWS_ENDPOINT_URL="http://127.0.0.1:5566",
    AWS_ACCESS_KEY_ID="test",
    AWS_SECRET_ACCESS_KEY="test",
    AWS_DEFAULT_REGION="us-east-1",
    AWS_REGION="us-east-1",
    SQS_AUTO_CREATE="true",
    SQS_QUEUE_NAME="df-test-orders",
    SQS_DLQ_NAME="df-test-orders-dlq",
    SQS_VISIBILITY_TIMEOUT="5",
    MAX_ASSIGN_ATTEMPTS="3",
    RETRY_BASE_DELAY="1",
    LOG_LEVEL="WARNING",
)


def _port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def pytest_collection_modifyitems(config, items):
    if not (_port_open(5432) and _port_open(6379)):
        skip = pytest.mark.skip(reason="PostgreSQL (5432) and Redis (6379) must be running: python scripts/stackctl.py up postgres redis")
        for item in items:
            if "test_domain" not in item.nodeid:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def moto_server():
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=5566, verbose=False)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
async def database():
    import asyncpg

    from app.db import engine, init_db

    url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
    admin = await asyncpg.connect(url.rsplit("/", 1)[0] + "/postgres")
    if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname='dispatchflow_test'"):
        await admin.execute("CREATE DATABASE dispatchflow_test")
    await admin.close()
    await init_db()
    yield
    await engine.dispose()


@pytest.fixture(autouse=True)
async def clean(database):
    """Fresh state for every test: empty tables, empty Redis db, healthy circuit breaker."""
    from sqlalchemy import text

    from app.db import engine
    from app.redis_store import store

    if store.client is None:
        await store.start()
    store._open_until = 0.0
    async with engine.begin() as c:
        await c.execute(text("TRUNCATE orders, couriers, order_events, outbox CASCADE"))
    await store.client.flushdb()
    yield


@pytest.fixture
async def queue(moto_server):
    from app.queue import SQSQueue

    q = SQSQueue()
    await q.start()
    await q.client.purge_queue(QueueUrl=q.url)
    await q.client.purge_queue(QueueUrl=q.dlq_url)
    yield q
    await q.stop()


@pytest.fixture(scope="session")
async def server(database):
    """A real uvicorn server on the test event loop (HTTP + WebSocket)."""
    import uvicorn

    from app.main import app

    cfg = uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning", lifespan="on")
    srv = uvicorn.Server(cfg)
    task = asyncio.create_task(srv.serve())
    for _ in range(100):
        if srv.started:
            break
        await asyncio.sleep(0.05)
    yield "http://127.0.0.1:8765"
    srv.should_exit = True
    await task
