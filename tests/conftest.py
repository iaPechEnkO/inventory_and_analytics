import pytest
from sqlalchemy import delete
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from connections.redis_connector import RedisConnector
from database.db_connector import DBConnector
from database.tables import stock_events, stock_agg
from settings.config import USE_CONTAINERS_FOR_TESTS, database_url, redis_url


@pytest.fixture(scope="session")
def pg_container():
    if not USE_CONTAINERS_FOR_TESTS:
        yield None
        return
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def redis_container():
    if not USE_CONTAINERS_FOR_TESTS:
        yield None
        return
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture(scope="session")
def test_db_url(pg_container):
    if pg_container:
        return (
            pg_container.get_connection_url()
            .replace("+psycopg2", "+asyncpg")
            .replace("postgresql://", "postgresql+asyncpg://", 1)
        )
    return database_url()


@pytest.fixture(scope="session")
def test_redis_url(redis_container):
    if redis_container:
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        return f"redis://{host}:{port}/0"
    return redis_url()


@pytest.fixture
async def db_conn(test_db_url):
    connector = DBConnector(test_db_url)
    await connector.create_all()
    yield connector
    await connector.dispose()


@pytest.fixture(autouse=True)
async def clean_tables(db_conn):
    async with db_conn.begin() as conn:
        await conn.execute(delete(stock_events))
        await conn.execute(delete(stock_agg))
    yield


@pytest.fixture
async def redis_conn(test_redis_url):
    connector = RedisConnector(test_redis_url)
    yield connector
    await connector.close()
