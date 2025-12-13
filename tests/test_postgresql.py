"""All tests for pytest-postgresql."""

import decimal

import pytest
from psycopg import AsyncConnection, Connection
from psycopg.pq import ConnStatus

from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.retry import retry
from tests.conftest import POSTGRESQL_VERSION

MAKE_Q = "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
SELECT_Q = "SELECT * FROM test_load;"


def test_postgresql_proc(postgresql_proc: PostgreSQLExecutor) -> None:
    """Test different postgresql versions."""
    assert postgresql_proc.running() is True


def test_main_postgres(postgresql: Connection) -> None:
    """Check main postgresql fixture."""
    cur = postgresql.cursor()
    cur.execute(MAKE_Q)
    postgresql.commit()
    cur.close()


def test_two_postgreses(postgresql: Connection, postgresql2: Connection) -> None:
    """Check two postgresql fixtures on one test."""
    cur = postgresql.cursor()
    cur.execute(MAKE_Q)
    postgresql.commit()
    cur.close()

    cur = postgresql2.cursor()
    cur.execute(MAKE_Q)
    postgresql2.commit()
    cur.close()


def test_postgres_load_two_files(postgresql_load_1: Connection) -> None:
    """Check postgresql fixture can load two files."""
    cur = postgresql_load_1.cursor()
    cur.execute(SELECT_Q)
    results = cur.fetchall()
    assert len(results) == 2
    cur.close()


def test_rand_postgres_port(postgresql2: Connection) -> None:
    """Check if postgres fixture can be started on random port."""
    assert postgresql2.info.status == ConnStatus.OK


@pytest.mark.skipif(
    decimal.Decimal(POSTGRESQL_VERSION) < 10,
    reason="Test query not supported in those postgresql versions, and soon will not be supported.",
)
@pytest.mark.parametrize("_", range(2))
def test_postgres_terminate_connection(postgresql2: Connection, _: int) -> None:
    """Test that connections are terminated between tests.

    And check that only one exists at a time.
    """
    with postgresql2.cursor() as cur:

        def check_if_one_connection() -> None:
            cur.execute("SELECT * FROM pg_stat_activity WHERE backend_type = 'client backend';")
            existing_connections = cur.fetchall()
            assert len(existing_connections) == 1, f"there is always only one connection, {existing_connections}"

        retry(check_if_one_connection, timeout=120, possible_exception=AssertionError)


@pytest.mark.asyncio
async def test_main_postgres_async(postgresql_async: AsyncConnection) -> None:
    """Async check main postgresql fixture."""
    async with postgresql_async.cursor() as cur:
        await cur.execute(MAKE_Q)
        await postgresql_async.commit()


@pytest.mark.asyncio
async def test_two_postgreses_async(postgresql_async: AsyncConnection, postgresql2_async: AsyncConnection) -> None:
    """Async check two postgresql fixtures on one test (async)."""
    async with postgresql_async.cursor() as cur:
        await cur.execute(MAKE_Q)
        await postgresql_async.commit()

    async with postgresql2_async.cursor() as cur:
        await cur.execute(MAKE_Q)
        await postgresql2_async.commit()


@pytest.mark.asyncio
async def test_postgres_load_two_files_async(postgresql_load_1_async: AsyncConnection) -> None:
    """Async check postgresql fixture can load two files."""
    async with postgresql_load_1_async.cursor() as cur:
        await cur.execute(SELECT_Q)
        results = await cur.fetchall()
        assert len(results) == 2


@pytest.mark.asyncio
async def test_rand_postgres_port_async(postgresql2_async: AsyncConnection) -> None:
    """Async check if postgres fixture can be started on random port."""
    assert postgresql2_async.info.status == ConnStatus.OK


@pytest.mark.asyncio
@pytest.mark.parametrize("_", range(2))
async def test_postgres_terminate_connection_async(postgresql2_async: AsyncConnection, _: int) -> None:
    """Async test that connections are terminated between tests.

    And check that only one exists at a time.
    """
    async with postgresql2_async.cursor() as cur:

        async def check_if_one_connection() -> None:
            await cur.execute("SELECT * FROM pg_stat_activity WHERE backend_type = 'client backend';")
            existing_connections = await cur.fetchall()
            assert len(existing_connections) == 1, f"there is always only one connection, {existing_connections}"

        await retry(check_if_one_connection, timeout=120, possible_exception=AssertionError)
