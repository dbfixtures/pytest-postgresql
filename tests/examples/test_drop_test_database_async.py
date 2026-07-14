"""Async tests for pytest-postgresql drop-test-database behaviour."""

import pytest
from psycopg import AsyncConnection

from pytest_postgresql import factories

postgresql_async = factories.postgresql_async("postgresql_noproc")


@pytest.mark.asyncio
async def test_postgres_load_override_async(postgresql_async: AsyncConnection) -> None:
    """Check postgresql_async can load one file and override a pre-existing database."""
    async with postgresql_async.cursor() as cur:
        await cur.execute("SELECT * FROM test;")
        results = await cur.fetchall()
        assert len(results) == 1
