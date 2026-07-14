"""Minimal async smoke test for postgresql_async on Windows."""

import pytest
from psycopg import AsyncConnection

from pytest_postgresql import factories

postgresql_async = factories.postgresql_async("postgresql_noproc")


@pytest.mark.asyncio
async def test_postgresql_async_windows_smoke(postgresql_async: AsyncConnection) -> None:
    """Verify postgresql_async works without manual Windows event-loop configuration."""
    async with postgresql_async.cursor() as cur:
        await cur.execute("SELECT 1 AS n")
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1
