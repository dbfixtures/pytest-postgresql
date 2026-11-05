"""Real client fixtures exercise the factory case supplied by the parent test."""

import importlib.util
import json
from pathlib import Path

import pytest
from psycopg import AsyncConnection, Connection

from pytest_postgresql import factories
from pytest_postgresql.executors import NoopExecutor, PostgreSQLExecutor
from tests.loader import load_database
from tests.test_create_strategy import _assert_creation

case = json.loads(Path(__file__).with_name("factory_case.json").read_text(encoding="utf-8"))
configured_proc = getattr(factories, case["factory_name"])(load=[load_database], **case["factory_kwargs"])
client = factories.postgresql("configured_proc", dbname='factory"sync')
async_client = factories.postgresql_async("configured_proc", dbname='factory"async')


def check_creation(
    connection: Connection | AsyncConnection, configured_proc: PostgreSQLExecutor | NoopExecutor
) -> None:
    """Check the actual CREATE and session setting received by PostgreSQL."""

    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    suffix = "" if case["strategy"] is None else f" STRATEGY '{case['strategy']}'"
    expected = (
        f"CREATE DATABASE {quote(connection.info.dbname)} TEMPLATE {quote(configured_proc.template_dbname)}{suffix}"
    )
    logfile = configured_proc.logfile if isinstance(configured_proc, PostgreSQLExecutor) else case["logfile"]
    _assert_creation(logfile, connection.info.dbname, expected, case["copy_method"], offset=case["offset"])


def test_sync(client: Connection, configured_proc: PostgreSQLExecutor | NoopExecutor) -> None:
    """The synchronous fixture clones the seeded template with the requested mode."""
    with client.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM stories")
        assert cursor.fetchone() == (4,)
    check_creation(client, configured_proc)


@pytest.mark.asyncio
@pytest.mark.skipif(
    importlib.util.find_spec("pytest_asyncio") is None, reason="requires the optional pytest-asyncio extra"
)
async def test_async(async_client: AsyncConnection, configured_proc: PostgreSQLExecutor | NoopExecutor) -> None:
    """The asynchronous fixture uses the same template cloning options."""
    async with async_client.cursor() as cursor:
        await cursor.execute("SELECT count(*) FROM stories")
        assert await cursor.fetchone() == (4,)
    check_creation(async_client, configured_proc)
