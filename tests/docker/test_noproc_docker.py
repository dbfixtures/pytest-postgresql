"""Noproc fixture tests."""

import pathlib

import pytest
from psycopg import AsyncConnection, Connection

import pytest_postgresql.factories.client
import pytest_postgresql.factories.noprocess
from tests.loader import load_database

postgresql_my_proc = pytest_postgresql.factories.noprocess.postgresql_noproc(
    load=[pathlib.Path("tests/test_sql/eidastats.sql")]
)
postgres_with_schema = pytest_postgresql.factories.client.postgresql("postgresql_my_proc")

async_postgres_with_schema = pytest_postgresql.factories.client.postgresql_async("postgresql_my_proc")

postgresql_my_proc_template = pytest_postgresql.factories.noprocess.postgresql_noproc(
    dbname="stories_templated", load=[load_database]
)
postgres_with_template = pytest_postgresql.factories.client.postgresql(
    "postgresql_my_proc_template", dbname="stories_templated"
)
async_postgres_with_template = pytest_postgresql.factories.client.postgresql_async(
    "postgresql_my_proc_template", dbname="stories_templated"
)


def test_postgres_docker_load(postgres_with_schema: Connection) -> None:
    """Check main postgres fixture."""
    with postgres_with_schema.cursor() as cur:
        # Query for public.tokens since the eidastats changes postgres' search_path to ''.
        # The search path by default is public, but without it,
        # every schema has to be written explicitly.
        cur.execute("select * from public.tokens")
        print(cur.fetchall())


@pytest.mark.asyncio
async def test_postgres_docker_load_async(async_postgres_with_schema: AsyncConnection) -> None:
    """Async check main postgres fixture."""
    async with async_postgres_with_schema.cursor() as cur:
        await cur.execute("select * from public.tokens")
        print(await cur.fetchall())


@pytest.mark.parametrize("_", range(5))
def test_template_database(postgres_with_template: Connection, _: int) -> None:
    """Check that the database structure gets recreated out of a template."""
    with postgres_with_template.cursor() as cur:
        cur.execute("SELECT * FROM stories")
        res = cur.fetchall()
        assert len(res) == 4
        cur.execute("TRUNCATE stories")
        cur.execute("SELECT * FROM stories")
        res = cur.fetchall()
        assert len(res) == 0


# Async version of test_template_database
@pytest.mark.asyncio
@pytest.mark.parametrize("_", range(5))
async def test_template_database_async(async_postgres_with_template, _: int) -> None:
    """Async check that the database structure gets recreated out of a template."""
    async with async_postgres_with_template.cursor() as cur:
        await cur.execute("SELECT * FROM stories")
        rows = await cur.fetchall()
        assert len(rows) == 4
        await cur.execute("TRUNCATE stories")
        await cur.execute("SELECT * FROM stories")
        rows = await cur.fetchall()
        assert len(rows) == 0
