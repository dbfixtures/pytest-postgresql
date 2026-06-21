"""Database Janitor tests."""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest
from packaging.version import parse
from psycopg import AsyncCursor

from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.factories.noprocess import xdistify_dbname
from pytest_postgresql.janitor import AsyncDatabaseJanitor, DatabaseJanitor

TEST_SQL_FILE = Path(__file__).resolve().parent / "test_sql" / "test.sql"

VERSION = parse("10")


@pytest.mark.parametrize("version", (VERSION, 10, "10"))
def test_version_cast(version: Any) -> None:
    """Test that version is cast to Version object."""
    janitor = DatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=version)
    assert janitor.version == VERSION


@pytest.mark.parametrize("version", (VERSION, 10, "10"))
@pytest.mark.asyncio
async def test_version_cast_async(version: Any) -> None:
    """Async test that version is cast to Version object."""
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=version)
    assert janitor.version == VERSION


@patch("pytest_postgresql.janitor.psycopg.connect")
def test_cursor_selects_postgres_database(connect_mock: MagicMock) -> None:
    """Test that the cursor requests the postgres database."""
    janitor = DatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=10)
    with janitor.cursor():
        connect_mock.assert_called_once_with(dbname="postgres", user="user", password=None, host="host", port="1234")


@pytest.mark.asyncio
async def test_cursor_selects_postgres_database_async() -> None:
    """Async test that the cursor requests the postgres database."""
    conn_mock = _make_async_conn_mock()
    connect_mock = AsyncMock(return_value=conn_mock)
    with patch("pytest_postgresql.janitor.psycopg.AsyncConnection.connect", connect_mock):
        janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=10)
        async with janitor.cursor():
            connect_mock.assert_called_once_with(
                dbname="postgres", user="user", password=None, host="host", port="1234"
            )


@patch("pytest_postgresql.janitor.psycopg.connect")
def test_cursor_connects_with_password(connect_mock: MagicMock) -> None:
    """Test that the cursor requests the postgres database."""
    janitor = DatabaseJanitor(
        user="user",
        host="host",
        port="1234",
        dbname="database_name",
        version=10,
        password="some_password",  # noqa: S106
    )
    with janitor.cursor():
        connect_mock.assert_called_once_with(
            dbname="postgres", user="user", password="some_password", host="host", port="1234"
        )


@pytest.mark.asyncio
async def test_cursor_connects_with_password_async() -> None:
    """Async test that the cursor requests the postgres database with password."""
    conn_mock = _make_async_conn_mock()
    connect_mock = AsyncMock(return_value=conn_mock)
    with patch("pytest_postgresql.janitor.psycopg.AsyncConnection.connect", connect_mock):
        janitor = AsyncDatabaseJanitor(
            user="user",
            host="host",
            port="1234",
            dbname="database_name",
            version=10,
            password="some_password",  # noqa: S106
        )
        async with janitor.cursor():
            connect_mock.assert_called_once_with(
                dbname="postgres", user="user", password="some_password", host="host", port="1234"
            )


@pytest.mark.asyncio
async def test_cursor_custom_dbname_async() -> None:
    """Test that a custom dbname is forwarded to the connection in AsyncDatabaseJanitor.cursor."""
    conn_mock = _make_async_conn_mock()
    connect_mock = AsyncMock(return_value=conn_mock)
    with patch("pytest_postgresql.janitor.psycopg.AsyncConnection.connect", connect_mock):
        janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=10)
        async with janitor.cursor(dbname="custom_db"):
            connect_mock.assert_called_once_with(
                dbname="custom_db", user="user", password=None, host="host", port="1234"
            )


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Unittest call_args.kwargs was introduced since python 3.8")
@pytest.mark.parametrize("load_database", ("tests.loader.load_database", "tests.loader:load_database"))
@patch("pytest_postgresql.janitor.psycopg.connect")
def test_janitor_populate(connect_mock: MagicMock, load_database: str) -> None:
    """Test that the cursor requests the postgres database.

    load_database tries to connect to database, which triggers mocks.
    """
    call_kwargs = {
        "host": "host",
        "port": "1234",
        "user": "user",
        "dbname": "database_name",
        "password": "some_password",  # noqa: S106
    }
    janitor = DatabaseJanitor(version=10, **call_kwargs)  # type: ignore[arg-type]
    janitor.load(load_database)
    assert connect_mock.called
    assert connect_mock.call_args.kwargs == call_kwargs


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Unittest call_args.kwargs was introduced since python 3.8")
@pytest.mark.parametrize("load_database", ("tests.loader.load_database", "tests.loader:load_database"))
@patch("tests.loader.psycopg.connect")
@pytest.mark.asyncio
async def test_janitor_populate_async(connect_mock: MagicMock, load_database: str) -> None:
    """Async test that the cursor requests the postgres database and populates.

    load_database (synchronous) uses psycopg.connect, so we mock that.
    """
    call_kwargs = {
        "host": "host",
        "port": "1234",
        "user": "user",
        "dbname": "database_name",
        "password": "some_password",  # noqa: S106
    }
    janitor = AsyncDatabaseJanitor(version=10, **call_kwargs)  # type: ignore[arg-type]
    await janitor.load(load_database)
    assert connect_mock.called
    assert connect_mock.call_args.kwargs == call_kwargs


@pytest.mark.asyncio
async def test_janitor_populate_async_awaitable_loader() -> None:
    """AsyncDatabaseJanitor.load awaits async loader callables."""
    call_kwargs = {
        "host": "host",
        "port": "1234",
        "user": "user",
        "dbname": "database_name",
        "password": "some_password",  # noqa: S106
    }
    loader_mock = AsyncMock()

    async def async_loader(**kwargs: object) -> None:
        await loader_mock(**kwargs)

    janitor = AsyncDatabaseJanitor(version=10, **call_kwargs)  # type: ignore[arg-type]
    await janitor.load(async_loader)
    loader_mock.assert_awaited_once_with(**call_kwargs)


@pytest.mark.asyncio
async def test_janitor_populate_async_sql_path(postgresql_proc: PostgreSQLExecutor) -> None:
    """AsyncDatabaseJanitor.load executes SQL from a Path via sql_async against live PostgreSQL."""
    dbname = xdistify_dbname("sql_async_load")
    janitor = AsyncDatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
        connection_timeout=5,
    )
    async with janitor:
        await janitor.load(TEST_SQL_FILE)
        async with await psycopg.AsyncConnection.connect(
            dbname=dbname,
            user=postgresql_proc.user,
            password=postgresql_proc.password,
            host=postgresql_proc.host,
            port=postgresql_proc.port,
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM test_load")
                rows = await cur.fetchall()
                assert len(rows) == 1


# ---------------------------------------------------------------------------
# AsyncDatabaseJanitor -- init() / drop() integration tests
# ---------------------------------------------------------------------------


async def _database_exists(proc: PostgreSQLExecutor, dbname: str) -> bool:
    async with await psycopg.AsyncConnection.connect(
        dbname="postgres",
        user=proc.user,
        password=proc.password,
        host=proc.host,
        port=proc.port,
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            return await cur.fetchone() is not None


async def _database_is_template(proc: PostgreSQLExecutor, dbname: str) -> bool:
    async with await psycopg.AsyncConnection.connect(
        dbname="postgres",
        user=proc.user,
        password=proc.password,
        host=proc.host,
        port=proc.port,
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT datistemplate FROM pg_database WHERE datname = %s", (dbname,))
            row = await cur.fetchone()
            return bool(row and row[0])


@pytest.mark.asyncio
async def test_async_janitor_init_and_drop(postgresql_proc: PostgreSQLExecutor) -> None:
    """init() creates a database and drop() removes it against live PostgreSQL."""
    dbname = xdistify_dbname("async_janitor_lifecycle")
    janitor = AsyncDatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
        connection_timeout=5,
    )
    await janitor.init()
    assert await _database_exists(postgresql_proc, dbname)
    await janitor.drop()
    assert not await _database_exists(postgresql_proc, dbname)


@pytest.mark.asyncio
async def test_async_janitor_template_flag_and_context_manager(postgresql_proc: PostgreSQLExecutor) -> None:
    """as_template marks the database as a template and async with drops it cleanly."""
    dbname = xdistify_dbname("async_janitor_tmpl")
    janitor = AsyncDatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
        as_template=True,
        connection_timeout=5,
    )
    async with janitor:
        assert await _database_is_template(postgresql_proc, dbname)
    assert not await _database_exists(postgresql_proc, dbname)


@pytest.mark.asyncio
async def test_async_janitor_creates_database_from_template(postgresql_proc: PostgreSQLExecutor) -> None:
    """init() clones schema and data from a template database."""
    base_dbname = xdistify_dbname("async_janitor_tmpl_base")
    clone_dbname = xdistify_dbname("async_janitor_tmpl_clone")
    base_janitor = AsyncDatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=base_dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
        as_template=True,
        connection_timeout=5,
    )
    clone_janitor = AsyncDatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=clone_dbname,
        template_dbname=base_dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
        connection_timeout=5,
    )
    try:
        await base_janitor.init()
        await base_janitor.load(TEST_SQL_FILE)
        await clone_janitor.init()
        async with await psycopg.AsyncConnection.connect(
            dbname=clone_dbname,
            user=postgresql_proc.user,
            password=postgresql_proc.password,
            host=postgresql_proc.host,
            port=postgresql_proc.port,
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM test_load")
                rows = await cur.fetchall()
                assert len(rows) == 1
    finally:
        await clone_janitor.drop()
        await base_janitor.drop()

    assert not await _database_exists(postgresql_proc, clone_dbname)
    assert not await _database_exists(postgresql_proc, base_dbname)


# ---------------------------------------------------------------------------
# AsyncDatabaseJanitor -- lightweight unit tests
# ---------------------------------------------------------------------------


def test_async_janitor_is_template_false() -> None:
    """is_template() returns False when as_template is not set."""
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", version=10)
    assert janitor.is_template() is False


def test_async_janitor_is_template_true() -> None:
    """is_template() returns True when as_template=True."""
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", as_template=True, version=10)
    assert janitor.is_template() is True


@pytest.mark.asyncio
async def test_async_janitor_context_manager_calls_init_and_drop() -> None:
    """__aenter__ calls init() and __aexit__ calls drop()."""
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", version=10)
    init_mock = AsyncMock()
    drop_mock = AsyncMock()
    with patch.object(AsyncDatabaseJanitor, "init", init_mock), patch.object(AsyncDatabaseJanitor, "drop", drop_mock):
        async with janitor:
            init_mock.assert_called_once()
            drop_mock.assert_not_called()
        drop_mock.assert_called_once()


@pytest.mark.asyncio
async def test_async_janitor_terminate_connection_sql() -> None:
    """_terminate_connection() executes pg_terminate_backend query with correct dbname."""
    cur = AsyncMock(spec=AsyncCursor)
    await AsyncDatabaseJanitor._terminate_connection(cur, "target_db")

    cur.execute.assert_called_once()
    sql_str, params = cur.execute.call_args.args
    assert "pg_terminate_backend" in sql_str
    assert params == ("target_db",)


def _make_async_conn_mock() -> MagicMock:
    """Create a MagicMock that behaves like a psycopg3 AsyncConnection."""
    conn = MagicMock()
    conn.close = AsyncMock()
    conn.set_isolation_level = AsyncMock()
    conn.set_autocommit = AsyncMock()
    cursor_mock = MagicMock()
    cursor_mock.__aenter__ = AsyncMock(return_value=MagicMock())
    cursor_mock.__aexit__ = AsyncMock(return_value=False)
    conn.cursor = MagicMock(return_value=cursor_mock)
    return conn
