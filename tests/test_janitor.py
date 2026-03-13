"""Database Janitor tests."""

import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from packaging.version import parse
from psycopg import AsyncCursor

from pytest_postgresql.janitor import AsyncDatabaseJanitor, DatabaseJanitor

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


# ---------------------------------------------------------------------------
# AsyncDatabaseJanitor -- init() / drop() / helper method tests
# ---------------------------------------------------------------------------


def _make_cursor_mock() -> MagicMock:
    """Create a mock async cursor that records execute() calls."""
    cur = AsyncMock(spec=AsyncCursor)
    return cur


def _make_cursor_context(cur: AsyncMock) -> Any:
    """Return an async context manager that yields the given cursor mock."""

    @asynccontextmanager
    async def _ctx(dbname: str = "postgres") -> AsyncIterator[AsyncMock]:
        yield cur

    return _ctx


@pytest.mark.asyncio
async def test_async_janitor_init_creates_database() -> None:
    """init() executes CREATE DATABASE with the configured dbname."""
    cur = _make_cursor_mock()
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", version=10)
    with patch.object(AsyncDatabaseJanitor, "cursor", _make_cursor_context(cur)):
        await janitor.init()

    executed_sql = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert 'CREATE DATABASE "mydb"' in executed_sql


@pytest.mark.asyncio
async def test_async_janitor_init_with_template() -> None:
    """init() uses TEMPLATE clause when template_dbname is set."""
    cur = _make_cursor_mock()
    janitor = AsyncDatabaseJanitor(
        user="user", host="host", port="1234", dbname="mydb", template_dbname="tmpl", version=10
    )
    with patch.object(AsyncDatabaseJanitor, "cursor", _make_cursor_context(cur)):
        await janitor.init()

    executed_sql = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert 'CREATE DATABASE "mydb" TEMPLATE "tmpl"' in executed_sql


@pytest.mark.asyncio
async def test_async_janitor_init_as_template() -> None:
    """init() appends IS_TEMPLATE = true when as_template is True."""
    cur = _make_cursor_mock()
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", as_template=True, version=10)
    with patch.object(AsyncDatabaseJanitor, "cursor", _make_cursor_context(cur)):
        await janitor.init()

    executed_sql = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "IS_TEMPLATE = true" in executed_sql


@pytest.mark.asyncio
async def test_async_janitor_drop_drops_database() -> None:
    """drop() executes DROP DATABASE IF EXISTS for the configured dbname."""
    cur = _make_cursor_mock()
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", version=10)
    with patch.object(AsyncDatabaseJanitor, "cursor", _make_cursor_context(cur)):
        await janitor.drop()

    executed_sql = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert 'DROP DATABASE IF EXISTS "mydb"' in executed_sql


@pytest.mark.asyncio
async def test_async_janitor_drop_as_template() -> None:
    """drop() resets is_template before dropping when as_template is True."""
    cur = _make_cursor_mock()
    janitor = AsyncDatabaseJanitor(user="user", host="host", port="1234", dbname="mydb", as_template=True, version=10)
    with patch.object(AsyncDatabaseJanitor, "cursor", _make_cursor_context(cur)):
        await janitor.drop()

    executed_sql = [str(c.args[0]) for c in cur.execute.call_args_list]
    assert any("is_template false" in s for s in executed_sql)
    assert any('DROP DATABASE IF EXISTS "mydb"' in s for s in executed_sql)
    # is_template false must come before DROP
    template_idx = next(i for i, s in enumerate(executed_sql) if "is_template false" in s)
    drop_idx = next(i for i, s in enumerate(executed_sql) if "DROP DATABASE" in s)
    assert template_idx < drop_idx


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


@pytest.mark.asyncio
async def test_async_janitor_dont_datallowconn_sql() -> None:
    """_dont_datallowconn() executes ALTER DATABASE allow_connections false for the dbname."""
    cur = AsyncMock(spec=AsyncCursor)
    await AsyncDatabaseJanitor._dont_datallowconn(cur, "target_db")

    cur.execute.assert_called_once()
    sql_str = cur.execute.call_args.args[0]
    assert "allow_connections false" in sql_str
    assert '"target_db"' in sql_str


def _make_async_conn_mock() -> MagicMock:
    """Create a MagicMock that behaves like a psycopg3 AsyncConnection."""
    conn = MagicMock()
    conn.set_isolation_level = AsyncMock()
    conn.set_autocommit = AsyncMock()
    conn.close = AsyncMock()
    cursor_mock = MagicMock()
    cursor_mock.__aenter__ = AsyncMock(return_value=MagicMock())
    cursor_mock.__aexit__ = AsyncMock(return_value=False)
    conn.cursor = MagicMock(return_value=cursor_mock)
    return conn
