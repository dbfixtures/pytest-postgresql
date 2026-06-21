"""Tests for factory error paths (missing optional dependencies)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pytest_postgresql.factories.client import postgresql_async


def test_postgresql_async_factory_creation_succeeds_without_pytest_asyncio() -> None:
    """postgresql_async() must not raise at factory-creation time when pytest-asyncio is absent.

    The plugin registers ``postgresql_async`` at load time (plugin.py), so raising here
    would break all users — including those who only use synchronous fixtures.
    """
    with patch("pytest_postgresql.factories.client.pytest_asyncio", None):
        fixture_func = postgresql_async("some_proc_fixture")
    assert callable(fixture_func)


def test_postgresql_async_raises_on_use_without_pytest_asyncio() -> None:
    """When pytest-asyncio is absent, the registered stub is synchronous and raises ImportError.

    A synchronous stub avoids the "coroutine was never awaited" warning that would
    result from registering an async def with plain pytest.fixture.
    """
    with patch("pytest_postgresql.factories.client.pytest_asyncio", None):
        fixture_func = postgresql_async("some_proc_fixture")
        # pytest 8+ wraps fixtures to prevent direct calls; unwrap first.
        raw_func = getattr(fixture_func, "__wrapped__", fixture_func)
        assert not hasattr(raw_func, "__await__"), "stub must be a sync function, not a coroutine"
        with pytest.raises(ImportError, match="pytest-asyncio"):
            raw_func(None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_postgresql_async_drops_database_when_configured() -> None:
    """Async fixture calls janitor.drop() when drop_test_database is configured."""
    fixture_func = postgresql_async("proc_fixture")
    raw_func = getattr(fixture_func, "__wrapped__", fixture_func)

    proc_mock = MagicMock()
    proc_mock.host = "127.0.0.1"
    proc_mock.port = 5432
    proc_mock.user = "postgres"
    proc_mock.password = None
    proc_mock.options = None
    proc_mock.dbname = "tests"
    proc_mock.template_dbname = "template_tests"
    proc_mock.version = 14

    janitor_mock = AsyncMock()
    janitor_mock.__aenter__ = AsyncMock(return_value=janitor_mock)
    janitor_mock.__aexit__ = AsyncMock(return_value=False)
    janitor_mock.drop = AsyncMock()

    conn_mock = AsyncMock()
    conn_mock.close = AsyncMock()

    request_mock = MagicMock()
    request_mock.getfixturevalue.return_value = proc_mock

    with (
        patch("pytest_postgresql.factories.client.AsyncDatabaseJanitor", return_value=janitor_mock),
        patch(
            "pytest_postgresql.factories.client.AsyncConnection.connect",
            new_callable=AsyncMock,
            return_value=conn_mock,
        ),
        patch("pytest_postgresql.factories.client.get_config") as get_config_mock,
    ):
        config_mock = MagicMock()
        config_mock.drop_test_database = True
        get_config_mock.return_value = config_mock

        agen = raw_func(request_mock)
        conn = await agen.__anext__()
        assert conn is conn_mock
        janitor_mock.drop.assert_awaited_once()
        with pytest.raises(StopAsyncIteration):
            await agen.__anext__()

    conn_mock.close.assert_awaited_once()
