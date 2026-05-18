"""Tests for factory error paths (missing optional dependencies)."""

from unittest.mock import patch

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
