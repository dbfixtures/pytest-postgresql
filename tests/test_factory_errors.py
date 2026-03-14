"""Tests for factory error paths (missing optional dependencies)."""

import asyncio
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
    """The fixture body raises ImportError with a helpful message when pytest-asyncio is absent."""

    async def _invoke() -> None:
        with patch("pytest_postgresql.factories.client.pytest_asyncio", None):
            fixture_func = postgresql_async("some_proc_fixture")
            # pytest 8+ wraps fixtures to prevent direct calls; unwrap first.
            raw_func = getattr(fixture_func, "__wrapped__", fixture_func)
            async for _ in raw_func(None):  # type: ignore[arg-type]
                break  # pragma: no cover

    with pytest.raises(ImportError, match="pytest-asyncio"):
        asyncio.run(_invoke())
