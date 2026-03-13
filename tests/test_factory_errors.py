"""Tests for factory error paths (missing optional dependencies)."""

from unittest.mock import patch

import pytest

from pytest_postgresql.factories.client import postgresql_async


def test_postgresql_async_raises_without_pytest_asyncio() -> None:
    """postgresql_async() raises ImportError with a helpful message when pytest_asyncio is not installed."""
    with patch("pytest_postgresql.factories.client.pytest_asyncio", None):
        with pytest.raises(ImportError, match="pytest-asyncio"):
            postgresql_async("some_proc_fixture")
