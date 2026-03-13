"""Tests for factory error paths (missing optional dependencies)."""

import sys
from unittest.mock import patch

import pytest


def test_postgresql_async_raises_without_pytest_asyncio() -> None:
    """postgresql_async() raises ImportError with a helpful message when pytest_asyncio is not installed."""
    with patch.dict(sys.modules, {"pytest_asyncio": None}):
        from pytest_postgresql.factories.client import postgresql_async  # noqa: PLC0415

        with pytest.raises(ImportError, match="pytest-asyncio"):
            postgresql_async("some_proc_fixture")
