"""Tests for the `build_loader` function."""

from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

from pytest_postgresql.loader import build_loader, build_loader_async, sql, sql_async
from tests.loader import load_database


def test_loader_callables() -> None:
    """Test handling callables in build_loader."""
    assert load_database == build_loader(load_database)
    assert load_database == build_loader("tests.loader:load_database")


def test_loader_callables_dot_separator() -> None:
    """Test dot-separated import path resolves the same callable as colon-separated."""
    assert build_loader("tests.loader.load_database") == load_database


def test_loader_deeply_nested_import_path() -> None:
    """All path segments before the final delimiter are joined as the import path."""
    sentinel = object()
    fake_module = ModuleType("fake_module")
    fake_module.my_loader = sentinel  # type: ignore[attr-defined]
    with patch("pytest_postgresql.loader.importlib.import_module", return_value=fake_module) as import_mock:
        result = build_loader("a.b.c.d:my_loader")
    import_mock.assert_called_once_with("a.b.c.d")
    assert result is sentinel


@pytest.mark.asyncio
async def test_loader_callables_async() -> None:
    """Async test handling callables in build_loader_async."""
    assert load_database == build_loader_async(load_database)
    assert load_database == build_loader_async("tests.loader:load_database")

    async def afun(*_args: object, **_kwargs: object) -> int:
        return 0

    assert afun == build_loader_async(afun)


@pytest.mark.asyncio
async def test_loader_callables_async_dot_separator() -> None:
    """Dot-separated import path is resolved identically by build_loader_async."""
    assert build_loader_async("tests.loader.load_database") == load_database


def test_loader_async_deeply_nested_import_path() -> None:
    """build_loader_async splits all path segments before the final loader name."""
    sentinel = object()
    fake_module = ModuleType("fake_module")
    fake_module.my_loader = sentinel  # type: ignore[attr-defined]
    with patch("pytest_postgresql.loader.importlib.import_module", return_value=fake_module) as import_mock:
        result = build_loader_async("a.b.c.d:my_loader")
    import_mock.assert_called_once_with("a.b.c.d")
    assert result is sentinel


def test_loader_sql() -> None:
    """Test returning partial running sql for the sql file path."""
    sql_path = Path("test_sql/eidastats.sql")
    loader_func = build_loader(sql_path)
    assert loader_func.args == (sql_path,)  # type: ignore
    assert loader_func.func == sql  # type: ignore


@pytest.mark.asyncio
async def test_loader_sql_async() -> None:
    """Async test returning partial running sql_async for the sql file path."""
    sql_path = Path("test_sql/eidastats.sql")
    loader_func = build_loader_async(sql_path)
    assert loader_func.args == (sql_path,)  # type: ignore
    assert loader_func.func == sql_async  # type: ignore


@pytest.mark.asyncio
async def test_sql_async_raises_without_aiofiles() -> None:
    """sql_async raises ImportError with a helpful message when aiofiles is not installed."""
    with patch("pytest_postgresql.loader.aiofiles", None):
        with pytest.raises(ImportError, match="aiofiles"):
            await sql_async(Path("dummy.sql"), host="h", port=5432, user="u", dbname="d")
