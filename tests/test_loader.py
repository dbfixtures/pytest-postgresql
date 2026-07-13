"""Tests for the `build_loader` function."""

from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

from pytest_postgresql.loader import build_loader, sql, sql_async
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


def test_loader_callables_with_async_sql_loader() -> None:
    """build_loader with sql_loader=sql_async resolves callables the same as the default."""
    assert load_database == build_loader(load_database, sql_loader=sql_async)
    assert load_database == build_loader("tests.loader:load_database", sql_loader=sql_async)

    async def afun(*_args: object, **_kwargs: object) -> int:
        return 0

    assert afun == build_loader(afun, sql_loader=sql_async)


def test_loader_sql() -> None:
    """Test returning partial running sql for the sql file path."""
    sql_path = Path("test_sql/eidastats.sql")
    loader_func = build_loader(sql_path)
    assert loader_func.args == (sql_path,)  # type: ignore
    assert loader_func.func == sql  # type: ignore


def test_loader_sql_async() -> None:
    """build_loader with sql_loader=sql_async returns partial for sql_async."""
    sql_path = Path("test_sql/eidastats.sql")
    loader_func = build_loader(sql_path, sql_loader=sql_async)
    assert loader_func.args == (sql_path,)  # type: ignore
    assert loader_func.func == sql_async  # type: ignore


@pytest.mark.asyncio
async def test_sql_async_raises_without_aiofiles() -> None:
    """sql_async raises ImportError with a helpful message when aiofiles is not installed."""
    with patch("pytest_postgresql.loader.aiofiles", None):
        with pytest.raises(ImportError, match="aiofiles"):
            await sql_async(Path("dummy.sql"), host="h", port=5432, user="u", dbname="d")
