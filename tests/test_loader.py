"""Tests for the `build_loader` function."""

from pathlib import Path
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
    real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__  # type: ignore[union-attr]

    def _block_aiofiles(name: str, *args: object, **kwargs: object) -> object:
        if name == "aiofiles":
            raise ImportError("No module named 'aiofiles'")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    with patch("builtins.__import__", side_effect=_block_aiofiles):
        with pytest.raises(ImportError, match="aiofiles"):
            await sql_async(Path("dummy.sql"), host="h", port=5432, user="u", dbname="d")
