"""Tests for the `build_loader` function."""

from pathlib import Path

import pytest

from pytest_postgresql.loader import build_loader, build_loader_async, sql, sql_async
from tests.loader import load_database


def test_loader_callables() -> None:
    """Test handling callables in build_loader."""
    assert load_database == build_loader(load_database)
    assert load_database == build_loader("tests.loader:load_database")


@pytest.mark.asyncio
async def test_loader_callables_async() -> None:
    """Async test handling callables in build_loader_async."""
    assert load_database == build_loader_async(load_database)
    assert load_database == build_loader_async("tests.loader:load_database")

    async def afun(*args, **kwargs):
        return 0

    assert afun == build_loader_async(afun)


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
