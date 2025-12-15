"""Loader helper functions."""

import importlib
import re
from functools import partial
from pathlib import Path
from typing import Any, Callable

import psycopg


def build_loader(load: Callable | str | Path) -> Callable:
    """Build a loader callable."""
    if isinstance(load, Path):
        return partial(sql, load)
    elif isinstance(load, str):
        loader_parts = re.split("[.:]", load, maxsplit=2)
        import_path = ".".join(loader_parts[:-1])
        loader_name = loader_parts[-1]
        _temp_import = importlib.import_module(import_path, loader_name)
        _loader: Callable = getattr(_temp_import, loader_name)
        return _loader
    else:
        return load


def sql(sql_filename: Path, **kwargs: Any) -> None:
    """Database loader for sql files."""
    with psycopg.connect(**kwargs) as db_connection:
        with open(sql_filename, "r") as _fd:
            with db_connection.cursor() as cur:
                cur.execute(_fd.read())
        db_connection.commit()


def build_loader_async(load: Callable | str | Path) -> Callable:
    """Build a loader callable."""
    if isinstance(load, Path):
        return partial(sql_async, load)
    elif isinstance(load, str):
        loader_parts = re.split("[.:]", load, maxsplit=2)
        import_path = ".".join(loader_parts[:-1])
        loader_name = loader_parts[-1]
        _temp_import = importlib.import_module(import_path, loader_name)
        _loader: Callable = getattr(_temp_import, loader_name)
        return _loader
    else:
        return load


async def sql_async(sql_filename: Path, **kwargs: Any) -> None:
    """Async database loader for sql files."""

    import aiofiles

    async with await psycopg.AsyncConnection.connect(**kwargs) as db_connection:
        async with db_connection.cursor() as cur:
            async with aiofiles.open(sql_filename, "r") as _fd:
                await cur.execute(await _fd.read())
        await db_connection.commit()
