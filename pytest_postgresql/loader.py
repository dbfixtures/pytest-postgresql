"""Loader helper functions."""

import re
from functools import partial
from pathlib import Path
from typing import Any, Callable, Union

import psycopg


def build_loader(load: Union[Callable, str, Path]) -> Callable:
    """Build a loader callable."""
    if isinstance(load, Path):
        return partial(sql, load)
    elif isinstance(load, str):
        loader_parts = re.split("[.:]", load, maxsplit=2)
        import_path = ".".join(loader_parts[:-1])
        loader_name = loader_parts[-1]
        _temp_import = __import__(import_path, globals(), locals(), fromlist=[loader_name])
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
