"""Config tests."""

from pathlib import Path
from typing import Any

import pytest
from _pytest._py.path import LocalPath

from pytest_postgresql.config import PostgreSQLConfig, detect_paths


@pytest.mark.parametrize("positional", [False, True], ids=["keywords", "positional"])
def test_config_preserves_legacy_constructor_arguments(*, positional: bool) -> None:
    """Existing constructor calls keep their argument meanings and default strategy."""
    arguments: dict[str, Any] = {
        "exec": "pg_ctl",
        "host": "localhost",
        "port": None,
        "port_search_count": 5,
        "user": "postgres",
        "password": None,
        "options": "",
        "startparams": "-w",
        "unixsocketdir": "/tmp",
        "dbname": "tests",
        "maintenance_dbname": "postgres",
        "load": [],
        "load_autocommit": False,
        "postgres_options": "",
        "drop_test_database": True,
    }
    config = PostgreSQLConfig(*arguments.values()) if positional else PostgreSQLConfig(**arguments)
    assert config.drop_test_database is True
    assert config.create_strategy is None


@pytest.mark.parametrize(
    ("path", "want"),
    [
        ("test.sql", Path("test.sql")),
        ("load.function", "load.function"),
        (LocalPath("test.sql"), Path("test.sql").absolute()),  # type: ignore[no-untyped-call]
    ],
)
def test_detect_paths(path: str | LocalPath, want: Path | str) -> None:
    """Check the correctness of detect_paths function."""
    assert detect_paths([path]) == [want]
