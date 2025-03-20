"""Plugin's configuration."""

from pathlib import Path
from typing import Any, List, Optional, TypedDict, Union

from _pytest._py.path import LocalPath
from pytest import FixtureRequest


class PostgresqlConfigDict(TypedDict):
    """Typed Config dictionary."""

    exec: str
    host: str
    port: Optional[str]
    port_search_count: int
    user: str
    password: str
    options: str
    startparams: str
    unixsocketdir: str
    dbname: str
    load: List[Union[Path, str]]
    postgres_options: str
    drop_test_database: bool


def get_config(request: FixtureRequest) -> PostgresqlConfigDict:
    """Return a dictionary with config options."""

    def get_postgresql_option(option: str, default: Any = None) -> Any:
        name = "postgresql_" + option
        return request.config.getoption(name) or request.config.getini(name) or default

    load_paths = detect_paths(get_postgresql_option("load"))

    return PostgresqlConfigDict(
        exec=get_postgresql_option("exec"),
        host=get_postgresql_option("host"),
        port=get_postgresql_option("port"),
        # Parse as int, because if it's defined in an INI file then it'll always be a string
        port_search_count=int(get_postgresql_option("port_search_count", default=5)),
        user=get_postgresql_option("user"),
        password=get_postgresql_option("password"),
        options=get_postgresql_option("options"),
        startparams=get_postgresql_option("startparams"),
        unixsocketdir=get_postgresql_option("unixsocketdir"),
        dbname=get_postgresql_option("dbname"),
        load=load_paths,
        postgres_options=get_postgresql_option("postgres_options"),
        drop_test_database=request.config.getoption("postgresql_drop_test_database"),
    )


def detect_paths(load_paths: List[Union[LocalPath, str]]) -> List[Union[Path, str]]:
    """Convert path to sql files to Path instances."""
    converted_load_paths: List[Union[Path, str]] = []
    for path in load_paths:
        if isinstance(path, LocalPath):
            path = str(path)
        if path.endswith(".sql"):
            converted_load_paths.append(Path(path))
        else:
            converted_load_paths.append(path)
    return converted_load_paths
