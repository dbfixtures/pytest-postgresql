"""Plugin's configuration."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _pytest._py.path import LocalPath
from pytest import FixtureRequest


@dataclass
class PostgreSQLConfig:
    """PostgreSQL Config."""

    exec: str
    host: str
    port: str | None
    port_search_count: int
    user: str
    password: str
    options: str
    startparams: str
    unixsocketdir: str
    dbname: str
    load: list[Path | str]
    postgres_options: str
    drop_test_database: bool


def get_config(request: FixtureRequest) -> PostgreSQLConfig:
    """
    Create a PostgreSQLConfig populated from pytest configuration options.
    
    Reads pytest options and INI values prefixed with "postgresql_" to populate a PostgreSQLConfig dataclass. The "load" option is normalized to Paths or strings and "port_search_count" is converted to an int.
    
    Parameters:
        request (FixtureRequest): pytest fixture request used to read config options and INI values.
    
    Returns:
        PostgreSQLConfig: Configuration populated from the pytest settings.
    """

    def get_postgresql_option(option: str) -> Any:
        """
        Retrieve a PostgreSQL-related pytest configuration value.
        
        Parameters:
            option (str): The suffix of the configuration name (without the "postgresql_" prefix).
        
        Returns:
            The value of the pytest configuration option named "postgresql_<option>", or `None` if not set.
        """
        name = "postgresql_" + option
        return request.config.getoption(name) or request.config.getini(name)

    load_paths: list[Path | str] = detect_paths(get_postgresql_option("load"))

    cfg = PostgreSQLConfig(
        exec=get_postgresql_option("exec"),
        host=get_postgresql_option("host"),
        port=get_postgresql_option("port"),
        # Parse as int, because if it's defined in an INI file then it'll always be a string
        port_search_count=int(get_postgresql_option("port_search_count")),
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
    return cfg


def detect_paths(load_paths: list[LocalPath | str]) -> list[Path | str]:
    """
    Normalize a sequence of load paths so SQL file paths are Path objects and other entries are preserved.
    
    Parameters:
        load_paths (list[LocalPath | str]): Iterable of paths to normalize; entries may be pytest LocalPath objects or strings.
    
    Returns:
        list[Path | str]: A new list where entries that refer to files ending with ".sql" are returned as pathlib.Path objects and all other entries are returned unchanged (strings).
    """
    converted_load_paths: list[Path | str] = []
    for path in load_paths:
        if isinstance(path, LocalPath):
            path = str(path)
        if path.endswith(".sql"):
            converted_load_paths.append(Path(path))
        else:
            converted_load_paths.append(path)
    return converted_load_paths