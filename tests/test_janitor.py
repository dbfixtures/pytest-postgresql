"""Database Janitor tests."""

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from packaging.version import parse

from pytest_postgresql.janitor import DatabaseJanitor

VERSION = parse("10")


@pytest.mark.parametrize("version", (VERSION, 10, "10"))
def test_version_cast(version: Any) -> None:
    """Test that version is cast to Version object."""
    janitor = DatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=version)
    assert janitor.version == VERSION


@patch("pytest_postgresql.janitor.psycopg.connect")
def test_cursor_selects_postgres_database(connect_mock: MagicMock) -> None:
    """Test that the cursor requests the postgres database."""
    janitor = DatabaseJanitor(user="user", host="host", port="1234", dbname="database_name", version=10)
    with janitor.cursor():
        connect_mock.assert_called_once_with(dbname="postgres", user="user", password=None, host="host", port="1234")


@patch("pytest_postgresql.janitor.psycopg.connect")
def test_cursor_connects_with_password(connect_mock: MagicMock) -> None:
    """Test that the cursor requests the postgres database."""
    janitor = DatabaseJanitor(
        user="user",
        host="host",
        port="1234",
        dbname="database_name",
        version=10,
        password="some_password",
    )
    with janitor.cursor():
        connect_mock.assert_called_once_with(
            dbname="postgres", user="user", password="some_password", host="host", port="1234"
        )


@pytest.mark.skipif(sys.version_info < (3, 8), reason="Unittest call_args.kwargs was introduced since python 3.8")
@pytest.mark.parametrize("load_database", ("tests.loader.load_database", "tests.loader:load_database"))
@patch("pytest_postgresql.janitor.psycopg.connect")
def test_janitor_populate(connect_mock: MagicMock, load_database: str) -> None:
    """Test that the cursor requests the postgres database.

    load_database tries to connect to database, which triggers mocks.
    """
    call_kwargs = {
        "host": "host",
        "port": "1234",
        "user": "user",
        "dbname": "database_name",
        "password": "some_password",
    }
    janitor = DatabaseJanitor(version=10, **call_kwargs)  # type: ignore[arg-type]
    janitor.load(load_database)
    assert connect_mock.called
    assert connect_mock.call_args.kwargs == call_kwargs
