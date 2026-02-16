"""Tests main conftest file."""

import os
import platform
from pathlib import Path

import pytest

from pytest_postgresql import factories

pytest_plugins = ["pytester"]
POSTGRESQL_VERSION = os.environ.get("POSTGRES", "13")


@pytest.fixture(scope="session", autouse=True)
def setup_windows_locale() -> None:
    """Set Windows-compatible locale environment variables.

    Windows doesn't support Unix-style locales like C.UTF-8 or en_US.UTF-8.
    PostgreSQL's initdb requires valid locale settings, so we set the 'C'
    locale which is supported on Windows.

    This fixture runs automatically for all test sessions on Windows.
    """
    if platform.system() == "Windows":
        # Set Windows-compatible locale (C locale is supported on Windows)
        os.environ["LC_ALL"] = "C"
        os.environ["LC_CTYPE"] = "C"
        os.environ["LANG"] = "C"


TEST_SQL_DIR = os.path.dirname(os.path.abspath(__file__)) + "/test_sql/"
TEST_SQL_FILE = Path(TEST_SQL_DIR + "test.sql")
TEST_SQL_FILE2 = Path(TEST_SQL_DIR + "test2.sql")

postgresql_proc2 = factories.postgresql_proc(port=None, load=[TEST_SQL_FILE, TEST_SQL_FILE2])
postgresql2 = factories.postgresql("postgresql_proc2", dbname="test-db")
postgresql_load_1 = factories.postgresql("postgresql_proc2")
