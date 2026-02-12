"""Tests main conftest file."""

import os
from pathlib import Path

from pytest_postgresql import factories

pytest_plugins = ["pytester"]
POSTGRESQL_VERSION = os.environ.get("POSTGRES", "13")


TEST_SQL_DIR = os.path.dirname(os.path.abspath(__file__)) + "/test_sql/"
TEST_SQL_FILE = Path(TEST_SQL_DIR + "test.sql")
TEST_SQL_FILE2 = Path(TEST_SQL_DIR + "test2.sql")

postgresql_proc2 = factories.postgresql_proc(port=None, load=[TEST_SQL_FILE, TEST_SQL_FILE2])
postgresql2 = factories.postgresql("postgresql_proc2", dbname="test-db")
postgresql_load_1 = factories.postgresql("postgresql_proc2")
