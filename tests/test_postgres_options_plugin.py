"""Test behavior of postgres_options passed in different ways."""

from pathlib import Path

import pytest
from pytest import Pytester

import pytest_postgresql
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.factories import postgresql_proc
from pytest_postgresql.factories.noprocess import xdistify_dbname
from pytest_postgresql.janitor import DatabaseJanitor
from tests.loader import load_database


@pytest.fixture
def pointed_pytester(pytester: Pytester) -> Pytester:
    """Pre-configured pytester fixture."""
    pytest_postgresql_path = Path(pytest_postgresql.__file__)
    root_path = pytest_postgresql_path.parent.parent
    pytester.syspathinsert(root_path)
    pytester.makeconftest("from pytest_postgresql.plugin import *\n")
    return pytester


def test_postgres_options_config_in_cli(pointed_pytester: Pytester) -> None:
    """Check that command line arguments are honored."""
    pointed_pytester.copy_example("test_postgres_options.py")
    ret = pointed_pytester.runpytest(
        "--postgresql-postgres-options", "-N 16", "test_postgres_options.py"
    )
    ret.assert_outcomes(passed=1)


def test_postgres_options_config_in_ini(pointed_pytester: Pytester) -> None:
    """Check that pytest.ini arguments are honored."""
    pointed_pytester.copy_example("test_postgres_options.py")
    pointed_pytester.makefile(".ini", pytest="[pytest]\npostgresql_postgres_options = -N 16\n")
    ret = pointed_pytester.runpytest("test_postgres_options.py")
    ret.assert_outcomes(passed=1)


def test_postgres_loader_in_cli(pointed_pytester: Pytester) -> None:
    """Check that command line arguments are honored."""
    pointed_pytester.copy_example("test_load.py")
    test_sql_path = pointed_pytester.copy_example("test.sql")
    ret = pointed_pytester.runpytest(f"--postgresql-load={test_sql_path}", "test_load.py")
    ret.assert_outcomes(passed=1)


def test_postgres_loader_in_ini(pointed_pytester: Pytester) -> None:
    """Check that pytest.ini arguments are honored for load."""
    pointed_pytester.copy_example("test_load.py")
    test_sql_path = pointed_pytester.copy_example("test.sql")
    pointed_pytester.makefile(".ini", pytest=f"[pytest]\npostgresql_load = {test_sql_path}\n")
    ret = pointed_pytester.runpytest("test_load.py")
    ret.assert_outcomes(passed=1)


postgresql_proc_to_override = postgresql_proc()


def test_postgres_drop_test_database(
    postgresql_proc_to_override: PostgreSQLExecutor,
    pointed_pytester: Pytester,
) -> None:
    """Check that the database is dropped on both process and client level if argument is passed.

    Given:
        Preexisting tables override_tmpl and override (created with two DatabaseJanitor instances)
    When:
        Run test that connects to the process from current process with flag to drop database
        specified
    Then:
        The internal pytest run will delete the database on start (and after).
        It Would fail if it did not. Checks are performed by trying to connect to both override_tmpl
        and override databases and checking resulting exceptions.
    """
    dbname = xdistify_dbname("override")
    template_dbname = dbname + "_tmpl"
    template_janitor = DatabaseJanitor(
        user=postgresql_proc_to_override.user,
        host=postgresql_proc_to_override.host,
        port=postgresql_proc_to_override.port,
        template_dbname=template_dbname,
        version=postgresql_proc_to_override.version,
        password=postgresql_proc_to_override.password,
        connection_timeout=5,
    )
    template_janitor.init()
    template_janitor.load(load_database)
    assert template_janitor.template_dbname
    janitor = DatabaseJanitor(
        user=postgresql_proc_to_override.user,
        host=postgresql_proc_to_override.host,
        port=postgresql_proc_to_override.port,
        dbname=dbname,
        template_dbname=template_janitor.template_dbname,
        version=postgresql_proc_to_override.version,
        password=postgresql_proc_to_override.password,
        connection_timeout=5,
    )
    janitor.init()
    assert janitor.dbname
    with janitor.cursor(janitor.dbname) as cur:
        cur.execute("SELECT * FROM stories")
        res = cur.fetchall()
        assert len(res) == 4
    # Actual test happens now
    pointed_pytester.copy_example("test_drop_test_database.py")
    test_sql_path = pointed_pytester.copy_example("test.sql")
    ret = pointed_pytester.runpytest(
        f"--postgresql-load={test_sql_path}",
        f"--postgresql-port={postgresql_proc_to_override.port}",
        "--postgresql-dbname=override",
        "--postgresql-drop-test-database",
        "test_drop_test_database.py",
    )
    ret.assert_outcomes(passed=1)

    with pytest.raises(TimeoutError) as excinfo:
        with janitor.cursor(janitor.dbname):
            pass
    assert hasattr(excinfo.value, "__cause__")
    assert f'FATAL:  database "{janitor.dbname}" does not exist' in str(excinfo.value.__cause__)
    with pytest.raises(TimeoutError) as excinfo:
        with template_janitor.cursor(template_janitor.template_dbname):
            pass
    assert hasattr(excinfo.value, "__cause__")
    assert f'FATAL:  database "{template_janitor.template_dbname}" does not exist' in str(
        excinfo.value.__cause__
    )
