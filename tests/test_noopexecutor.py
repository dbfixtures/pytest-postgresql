"""Test for NoopExecutor."""

import psycopg
import pytest

from pytest_postgresql.executors import NoopExecutor, PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor
from pytest_postgresql.retry import retry


def test_noproc_version(postgresql_proc: PostgreSQLExecutor) -> None:
    """Test the way postgresql version is being read.

    Version behaves differently for postgresql >= 10 and differently for older ones
    """
    postgresql_noproc = NoopExecutor(
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.user,
        postgresql_proc.options,
        postgresql_proc.dbname,
    )
    noproc_version = retry(
        lambda: postgresql_noproc.version,
        possible_exception=psycopg.OperationalError,
    )
    assert postgresql_proc.version == noproc_version


def test_noproc_cached_version(postgresql_proc: PostgreSQLExecutor) -> None:
    """Test that the version is being cached."""
    postgresql_noproc = NoopExecutor(
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.user,
        postgresql_proc.options,
        postgresql_proc.dbname,
    )
    ver = retry(
        lambda: postgresql_noproc.version,
        possible_exception=psycopg.OperationalError,
    )
    with postgresql_proc.stopped():
        assert ver == postgresql_noproc.version


def test_noproc_version_uses_maintenance_dbname(postgresql_proc: PostgreSQLExecutor) -> None:
    """Test that the version is read through the configured maintenance database.

    The executor's own dbname does not exist, so a successful read proves the
    connection went to the maintenance database instead.
    """
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="maintenance_for_version",
        password=postgresql_proc.password,
    ):
        postgresql_noproc = NoopExecutor(
            postgresql_proc.host,
            postgresql_proc.port,
            postgresql_proc.user,
            postgresql_proc.options,
            "database_that_does_not_exist",
            maintenance_dbname="maintenance_for_version",
        )
        noproc_version = retry(
            lambda: postgresql_noproc.version,
            possible_exception=psycopg.OperationalError,
        )
    assert postgresql_proc.version == noproc_version


def test_noproc_version_fails_on_missing_maintenance_dbname(postgresql_proc: PostgreSQLExecutor) -> None:
    """Test that the maintenance database is the one actually connected to."""
    postgresql_noproc = NoopExecutor(
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.user,
        postgresql_proc.options,
        postgresql_proc.dbname,
        maintenance_dbname="maintenance_that_does_not_exist",
    )
    with pytest.raises(psycopg.OperationalError, match="maintenance_that_does_not_exist"):
        postgresql_noproc.version
