"""Noproc client test used by the maintenance dbname tests."""

from psycopg import Connection

from pytest_postgresql import factories

postgresql = factories.postgresql("postgresql_noproc")


def test_maintenance_dbname(postgresql: Connection) -> None:
    """Check the fixtures work with a maintenance database other than postgres."""
    cur = postgresql.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)
    cur.close()
