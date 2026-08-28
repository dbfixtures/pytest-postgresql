"""Auxiliary tests."""

import pytest

from pytest_postgresql.executors import PostgreSQLExecutor


@pytest.mark.parametrize(
    ("ctl_input", "version"),
    [
        ("pg_ctl (PostgreSQL) 10.18", "10.18"),
        ("pg_ctl (PostgreSQL) 11.13", "11.13"),
        ("pg_ctl (PostgreSQL) 12.8", "12.8"),
        ("pg_ctl (PostgreSQL) 13.4", "13.4"),
        ("pg_ctl (PostgreSQL) 14.0", "14.0"),
        ("pg_ctl (PostgreSQL) 16devel", "16"),
    ],
)
def test_versions(ctl_input: str, version: str) -> None:
    """Check correctness of the version regexp."""
    match = PostgreSQLExecutor.VERSION_RE.search(ctl_input)
    assert match is not None
    assert match.groupdict()["version"] == version
