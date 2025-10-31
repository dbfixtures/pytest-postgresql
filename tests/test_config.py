"""Config tests."""

from pathlib import Path

import pytest
from _pytest._py.path import LocalPath

from pytest_postgresql.config import detect_paths


@pytest.mark.parametrize(
    "path, want",
    (
        ("test.sql", Path("test.sql")),
        ("load.function", "load.function"),
        (LocalPath("test.sql"), Path("test.sql").absolute()),  # type: ignore[no-untyped-call]
    ),
)
def test_detect_paths(path: str | LocalPath, want: Path | str) -> None:
    """Check the correctness of detect_paths function."""
    assert detect_paths([path]) == [want]
