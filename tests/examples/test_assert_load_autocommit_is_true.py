"""Asserts that load_autocommit is True.

That shows that it is not the default (False), and that we parsed it as a bool.
"""

import pytest

from pytest_postgresql.config import get_config


def test_assert_load_autocommit_is_true(request: pytest.FixtureRequest) -> None:
    """Asserts that load_autocommit is True."""
    config = get_config(request)
    assert config.load_autocommit is True
