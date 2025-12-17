"""Asserts that port_search_count is 10.

That shows that it is not the default (5), and that we parsed it as an integer.
"""

from pytest import FixtureRequest

from pytest_postgresql.config import get_config


def test_assert_port_search_count_is_ten(request: FixtureRequest) -> None:
    """Asserts that port_search_count is 10."""
    config = get_config(request)
    assert config.port_search_count == 10
