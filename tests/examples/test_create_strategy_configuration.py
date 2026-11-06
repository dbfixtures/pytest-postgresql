"""Configuration assertions run through pytester to exercise CLI and ini parsing."""

import json
from pathlib import Path

import pytest

from pytest_postgresql.config import get_config


def test_strategy(request: pytest.FixtureRequest) -> None:
    """Resolve the strategy through the real plugin configuration."""
    expected = json.loads(Path(__file__).with_name("expected_strategy.json").read_text(encoding="utf-8"))
    assert get_config(request).create_strategy == expected
