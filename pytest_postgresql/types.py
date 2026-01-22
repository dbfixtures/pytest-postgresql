"""Pytest Postgresql Types."""

from typing import Literal

FixtureScopeT = Literal["session", "package", "module", "class", "function"]
