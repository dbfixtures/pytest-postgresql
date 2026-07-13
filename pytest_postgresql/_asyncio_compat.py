"""Shared helpers for optional pytest-asyncio integration."""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from packaging.version import parse

if TYPE_CHECKING:
    import pytest

_MIN_PYTEST_ASYNCIO_VERSION = parse("1.4.0")


def supports_loop_factories(pytest_asyncio: ModuleType | None) -> bool:
    """Return True when pytest-asyncio is installed at a version that supports loop factories."""
    if pytest_asyncio is None:
        return False
    return parse(pytest_asyncio.__version__) >= _MIN_PYTEST_ASYNCIO_VERSION


def is_async_extra_available(pytest_asyncio: ModuleType | None) -> bool:
    """Return True when pytest-asyncio meets the minimum version for async fixtures."""
    return supports_loop_factories(pytest_asyncio)


def item_uses_postgresql_async_fixture(item: pytest.Item) -> bool:
    """Return True when the test item requests a postgresql async client fixture."""
    fixture_info = getattr(item, "_fixtureinfo", None)
    if fixture_info is None:
        return False

    name2fixturedefs = getattr(fixture_info, "name2fixturedefs", {})
    for name in item.fixturenames:
        if name != "postgresql_async" and not name.endswith("_async"):
            continue
        for fixturedef in name2fixturedefs.get(name, ()):
            func = getattr(fixturedef, "func", None)
            if func is None:
                continue
            wrapped = getattr(func, "__wrapped__", func)
            module = getattr(wrapped, "__module__", "")
            if module.startswith("pytest_postgresql"):
                return True
    return False
