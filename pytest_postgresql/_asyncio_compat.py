"""Shared helpers for optional pytest-asyncio integration."""

from __future__ import annotations

from collections.abc import Callable
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeGuard

from packaging.version import parse

if TYPE_CHECKING:
    import pytest

_MIN_PYTEST_ASYNCIO_VERSION = parse("1.4.0")
POSTGRESQL_ASYNC_FIXTURE_ATTR = "_pytest_postgresql_async_fixture"


def supports_loop_factories(pytest_asyncio: ModuleType | None) -> TypeGuard[ModuleType]:
    """Return True when pytest-asyncio is installed at a version that supports loop factories."""
    if pytest_asyncio is None:
        return False
    return parse(pytest_asyncio.__version__) >= _MIN_PYTEST_ASYNCIO_VERSION


def mark_postgresql_async_fixture(func: Callable[..., Any]) -> Callable[..., Any]:
    """Tag a fixture function as a postgresql async client fixture."""
    setattr(func, POSTGRESQL_ASYNC_FIXTURE_ATTR, True)
    return func


def is_postgresql_async_fixture_func(func: object) -> bool:
    """Return True when func was created by postgresql_async()."""
    wrapped = getattr(func, "__wrapped__", func)
    if getattr(wrapped, POSTGRESQL_ASYNC_FIXTURE_ATTR, False):
        return True
    module = getattr(wrapped, "__module__", "")
    return module.startswith("pytest_postgresql") and getattr(wrapped, "__name__", "") == "postgresql_async_factory"


def item_uses_postgresql_async_fixture(item: pytest.Item) -> bool:
    """Return True when the test item requests a postgresql async client fixture."""
    fixture_info = getattr(item, "_fixtureinfo", None)
    if fixture_info is None:
        return False

    name2fixturedefs = getattr(fixture_info, "name2fixturedefs", {})
    fixturenames: tuple[str, ...] = getattr(item, "fixturenames", ())
    for name in fixturenames:
        for fixturedef in name2fixturedefs.get(name, ()):
            func = getattr(fixturedef, "func", None)
            if func is not None and is_postgresql_async_fixture_func(func):
                return True
    return False
