"""Tests for Windows asyncio loop configuration in the plugin."""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import pytest_postgresql.plugin as plugin_module
from pytest_postgresql._asyncio_compat import item_uses_postgresql_async_fixture
from pytest_postgresql.factories.client import postgresql_async
from pytest_postgresql.plugin import _windows_selector_event_loop, pytest_configure


def _make_item_with_fixtures(*fixture_names: str) -> MagicMock:
    """Build a pytest item mock wired for postgresql async fixture detection."""
    fixturedefs: dict[str, tuple[SimpleNamespace, ...]] = {}
    for name in fixture_names:
        if name == "postgresql_async" or name.endswith("_async"):
            fixture_func = postgresql_async("postgresql_proc")
            raw_func = getattr(fixture_func, "__wrapped__", fixture_func)
            fixturedefs[name] = (SimpleNamespace(func=raw_func),)
        else:
            fixturedefs[name] = (SimpleNamespace(func=lambda: None),)

    item = MagicMock()
    item.fixturenames = list(fixture_names)
    item._fixtureinfo = SimpleNamespace(name2fixturedefs=fixturedefs)
    return item


@pytest.mark.skipif(sys.version_info < (3, 14), reason="Deprecation applies from Python 3.14")
def test_pytest_configure_skips_deprecated_policy_on_python_314() -> None:
    """pytest_configure must not call deprecated asyncio policy APIs on Python 3.14+."""
    config = MagicMock()
    config.pluginmanager.has_plugin.return_value = True

    with (
        patch("pytest_postgresql.plugin.platform.system", return_value="Windows"),
        patch.object(asyncio, "set_event_loop_policy") as set_policy,
    ):
        pytest_configure(config)

    set_policy.assert_not_called()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory")
def test_windows_selector_loop_factory() -> None:
    """Windows selector loop factory returns a SelectorEventLoop instance."""
    loop = _windows_selector_event_loop()
    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Windows registers loop factory hook at import")
def test_loop_factory_hook_not_registered_on_non_windows() -> None:
    """Non-Windows platforms must not register pytest_asyncio_loop_factories."""
    assert not hasattr(plugin_module, "pytest_asyncio_loop_factories")


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_pytest_asyncio_loop_factories_on_windows_for_postgresql_async() -> None:
    """Windows configures a selector loop factory for postgresql async tests."""
    config = MagicMock()
    item = _make_item_with_fixtures("postgresql_async")
    factories = plugin_module.pytest_asyncio_loop_factories(config, item)

    assert factories is not None
    assert set(factories) == {"selector"}
    loop = factories["selector"]()
    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_pytest_asyncio_loop_factories_skips_unrelated_async_tests() -> None:
    """Unrelated asyncio tests must receive the default event loop factory."""
    config = MagicMock()
    item = _make_item_with_fixtures("event_loop")
    assert item_uses_postgresql_async_fixture(item) is False

    factories = plugin_module.pytest_asyncio_loop_factories(config, item)
    assert factories is not None
    assert set(factories) == {"default"}
    loop = factories["default"]()
    try:
        assert isinstance(loop, asyncio.AbstractEventLoop)
    finally:
        loop.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_item_uses_postgresql_async_fixture_detects_custom_factory() -> None:
    """Custom postgresql_async fixtures created via the factory are detected."""
    item = _make_item_with_fixtures("postgresql2_async")
    assert item_uses_postgresql_async_fixture(item) is True
