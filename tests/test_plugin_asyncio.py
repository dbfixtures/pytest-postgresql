"""Tests for Windows asyncio loop configuration in the plugin."""

import asyncio
import sys
from unittest.mock import MagicMock, patch

import pytest

import pytest_postgresql.plugin as plugin_module
from pytest_postgresql.plugin import _windows_selector_event_loop, pytest_configure


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
def test_pytest_asyncio_loop_factories_on_windows() -> None:
    """Windows configures a single selector loop factory for pytest-asyncio."""
    config = MagicMock()
    item = MagicMock()
    factories = plugin_module.pytest_asyncio_loop_factories(config, item)

    assert factories is not None
    assert set(factories) == {"selector"}
    loop = factories["selector"]()
    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()
