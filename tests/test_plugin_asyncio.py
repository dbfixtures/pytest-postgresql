"""Tests for Windows asyncio loop configuration in the plugin."""

import asyncio
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pytest import Pytester

import pytest_postgresql
import pytest_postgresql.plugin as plugin_module
from pytest_postgresql._asyncio_compat import item_uses_postgresql_async_fixture
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.factories import postgresql_proc
from pytest_postgresql.factories.client import postgresql_async
from pytest_postgresql.plugin import (
    _resolve_windows_loop_factories,
    _windows_selector_event_loop,
    _windows_selector_event_loop_policy_cls,
    pytest_configure,
)


@pytest.fixture
def pointed_pytester(pytester: Pytester) -> Pytester:
    """Pre-configured pytester fixture."""
    pytest_postgresql_path = Path(pytest_postgresql.__file__)
    root_path = pytest_postgresql_path.parent.parent
    pytester.syspathinsert(root_path)
    return pytester


postgresql_proc_to_override = postgresql_proc()


def _postgresql_available() -> bool:
    """Return True when a PostgreSQL installation is likely available for integration tests."""
    postgresql_exec = os.environ.get("POSTGRESQL_EXEC")
    if postgresql_exec and os.path.exists(postgresql_exec):
        return True
    if shutil.which("pg_config") is not None:
        return True
    return shutil.which("pg_ctl") is not None


def _make_item_with_fixtures(*fixture_names: str, postgresql_async_names: set[str] | None = None) -> MagicMock:
    """Build a pytest item mock wired for postgresql async fixture detection."""
    async_names = postgresql_async_names or {
        name for name in fixture_names if name == "postgresql_async" or name.endswith("_async")
    }
    fixturedefs: dict[str, tuple[SimpleNamespace, ...]] = {}
    for name in fixture_names:
        if name in async_names:
            fixture_func = postgresql_async("postgresql_proc")
            fixturedefs[name] = (SimpleNamespace(func=fixture_func),)
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


@pytest.mark.skipif(sys.platform != "win32", reason="WindowsSelectorEventLoopPolicy only exists on Windows")
@pytest.mark.skipif(sys.version_info >= (3, 14), reason="Legacy policy only applies before Python 3.14")
def test_pytest_configure_sets_legacy_policy_on_old_pytest_asyncio() -> None:
    """pytest_configure sets WindowsSelectorEventLoopPolicy when loop factories are unavailable."""
    policy_cls = _windows_selector_event_loop_policy_cls()
    if policy_cls is None:
        pytest.skip("WindowsSelectorEventLoopPolicy is unavailable on this platform")

    config = MagicMock()
    config.pluginmanager.has_plugin.return_value = True
    old_pytest_asyncio = pytest.importorskip("pytest_asyncio")

    with (
        patch("pytest_postgresql.plugin.platform.system", return_value="Windows"),
        patch("pytest_postgresql.plugin.pytest_asyncio", old_pytest_asyncio),
        patch("pytest_postgresql.plugin.supports_loop_factories", return_value=False),
        patch.object(old_pytest_asyncio, "__version__", "1.3.0"),
        patch.object(asyncio, "set_event_loop_policy") as set_policy,
    ):
        pytest_configure(config)

    set_policy.assert_called_once_with(policy_cls())


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
    item = _make_item_with_fixtures("postgresql_async")
    factories = _resolve_windows_loop_factories(item, None)

    assert factories is not None
    assert set(factories) == {"selector"}
    loop = factories["selector"]()
    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_pytest_asyncio_loop_factories_uses_selector_for_unrelated_async_tests() -> None:
    """All asyncio tests on Windows need SelectorEventLoop for psycopg compatibility."""
    item = _make_item_with_fixtures("event_loop")
    assert item_uses_postgresql_async_fixture(item) is False

    factories = _resolve_windows_loop_factories(item, None)
    assert factories is not None
    assert set(factories) == {"selector"}
    loop = factories["selector"]()
    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_pytest_asyncio_loop_factories_uses_selector_by_default_on_windows() -> None:
    """Windows defaults to selector loops because ProactorEventLoop breaks psycopg."""
    item = _make_item_with_fixtures("postgresql")
    assert item_uses_postgresql_async_fixture(item) is False
    factories = _resolve_windows_loop_factories(item, None)
    assert set(factories) == {"selector"}


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_pytest_asyncio_loop_factories_preserves_prior_user_factories() -> None:
    """Unrelated asyncio tests keep loop factories from earlier hook implementations."""
    item = _make_item_with_fixtures("event_loop")
    prior = {"custom": _windows_selector_event_loop}
    factories = _resolve_windows_loop_factories(item, prior)
    assert factories is prior


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_item_uses_postgresql_async_fixture_detects_custom_factory() -> None:
    """Custom postgresql_async fixtures created via the factory are detected."""
    item = _make_item_with_fixtures("postgresql2_async")
    assert item_uses_postgresql_async_fixture(item) is True


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific loop factory hook")
def test_item_uses_postgresql_async_fixture_detects_non_suffix_name() -> None:
    """Fixture names without an _async suffix are detected via the factory marker."""
    item = _make_item_with_fixtures("async_postgresql_template", postgresql_async_names={"async_postgresql_template"})
    assert item_uses_postgresql_async_fixture(item) is True


@pytest.mark.skipif(not _postgresql_available(), reason="PostgreSQL not available")
@pytest.mark.skipif(sys.platform != "win32", reason="Windows postgresql_async E2E")
def test_postgresql_async_windows_subprocess_smoke(
    postgresql_proc_to_override: PostgreSQLExecutor,
    pointed_pytester: Pytester,
) -> None:
    """postgresql_async works in a subprocess on Windows without manual loop configuration."""
    pointed_pytester.copy_example("test_postgresql_async_windows_smoke.py")
    ret = pointed_pytester.runpytest(
        f"--postgresql-port={postgresql_proc_to_override.port}",
        "--postgresql-drop-test-database",
        "test_postgresql_async_windows_smoke.py",
    )
    ret.assert_outcomes(passed=1)
