# Copyright (C) 2016 by Clearcode <http://clearcode.cc>
# and associates (see AUTHORS).

# This file is part of pytest-postgresql.

# pytest-postgresql is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# pytest-postgresql is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with pytest-postgresql.  If not, see <http://www.gnu.org/licenses/>.
"""Plugin module of pytest-postgresql."""

import asyncio
import platform
import selectors
from collections.abc import Callable, Generator
from tempfile import gettempdir
from typing import Any, cast

import pytest
from _pytest.config.argparsing import Parser
from packaging.version import Version

from pytest_postgresql import factories
from pytest_postgresql._asyncio_compat import supports_loop_factories

try:
    import pytest_asyncio
except ImportError:
    pytest_asyncio = None  # type: ignore[assignment]

_help_executable = "Path to PostgreSQL executable"
_help_host = "Host at which PostgreSQL will accept connections"
_help_port = "Port at which PostgreSQL will accept connections"
_help_port_search_count = "Number of times, pytest-postgresql will search for free port"
_help_user = "PostgreSQL username"
_help_password = "PostgreSQL password"
_help_options = "PostgreSQL connection options"
_help_startparams = "Starting parameters for the PostgreSQL"
_help_unixsocketdir = "Location of the socket directory"
_help_dbname = "Default database name"
_help_load = "Dotted-style or entrypoint-style path to callable or path to SQL File"
_help_postgres_options = "Postgres executable extra parameters. Passed via the -o option to pg_ctl"
_help_drop_test_database = (
    "Drop test database in noproc and client fixture, for the cases, "
    "when database was not cleared due to errors in previous test runs. "
    "Use cautiously and not on CI."
)

# psycopg async cannot use Windows' default ProactorEventLoop (the default since
# Python 3.8).  libpq socket I/O relies on selector APIs (add_reader / fileno)
# that Proactor does not support.  See:
# https://www.psycopg.org/psycopg3/docs/advanced/async.html
#
# pytest-asyncio does not switch event loops for us, so without the hook below
# ``postgresql_async`` tests fail on Windows with:
# "Psycopg cannot use the 'ProactorEventLoop' to run in async mode".
#
# We register a SelectorEventLoop via pytest-asyncio's official
# ``pytest_asyncio_loop_factories`` hook (pytest-asyncio >= 1.4).  A legacy
# ``WindowsSelectorEventLoopPolicy`` fallback in ``pytest_configure`` remains for
# Windows + Python < 3.14 when the loop-factory hook is unavailable.


def _windows_selector_event_loop() -> asyncio.AbstractEventLoop:
    """Create a SelectorEventLoop for psycopg async on Windows."""
    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def _is_windows() -> bool:
    return platform.system() == "Windows"


def _uses_deprecated_asyncio_policy_on_windows() -> bool:
    return Version(platform.python_version()) < Version("3.14") and not supports_loop_factories(pytest_asyncio)


def _windows_selector_event_loop_policy_cls() -> type[asyncio.AbstractEventLoopPolicy] | None:
    """Return WindowsSelectorEventLoopPolicy when available (removed in Python 3.14)."""
    policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_cls is None:
        return None
    return cast(type[asyncio.AbstractEventLoopPolicy], policy_cls)


def _resolve_windows_loop_factories(
    item: pytest.Item,
    prior_result: dict[str, Callable[[], asyncio.AbstractEventLoop]] | None,
) -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    """Choose loop factories for a test item on Windows."""
    if prior_result is not None:
        return prior_result
    # psycopg async is incompatible with ProactorEventLoop on Windows.
    return {"selector": _windows_selector_event_loop}


def pytest_configure(config: pytest.Config) -> None:
    """Set legacy Windows selector policy when loop-factory hook is unavailable."""
    if not _is_windows() or not config.pluginmanager.has_plugin("asyncio"):
        return
    if not _uses_deprecated_asyncio_policy_on_windows():
        return
    policy_cls = _windows_selector_event_loop_policy_cls()
    if policy_cls is None:
        return
    asyncio.set_event_loop_policy(policy_cls())


if _is_windows():

    @pytest.hookimpl(hookwrapper=True, optionalhook=True)
    def pytest_asyncio_loop_factories(
        config: pytest.Config,
        item: pytest.Item,
    ) -> Generator[None, object, None]:
        """Register a SelectorEventLoop factory for psycopg async on Windows.

        psycopg async is incompatible with the default ProactorEventLoop; see
        https://www.psycopg.org/psycopg3/docs/advanced/async.html .  pytest-asyncio
        exposes this hook (>= 1.4) so plugins can supply a compatible loop without
        requiring users to call ``asyncio.set_event_loop_policy`` themselves.

        The selector factory is forced for tests that use a postgresql async client
        fixture.  Other asyncio tests defer to earlier hook implementations; when
        none are registered, a default factory is supplied because pytest-asyncio
        rejects an empty mapping once this hook is present (see README Windows
        notes for the resulting ``[default]`` test IDs).
        """
        outcome: Any = yield
        result = outcome.get_result()
        outcome.force_result(_resolve_windows_loop_factories(item, result))


def pytest_addoption(parser: Parser) -> None:
    """Configure options for pytest-postgresql."""
    parser.addini(name="postgresql_exec", help=_help_executable, default="/usr/lib/postgresql/14/bin/pg_ctl")

    parser.addini(name="postgresql_host", help=_help_host, default="127.0.0.1")

    parser.addini(
        name="postgresql_port",
        help=_help_port,
        default=None,
    )
    parser.addini(name="postgresql_port_search_count", help=_help_port_search_count, default=5)

    parser.addini(name="postgresql_user", help=_help_user, default="postgres")

    parser.addini(name="postgresql_password", help=_help_password, default=None)

    parser.addini(name="postgresql_options", help=_help_options, default="")

    parser.addini(name="postgresql_startparams", help=_help_startparams, default="-w")

    parser.addini(name="postgresql_unixsocketdir", help=_help_unixsocketdir, default=gettempdir())

    parser.addini(name="postgresql_dbname", help=_help_dbname, default="tests")

    parser.addini(name="postgresql_load", type="pathlist", help=_help_load)
    parser.addini(name="postgresql_postgres_options", help=_help_postgres_options, default="")

    parser.addoption(
        "--postgresql-exec",
        action="store",
        metavar="path",
        dest="postgresql_exec",
        help=_help_executable,
    )

    parser.addoption(
        "--postgresql-host",
        action="store",
        dest="postgresql_host",
        help=_help_host,
    )

    parser.addoption("--postgresql-port", action="store", dest="postgresql_port", help=_help_port)
    parser.addoption(
        "--postgresql-port-search-count",
        action="store",
        dest="postgresql_port_search_count",
        help=_help_port_search_count,
    )

    parser.addoption("--postgresql-user", action="store", dest="postgresql_user", help=_help_user)

    parser.addoption("--postgresql-password", action="store", dest="postgresql_password", help=_help_password)

    parser.addoption("--postgresql-options", action="store", dest="postgresql_options", help=_help_options)

    parser.addoption(
        "--postgresql-startparams",
        action="store",
        dest="postgresql_startparams",
        help=_help_startparams,
    )

    parser.addoption(
        "--postgresql-unixsocketdir",
        action="store",
        dest="postgresql_unixsocketdir",
        help=_help_unixsocketdir,
    )

    parser.addoption("--postgresql-dbname", action="store", dest="postgresql_dbname", help=_help_dbname)

    parser.addoption("--postgresql-load", action="append", dest="postgresql_load", help=_help_load)

    parser.addoption(
        "--postgresql-postgres-options",
        action="store",
        dest="postgresql_postgres_options",
        help=_help_postgres_options,
    )

    parser.addoption(
        "--postgresql-drop-test-database",
        action="store_true",
        dest="postgresql_drop_test_database",
        help=_help_drop_test_database,
    )


postgresql_proc = factories.postgresql_proc()
postgresql_noproc = factories.postgresql_noproc()
postgresql = factories.postgresql("postgresql_proc")
postgresql_async = factories.postgresql_async("postgresql_proc")
