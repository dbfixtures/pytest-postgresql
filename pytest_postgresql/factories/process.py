# Copyright (C) 2013-2021 by Clearcode <http://clearcode.cc>
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
"""Fixture factory for postgresql process."""

import logging
import os
import os.path
import platform
import subprocess
import tempfile
from pathlib import Path
from typing import Callable, Iterable

import port_for
import pytest
from port_for import PortForException, get_port
from pytest import FixtureRequest, TempPathFactory

from pytest_postgresql.config import PostgreSQLConfig, get_config
from pytest_postgresql.exceptions import ExecutableMissingException
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor

logger = logging.getLogger(__name__)

PortType = port_for.PortType  # mypy requires explicit export


PG_CTL_NAMES = ("pg_ctl.exe", "pg_ctl") if platform.system() == "Windows" else ("pg_ctl",)
"""Names to look for when probing the filesystem for pg_ctl.

os.path.isfile matches the literal name, so finding the binary on Windows means asking
for pg_ctl.exe. Launching it is more forgiving: Windows appends .exe to an extensionless
program name, which is why the old code could run a path it never checked.
"""

PG_CONFIG_TIMEOUT = 60
"""Seconds to wait for pg_config, so a hung binary cannot stall fixture setup.

Matches PostgreSQLExecutor's default subprocess timeout.
"""


def _pg_bindir(checked: Iterable[str]) -> str:
    """Ask pg_config where PostgreSQL keeps its binaries.

    pg_config comes from the client development package, which on most distributions
    can be installed without the server, so its answer is a hint rather than an answer.

    :param checked: locations probed so far, to name in the error if pg_config can't be run
    :raises ExecutableMissingException: pg_config is missing, unusable, hanging or failing.
        TimeoutExpired is a SubprocessError, so it needs no branch of its own.
    """
    try:
        return subprocess.check_output(
            ["pg_config", "--bindir"], universal_newlines=True, timeout=PG_CONFIG_TIMEOUT
        ).strip()
    except (OSError, subprocess.SubprocessError) as ex:
        logger.debug("Could not read the binaries directory from pg_config: %s", ex)
        raise ExecutableMissingException.pg_config_unusable(checked) from ex


def _is_executable(path: str) -> bool:
    """Whether path is a file this user can actually run.

    isfile is needed alongside the access check because X_OK on a directory only means
    it can be traversed. On Windows os.access ignores X_OK, so this is an existence test.
    """
    return os.path.isfile(path) and os.access(path, os.X_OK)


def _pg_exe(executable: str | None, config: PostgreSQLConfig) -> str:
    """If executable is set, use it. Otherwise best effort to find the executable."""
    # an explicitly passed executable is taken at face value, it's not ours to second-guess
    if executable is not None:
        return executable
    postgresql_ctl = config.exec
    # check if that executable exists, as it's not on systems' PATH
    if _is_executable(postgresql_ctl):
        return postgresql_ctl
    checked = [postgresql_ctl]
    bindir = _pg_bindir(checked)
    for name in PG_CTL_NAMES:
        candidate = os.path.join(bindir, name)
        if candidate in checked:
            continue
        checked.append(candidate)
        if _is_executable(candidate):
            return candidate
    raise ExecutableMissingException.not_in_bindir(bindir, checked)


def _pg_port(port: PortType | None, config: PostgreSQLConfig, excluded_ports: Iterable[int]) -> int:
    """User specified port, otherwise find an unused port from config."""
    pg_port = get_port(port, excluded_ports) or get_port(config.port, excluded_ports)
    assert pg_port is not None
    return pg_port


def _prepare_dir(tmpdir: Path, pg_port: PortType, session_token: str) -> tuple[Path, Path]:
    """Prepare a directory for the executor."""
    if platform.system() == "Windows":
        # initdb on Windows cannot mkdir through existing pytest temp parents.
        temp_dir = Path(tempfile.gettempdir())
        datadir = temp_dir / f"pytest-postgresql-data-{session_token}-{pg_port}"
        # Keep the logfile on the same drive as pgdata; pytest basetemp can be
        # on a different volume and pg_ctl rejects the -l path with Access denied.
        logfile_path = temp_dir / f"pytest-postgresql-{session_token}-{pg_port}.log"
    else:
        datadir = tmpdir / f"data-{pg_port}"
        logfile_path = tmpdir / f"postgresql.{pg_port}.log"

    if platform.system() == "FreeBSD":
        datadir.mkdir()
        with (datadir / "pg_hba.conf").open(mode="a") as conf_file:
            conf_file.write("host all all 0.0.0.0/0 trust\n")
    return datadir, logfile_path


def postgresql_proc(
    executable: str | None = None,
    host: str | None = None,
    port: PortType | None = -1,
    user: str | None = None,
    password: str | None = None,
    dbname: str | None = None,
    *,
    options: str = "",
    startparams: str | None = None,
    unixsocketdir: str | None = None,
    postgres_options: str | None = None,
    load: list[Callable | str | Path] | None = None,
    load_autocommit: bool | None = None,
) -> Callable[[FixtureRequest, TempPathFactory], PostgreSQLExecutor]:
    """Postgresql process factory.

    :param executable: path to postgresql_ctl
    :param host: hostname
    :param port:
        exact port (e.g. '8000', 8000)
        randomly selected port (None) - any random available port
        -1 - command line or pytest.ini configured port
        [(2000,3000)] or (2000,3000) - random available port from a given range
        [{4002,4003}] or {4002,4003} - random of 4002 or 4003 ports
        [(2000,3000), {4002,4003}] - random of given range and set
    :param user: postgresql username
    :param password: postgresql password
    :param dbname: postgresql database name
    :param options: Postgresql connection options
    :param startparams: postgresql starting parameters
    :param unixsocketdir: directory to create postgresql's unixsockets
    :param postgres_options: Postgres executable options for use by pg_ctl
    :param load: List of functions used to initialize database's template.
    :param load_autocommit: run the SQL loader connection with autocommit on.
        Required for statements that cannot run inside a transaction block,
        e.g. ``CREATE DATABASE`` in a loaded ``.sql`` file.
    :returns: function which makes a postgresql process
    """

    @pytest.fixture(scope="session")
    def postgresql_proc_fixture(request: FixtureRequest, tmp_path_factory: TempPathFactory) -> PostgreSQLExecutor:
        """Process fixture for PostgreSQL.

        :param request: fixture request object
        :param tmp_path_factory: temporary path object (fixture)
        :returns: tcp executor
        """
        config = get_config(request)
        pg_dbname = dbname or config.dbname
        pg_load = load or config.load
        postgresql_ctl = _pg_exe(executable, config)
        port_path = tmp_path_factory.getbasetemp()
        if hasattr(request.config, "workerinput"):
            port_path = tmp_path_factory.getbasetemp().parent

        n = 0
        used_ports: set[int] = set()
        port_filename_path: Path | None = None
        postgresql_executor: PostgreSQLExecutor | None = None
        session_token = str(os.getpid())

        def _unlink_port_sentinel() -> None:
            if port_filename_path is not None:
                port_filename_path.unlink(missing_ok=True)

        def _stop_executor_best_effort() -> None:
            if postgresql_executor is None:
                return
            try:
                postgresql_executor.stop()
            except Exception:
                logger.exception("Failed to stop PostgreSQL executor during cleanup")

        def _cleanup_executor_resources() -> None:
            try:
                _stop_executor_best_effort()
            finally:
                if postgresql_executor is not None:
                    try:
                        postgresql_executor.clean_directory()
                    except Exception:
                        logger.exception("Failed to clean PostgreSQL data directory during cleanup")
                    try:
                        logfile = Path(postgresql_executor.logfile)
                        if logfile.is_file():
                            logfile.unlink(missing_ok=True)
                    except OSError:
                        logger.exception("Failed to remove PostgreSQL log file during cleanup")
                _unlink_port_sentinel()

        try:
            while True:
                try:
                    pg_port = _pg_port(port, config, used_ports)
                    candidate_port_file = port_path / f"postgresql-{pg_port}.port"
                    if pg_port in used_ports:
                        raise PortForException(
                            f"Port {pg_port} already in use, probably by other instances of the test. "
                            f"{candidate_port_file} is already used."
                        )
                    used_ports.add(pg_port)
                    with candidate_port_file.open("x") as port_file:
                        port_file.write(f"pg_port {pg_port}\n")
                    port_filename_path = candidate_port_file
                    break
                except FileExistsError:
                    if n >= config.port_search_count:
                        raise PortForException(
                            f"Attempted {n} times to select ports. "
                            f"All attempted ports: {', '.join(map(str, used_ports))} are already "
                            f"in use, probably by other instances of the test."
                        ) from None
                    n += 1

            tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.fixturename}")
            assert tmpdir.is_dir()
            datadir, logfile_path = _prepare_dir(tmpdir, str(pg_port), session_token)

            postgresql_executor = PostgreSQLExecutor(
                executable=postgresql_ctl,
                host=host or config.host,
                port=pg_port,
                user=user or config.user,
                password=password or config.password,
                dbname=pg_dbname,
                options=options or config.options,
                datadir=str(datadir.resolve()),
                unixsocketdir=unixsocketdir or config.unixsocketdir,
                logfile=str(logfile_path.resolve()),
                startparams=startparams or config.startparams,
                postgres_options=postgres_options or config.postgres_options,
            )
            postgresql_executor.start()
            postgresql_executor.wait_for_postgres()
            janitor_load_autocommit = config.load_autocommit
            if load_autocommit is not None:
                janitor_load_autocommit = load_autocommit
            janitor = DatabaseJanitor(
                user=postgresql_executor.user,
                host=postgresql_executor.host,
                port=postgresql_executor.port,
                dbname=postgresql_executor.template_dbname,
                maintenance_dbname=postgresql_executor.maintenance_dbname,
                as_template=True,
                version=postgresql_executor.version,
                password=postgresql_executor.password,
                autocommit=janitor_load_autocommit,
            )
            if config.drop_test_database:
                janitor.drop()
            janitor.init()
            for load_element in pg_load:
                janitor.load(load_element)

            def cleanup() -> None:
                try:
                    janitor.drop()
                finally:
                    _cleanup_executor_resources()

            request.addfinalizer(cleanup)
            return postgresql_executor
        except Exception:
            _cleanup_executor_resources()
            raise

    return postgresql_proc_fixture
