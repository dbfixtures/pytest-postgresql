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


def _pg_exe(executable: str | None, config: PostgreSQLConfig) -> str:
    """If executable is set, use it. Otherwise best effort to find the executable."""
    postgresql_ctl = executable or config.exec
    # check if that executable exists, as it's no on systems' PATH
    # only replace it if executable isn't passed manually
    if not os.path.exists(postgresql_ctl) and executable is None:
        try:
            pg_bindir = subprocess.check_output(["pg_config", "--bindir"], universal_newlines=True).strip()
        except FileNotFoundError as ex:
            raise ExecutableMissingException("Could not find pg_config executable. Is it in system $PATH?") from ex
        postgresql_ctl = os.path.join(pg_bindir, "pg_ctl")
    return postgresql_ctl


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
    options: str = "",
    startparams: str | None = None,
    unixsocketdir: str | None = None,
    postgres_options: str | None = None,
    load: list[Callable | str | Path] | None = None,
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
            janitor = DatabaseJanitor(
                user=postgresql_executor.user,
                host=postgresql_executor.host,
                port=postgresql_executor.port,
                dbname=postgresql_executor.template_dbname,
                as_template=True,
                version=postgresql_executor.version,
                password=postgresql_executor.password,
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
            try:
                _stop_executor_best_effort()
            finally:
                _unlink_port_sentinel()
            raise

    return postgresql_proc_fixture
