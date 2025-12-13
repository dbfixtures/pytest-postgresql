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

import os.path
import platform
import subprocess
from pathlib import Path
from typing import Callable, Iterable, Iterator

import port_for
import pytest
from _pytest.scope import _ScopeName
from port_for import PortForException, get_port
from pytest import FixtureRequest, TempPathFactory

from pytest_postgresql.config import PostgresqlConfigDict, get_config
from pytest_postgresql.exceptions import ExecutableMissingException
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor

PortType = port_for.PortType  # mypy requires explicit export


def _pg_exe(executable: str | None, config: PostgresqlConfigDict) -> str:
    """If executable is set, use it. Otherwise best effort to find the executable."""
    postgresql_ctl = executable or config["exec"]
    # check if that executable exists, as it's no on systems' PATH
    # only replace it if executable isn't passed manually
    if not os.path.exists(postgresql_ctl) and executable is None:
        try:
            pg_bindir = subprocess.check_output(["pg_config", "--bindir"], universal_newlines=True).strip()
        except FileNotFoundError as ex:
            raise ExecutableMissingException("Could not found pg_config executable. Is it in systenm $PATH?") from ex
        postgresql_ctl = os.path.join(pg_bindir, "pg_ctl")
    return postgresql_ctl


def _pg_port(port: PortType | None, config: PostgresqlConfigDict, excluded_ports: Iterable[int]) -> int:
    """User specified port, otherwise find an unused port from config."""
    pg_port = get_port(port, excluded_ports) or get_port(config["port"], excluded_ports)
    assert pg_port is not None
    return pg_port


def _prepare_dir(tmpdir: Path, pg_port: PortType) -> tuple[Path, Path]:
    """Prepare a directory for the executor."""
    datadir = tmpdir / f"data-{pg_port}"
    datadir.mkdir()
    logfile_path = tmpdir / f"postgresql.{pg_port}.log"

    if platform.system() == "FreeBSD":
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
    scope: _ScopeName = "session",
) -> Callable[[FixtureRequest, TempPathFactory], Iterator[PostgreSQLExecutor]]:
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
    :param scope: fixture scope; by default "session" which is recommended.
    :returns: function which makes a postgresql process
    """

    @pytest.fixture(scope=scope)
    def postgresql_proc_fixture(
        request: FixtureRequest, tmp_path_factory: TempPathFactory
    ) -> Iterator[PostgreSQLExecutor]:
        """Process fixture for PostgreSQL.

        :param request: fixture request object
        :param tmp_path_factory: temporary path object (fixture)
        :returns: tcp executor
        """
        config = get_config(request)
        pg_dbname = dbname or config["dbname"]
        pg_load = load or config["load"]
        postgresql_ctl = _pg_exe(executable, config)
        port_path = tmp_path_factory.getbasetemp()
        if hasattr(request.config, "workerinput"):
            port_path = tmp_path_factory.getbasetemp().parent

        n = 0
        used_ports: set[int] = set()
        while True:
            try:
                pg_port = _pg_port(port, config, used_ports)
                port_filename_path = port_path / f"postgresql-{pg_port}.port"
                if pg_port in used_ports:
                    raise PortForException(
                        f"Port {pg_port} already in use, probably by other instances of the test. "
                        f"{port_filename_path} is already used."
                    )
                used_ports.add(pg_port)
                with (port_filename_path).open("x") as port_file:
                    port_file.write(f"pg_port {pg_port}\n")
                break
            except FileExistsError:
                if n >= config["port_search_count"]:
                    raise PortForException(
                        f"Attempted {n} times to select ports. "
                        f"All attempted ports: {', '.join(map(str, used_ports))} are already "
                        f"in use, probably by other instances of the test."
                    )
                n += 1

        tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.fixturename}")
        datadir, logfile_path = _prepare_dir(tmpdir, str(pg_port))

        postgresql_executor = PostgreSQLExecutor(
            executable=postgresql_ctl,
            host=host or config["host"],
            port=pg_port,
            user=user or config["user"],
            password=password or config["password"],
            dbname=pg_dbname,
            options=options or config["options"],
            datadir=str(datadir),
            unixsocketdir=unixsocketdir or config["unixsocketdir"],
            logfile=str(logfile_path),
            startparams=startparams or config["startparams"],
            postgres_options=postgres_options or config["postgres_options"],
        )
        # start server
        with postgresql_executor:
            postgresql_executor.wait_for_postgres()
            janitor = DatabaseJanitor(
                user=postgresql_executor.user,
                host=postgresql_executor.host,
                port=postgresql_executor.port,
                template_dbname=postgresql_executor.template_dbname,
                version=postgresql_executor.version,
                password=postgresql_executor.password,
            )
            if config["drop_test_database"]:
                janitor.drop()
            with janitor:
                for load_element in pg_load:
                    janitor.load(load_element)
                yield postgresql_executor

    return postgresql_proc_fixture
