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
"""Fixture factory for existing postgresql server."""

import os
from pathlib import Path
from typing import Callable, Iterator

import pytest
from pytest import FixtureRequest

from pytest_postgresql.config import get_config
from pytest_postgresql.executor_noop import NoopExecutor
from pytest_postgresql.janitor import DatabaseJanitor


def xdistify_dbname(dbname: str) -> str:
    """Modify the database name depending on the presence and usage of xdist."""
    xdist_worker = os.getenv("PYTEST_XDIST_WORKER")
    if xdist_worker:
        return f"{dbname}{xdist_worker}"
    return dbname


def postgresql_noproc(
    host: str | None = None,
    port: str | int | None = None,
    user: str | None = None,
    password: str | None = None,
    dbname: str | None = None,
    options: str = "",
    load: list[Callable | str | Path] | None = None,
    depends_on: str | None = None,
) -> Callable[[FixtureRequest], Iterator[NoopExecutor]]:
    """Postgresql noprocess factory.

    :param host: hostname
    :param port: exact port (e.g. '8000', 8000)
    :param user: postgresql username
    :param password: postgresql password
    :param dbname: postgresql database name
    :param options: Postgresql connection options
    :param load: List of functions used to initialize database's template.
    :param depends_on: Optional name of the fixture to depend on.
    :returns: function which makes a postgresql process
    """

    @pytest.fixture(scope="session")
    def postgresql_noproc_fixture(request: FixtureRequest) -> Iterator[NoopExecutor]:
        """Noop Process fixture for PostgreSQL.

        :param request: fixture request object
        :returns: tcp executor-like object
        """
        config = get_config(request)

        if depends_on:
            base = request.getfixturevalue(depends_on)
            pg_host = host or base.host
            pg_port = port or base.port
            pg_user = user or base.user
            pg_password = password or base.password
            pg_options = options or base.options
            base_template_dbname = base.template_dbname
        else:
            pg_host = host or config.host
            pg_port = port or config.port or 5432
            pg_user = user or config.user
            pg_password = password or config.password
            pg_options = options or config.options
            base_template_dbname = None

        pg_dbname = xdistify_dbname(dbname or config.dbname)
        pg_load = load or config.load
        drop_test_database = config.drop_test_database

        # In this case there's a risk that both seeded and depends_on fixture
        # might end up with the same configured dbname.
        if depends_on and not dbname:
            noop_exec_dbname = f"{pg_dbname}_{depends_on}"
        else:
            noop_exec_dbname = pg_dbname

        noop_exec = NoopExecutor(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=noop_exec_dbname,
            options=pg_options,
        )
        janitor = DatabaseJanitor(
            user=noop_exec.user,
            host=noop_exec.host,
            port=noop_exec.port,
            dbname=noop_exec.template_dbname,
            template_dbname=base_template_dbname,
            as_template=True,
            version=noop_exec.version,
            password=noop_exec.password,
        )
        if drop_test_database:
            janitor.drop()
        with janitor:
            for load_element in pg_load:
                janitor.load(load_element)
            yield noop_exec

    return postgresql_noproc_fixture
