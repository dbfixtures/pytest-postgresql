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
"""Fixture factory for postgresql client."""
from typing import Callable, Iterator, Optional, Union

import psycopg
import pytest
from psycopg import Connection
from pytest import FixtureRequest

from pytest_postgresql.config import get_config
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.executor_noop import NoopExecutor
from pytest_postgresql.janitor import DatabaseJanitor


def postgresql(
    process_fixture_name: str,
    scope: str = 'function',
    dbname: Optional[str] = None,
    isolation_level: "Optional[psycopg.IsolationLevel]" = None,
) -> Callable[[FixtureRequest], Iterator[Connection]]:
    """Return connection fixture factory for PostgreSQL.

    :param process_fixture_name: name of the process fixture
    :param dbname: database name
    :param isolation_level: optional postgresql isolation level
                            defaults to server's default
    :returns: function which makes a connection to postgresql
    """

    @pytest.fixture(scope=scope)
    def postgresql_factory(request: FixtureRequest) -> Iterator[Connection]:
        """Fixture factory for PostgreSQL.

        :param request: fixture request object
        :returns: postgresql client
        """
        proc_fixture: Union[PostgreSQLExecutor, NoopExecutor] = request.getfixturevalue(
            process_fixture_name
        )
        config = get_config(request)

        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_password = proc_fixture.password
        pg_options = proc_fixture.options
        pg_db = dbname or proc_fixture.dbname
        janitor = DatabaseJanitor(
            user=pg_user,
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            template_dbname=proc_fixture.template_dbname,
            version=proc_fixture.version,
            password=pg_password,
            isolation_level=isolation_level,
        )
        if config["drop_test_database"]:
            janitor.drop()
        with janitor:
            db_connection: Connection = psycopg.connect(
                dbname=pg_db,
                user=pg_user,
                password=pg_password,
                host=pg_host,
                port=pg_port,
                options=pg_options,
            )
            yield db_connection
            db_connection.close()

    return postgresql_factory
