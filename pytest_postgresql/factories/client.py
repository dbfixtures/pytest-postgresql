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

from typing import Callable, Iterator

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
    dbname: str | None = None,
    isolation_level: "psycopg.IsolationLevel | None" = None,
) -> Callable[[FixtureRequest], Iterator[Connection]]:
    """
    Create a pytest fixture factory that yields a PostgreSQL connection.
    
    Parameters:
        process_fixture_name (str): Name of the pytest fixture that provides the database process executor (used to obtain host, port, user, password, template DB name, and server version).
        dbname (str | None): Database name to connect to; if None, use the executor's database name.
        isolation_level (psycopg.IsolationLevel | None): Optional transaction isolation level to configure the janitor; if None, use the server default.
    
    Returns:
        Callable[[FixtureRequest], Iterator[psycopg.Connection]]: A pytest fixture factory function which, when used in a test, yields an open psycopg Connection to the specified database and ensures database janitor lifecycle management around the connection.
    """

    @pytest.fixture
    def postgresql_factory(request: FixtureRequest) -> Iterator[Connection]:
        """
        Provide a pytest fixture that yields a psycopg Connection to the test PostgreSQL database.
        
        The fixture resolves the process executor and global config from the given request, prepares or drops the test database as configured, and manages the database janitor and connection lifecycle so the connection is open for the duration of the consuming test.
        
        Parameters:
            request (FixtureRequest): Pytest fixture request used to obtain the process fixture and test configuration.
        
        Returns:
            Connection: A psycopg Connection connected to the selected test database; the connection is closed after the fixture completes.
        """
        proc_fixture: PostgreSQLExecutor | NoopExecutor = request.getfixturevalue(process_fixture_name)
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
        if config.drop_test_database:
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