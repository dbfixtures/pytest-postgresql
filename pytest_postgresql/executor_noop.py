# Copyright (C) 2020 by Clearcode <http://clearcode.cc>
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
"""PostgreSQL Noop executor providing connection details for postgres client."""
from typing import Any

import psycopg
from packaging.version import parse


class NoopExecutor:
    """Nooperator executor.

    This executor actually does nothing more than provide connection details
    for existing PostgreSQL server. I.E. one already started either on machine
    or with the use of containerisation like kubernetes or docker compose.
    """

    def __init__(
        self,
        host: str,
        port: str | int,
        user: str,
        options: str,
        dbname: str,
        password: str | None = None,
    ):
        """Initialize nooperator executor mock.

        :param host: Postgresql hostname
        :param port: Postgresql port
        :param user: Postgresql username
        :param options: Additional connection options
        :param password: postgresql password
        :param dbname: postgresql database name
        """
        self.host = host
        self.port = int(port)
        self.user = user
        self.options = options
        self.password = password
        self.dbname = dbname
        self._version: Any = None

    @property
    def template_dbname(self) -> str:
        """Return the template database name."""
        return f"{self.dbname}_tmpl"

    @property
    def version(self) -> Any:
        """Get postgresql's version."""
        if not self._version:
            # could be called before self.dbname will be created.
            # Use default postgres database
            with psycopg.connect(
                dbname="postgres",
                user=self.user,
                host=self.host,
                port=self.port,
                password=self.password,
                options=self.options,
            ) as connection:
                version = str(connection.info.server_version)
                # Pad the version for releases before 10
                # if not we get 90524 instead of 090524
                if len(version) < 6:
                    version = "0" + version
                version_parts = []

                # extract version parts to construct a two-part version
                for i in range(0, len(version), 2):
                    j = i + 2
                    part = version[i:j]
                    if not int(part):
                        continue
                    version_parts.append(part)
                self._version = parse(".".join(version_parts[:2]))
        return self._version
