# Copyright (C) 2016-2020 by Clearcode <http://clearcode.cc>
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
"""PostgreSQL executor crafter around pg_ctl."""

import logging
import os
import os.path
import platform
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any, Optional, TypeVar

from mirakuru import TCPExecutor
from mirakuru.exceptions import ProcessFinishedWithError
from packaging.version import parse

from pytest_postgresql.exceptions import ExecutableMissingException, PostgreSQLUnsupported

logger = logging.getLogger(__name__)

_LOCALE = "C.UTF-8"

if platform.system() == "Darwin":
    _LOCALE = "en_US.UTF-8"


T = TypeVar("T", bound="PostgreSQLExecutor")


class PostgreSQLExecutor(TCPExecutor):
    """PostgreSQL executor running on pg_ctl.

    Based over an `pg_ctl program
    <http://www.postgresql.org/docs/current/static/app-pg-ctl.html>`_
    """

    # Base PostgreSQL start command template - cross-platform compatible
    # Use unified format without single quotes around values
    # This format works on both Windows and Unix systems
    BASE_PROC_START_COMMAND = (
        '{executable} start -D "{datadir}" '
        '-o "-F -p {port} -c log_destination=stderr '
        "-c logging_collector=off "
        '-c unix_socket_directories={unixsocketdir} {postgres_options}" '
        '-l "{logfile}" {startparams}'
    )

    VERSION_RE = re.compile(r".* (?P<version>\d+(?:\.\d+)?)")
    MIN_SUPPORTED_VERSION = parse("14")

    def __init__(
        self,
        executable: str,
        host: str,
        port: int,
        datadir: str,
        unixsocketdir: str,
        logfile: str,
        startparams: str,
        dbname: str,
        shell: bool = False,
        timeout: Optional[int] = 60,
        sleep: float = 0.1,
        user: str = "postgres",
        password: str = "",
        options: str = "",
        postgres_options: str = "",
    ):
        """Initialize PostgreSQLExecutor executor.

        :param executable: pg_ctl location
        :param host: host under which process is accessible
        :param port: port under which process is accessible
        :param datadir: path to postgresql datadir
        :param unixsocketdir: path to socket directory
        :param logfile: path to logfile for postgresql
        :param startparams: additional start parameters
        :param shell: see `subprocess.Popen`
        :param timeout: time to wait for process to start or stop.
            if None, wait indefinitely.
        :param sleep: how often to check for start/stop condition
        :param user: postgresql's username used to manage
            and access PostgreSQL
        :param password: optional password for the user
        :param dbname: database name (might not yet exist)
        :param options:
        :param postgres_options: extra arguments to `postgres start`
        """
        self._directory_initialised = False
        self.executable = executable
        self.user = user
        self.password = password
        self.dbname = dbname
        self.options = options
        self.datadir = datadir
        self.unixsocketdir = unixsocketdir
        self.logfile = logfile
        self.startparams = startparams
        self.postgres_options = postgres_options
        command = self.BASE_PROC_START_COMMAND.format(
            executable=self.executable,
            datadir=self.datadir,
            port=port,
            unixsocketdir=self.unixsocketdir,
            logfile=self.logfile,
            startparams=self.startparams,
            postgres_options=self.postgres_options,
        )
        super().__init__(
            command,
            host,
            port,
            shell=shell,
            timeout=timeout,
            sleep=sleep,
            envvars={
                "LC_ALL": _LOCALE,
                "LC_CTYPE": _LOCALE,
                "LANG": _LOCALE,
            },
        )

    @property
    def template_dbname(self) -> str:
        """Return the template database name."""
        return f"{self.dbname}_tmpl"

    def start(self: T) -> T:
        """Add check for postgresql version before starting process."""
        if self.version < self.MIN_SUPPORTED_VERSION:
            raise PostgreSQLUnsupported(
                f"Your version of PostgreSQL is not supported. "
                f"Consider updating to PostgreSQL {self.MIN_SUPPORTED_VERSION} at least. "
                f"The currently installed version of PostgreSQL: {self.version}."
            )
        self.init_directory()
        return super().start()

    def clean_directory(self) -> None:
        """Remove directory created for postgresql run."""
        if os.path.isdir(self.datadir):
            shutil.rmtree(self.datadir)
        self._directory_initialised = False

    def init_directory(self) -> None:
        """Initialize postgresql data directory.

        See `Initialize postgresql data directory
            <www.postgresql.org/docs/9.5/static/app-initdb.html>`_
        """
        # only make sure it's removed if it's handled by this exact process
        if self._directory_initialised:
            return
        # remove old one if exists first.
        self.clean_directory()
        init_directory = [self.executable, "initdb", "--pgdata", self.datadir]
        options = ["--username=%s" % self.user]

        if self.password:
            with tempfile.NamedTemporaryFile() as password_file:
                options += ["--auth=password", "--pwfile=%s" % password_file.name]
                if hasattr(self.password, "encode"):
                    password = self.password.encode("utf-8")
                else:
                    password = self.password  # type: ignore[assignment]
                password_file.write(password)
                password_file.flush()
                init_directory += ["-o", " ".join(options)]
                # Passing envvars to command to avoid weird MacOs error.
                subprocess.check_output(init_directory, env=self.envvars)
        else:
            options += ["--auth=trust"]
            init_directory += ["-o", " ".join(options)]
            # Passing envvars to command to avoid weird MacOs error.
            subprocess.check_output(init_directory, env=self.envvars)

        self._directory_initialised = True

    def wait_for_postgres(self) -> None:
        """Wait for postgresql being started."""
        if "-w" not in self.startparams:
            return
        # wait until server is running
        while 1:
            if self.running():
                break
            time.sleep(1)

    @property
    def version(self) -> Any:
        """Detect postgresql version."""
        try:
            version_string = subprocess.check_output([self.executable, "--version"]).decode("utf-8")
        except FileNotFoundError as ex:
            raise ExecutableMissingException(
                f"Could not found {self.executable}. Is PostgreSQL server installed? "
                f"Alternatively pg_config installed might be from different "
                f"version that postgresql-server."
            ) from ex
        matches = self.VERSION_RE.search(version_string)
        assert matches is not None
        return parse(matches.groupdict()["version"])

    def running(self) -> bool:
        """Check if server is running."""
        if not os.path.exists(self.datadir):
            return False
        status_code = subprocess.getstatusoutput(f'{self.executable} status -D "{self.datadir}"')[0]
        return status_code == 0

    def _windows_terminate_process(self, _sig: Optional[int] = None) -> None:
        """Terminate process on Windows.

        :param _sig: Signal parameter (unused on Windows but included for consistency)
        """
        if self.process is None:
            return

        try:
            # On Windows, try to terminate gracefully first
            self.process.terminate()
            # Give it a chance to terminate gracefully
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # If it doesn't terminate gracefully, force kill
                self.process.kill()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning(
                        "Process %s could not be cleaned up after kill() and may be a zombie process",
                        self.process.pid if self.process else "unknown",
                    )
        except (OSError, AttributeError) as e:
            # Process might already be dead or other issues
            logger.debug(
                "Exception during Windows process termination: %s: %s",
                type(e).__name__,
                e,
            )

    def stop(self: T, sig: Optional[int] = None, exp_sig: Optional[int] = None) -> T:
        """Issue a stop request to executable."""
        subprocess.check_output(
            [self.executable, "stop", "-D", self.datadir, "-m", "f"],
        )
        try:
            if platform.system() == "Windows":
                self._windows_terminate_process(sig)
            else:
                super().stop(sig, exp_sig)
        except ProcessFinishedWithError:
            # Finished, leftovers ought to be cleaned afterwards anyway
            pass
        except AttributeError:
            # Fallback for edge cases where os.killpg doesn't exist (e.g., Windows)
            if not hasattr(os, "killpg"):
                self._windows_terminate_process(sig)
            else:
                raise
        return self

    def __del__(self) -> None:
        """Make sure the directories are properly removed at the end."""
        try:
            super().__del__()
        finally:
            self.clean_directory()
