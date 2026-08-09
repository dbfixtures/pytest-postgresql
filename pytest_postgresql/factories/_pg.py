"""Internal module for functions handling pg_ctl discovery."""

import logging
import os
import platform
import subprocess
from typing import Iterable

from pytest_postgresql.config import PostgreSQLConfig
from pytest_postgresql.exceptions import ExecutableMissingException

logger = logging.getLogger(__name__)

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
