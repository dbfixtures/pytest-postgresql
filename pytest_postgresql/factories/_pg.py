"""Internal module for functions handling pg_ctl discovery."""

import logging
import os
import platform
import shutil
import subprocess
from pathlib import Path

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

_WINDOWS_PROGRAM_FILES_FALLBACK = r"C:\Program Files"


def _pg_bindir() -> str:
    """Ask pg_config where PostgreSQL keeps its binaries.

    pg_config comes from the client development package, which on most distributions
    can be installed without the server, so its answer is a hint rather than an answer.

    :raises OSError: pg_config is missing or not executable
    :raises subprocess.SubprocessError: pg_config failed or hung.
        TimeoutExpired is a SubprocessError, so it needs no branch of its own.
    """
    try:
        bindir = subprocess.check_output(
            ["pg_config", "--bindir"], universal_newlines=True, timeout=PG_CONFIG_TIMEOUT
        ).strip()
    except (OSError, subprocess.SubprocessError) as ex:
        logger.debug("Could not read the binaries directory from pg_config: %s", ex)
        raise
    logger.debug("pg_config --bindir reported %s", bindir)
    return bindir


def _is_executable(path: str) -> bool:
    """Whether path is a file this user can actually run.

    isfile is needed alongside the access check because X_OK on a directory only means
    it can be traversed. On Windows os.access ignores X_OK, so this is an existence test.
    """
    return os.path.isfile(path) and os.access(path, os.X_OK)


def _windows_postgresql_root() -> Path:
    """Directory under which the Windows installer places versioned PostgreSQL copies."""
    program_files = os.environ.get("ProgramFiles", _WINDOWS_PROGRAM_FILES_FALLBACK)
    return Path(program_files) / "PostgreSQL"


def _windows_version_sort_key(name: str) -> tuple[int, tuple[int, ...] | str]:
    """Sort key for Windows PostgreSQL version directory names, newest first when reversed.

    Numeric versions (``16``, ``16.4``) rank above non-numeric names so a real install
    wins over stray folders; non-numeric names fall back to lexicographic comparison.
    """
    parts = name.split(".")
    try:
        numbers = tuple(int(part) for part in parts)
    except ValueError:
        return (0, name)
    return (1, numbers)


def _windows_program_files_pg_ctls() -> list[str]:
    """pg_ctl paths under Program Files/PostgreSQL/*/bin, newest version first."""
    root = _windows_postgresql_root()
    if not root.is_dir():
        logger.debug("Windows PostgreSQL directory does not exist: %s", root)
        return []
    found: list[tuple[str, str]] = []
    try:
        version_dirs = [entry for entry in root.iterdir() if entry.is_dir()]
    except OSError as ex:
        logger.debug("Could not scan %s: %s", root, ex)
        return []
    for version_dir in version_dirs:
        bindir = version_dir / "bin"
        for name in PG_CTL_NAMES:
            candidate = bindir / name
            if candidate.is_file():
                found.append((version_dir.name, str(candidate)))
    found.sort(key=lambda item: _windows_version_sort_key(item[0]), reverse=True)
    paths = [path for _, path in found]
    logger.debug("Windows Program Files pg_ctl candidates (newest first): %s", paths)
    return paths


def _which_pg_ctl() -> str | None:
    """Return pg_ctl from PATH, if it is a file this user can run."""
    for name in PG_CTL_NAMES:
        found = shutil.which(name)
        if found is None:
            logger.debug("which(%s) found nothing", name)
            continue
        logger.debug("which(%s) found %s", name, found)
        if _is_executable(found):
            return found
    return None


def _remember(checked: list[str], candidate: str) -> None:
    """Append candidate to the probed-locations list unless it is already there."""
    if candidate not in checked:
        checked.append(candidate)


def _from_config_exec(config: PostgreSQLConfig, checked: list[str]) -> str | None:
    """Use postgresql_exec when it names a real executable."""
    postgresql_ctl = config.exec
    if not postgresql_ctl:
        logger.debug("No postgresql_exec configured; skipping that probe")
        return None
    logger.debug("Checking configured postgresql_exec: %s", postgresql_ctl)
    if _is_executable(postgresql_ctl):
        logger.debug("Using configured postgresql_exec: %s", postgresql_ctl)
        return postgresql_ctl
    _remember(checked, postgresql_ctl)
    return None


def _from_pg_config(checked: list[str], causes: list[str]) -> str | None:
    """Use pg_ctl next to the binaries directory pg_config reports, if any."""
    try:
        bindir = _pg_bindir()
    except (OSError, subprocess.SubprocessError) as ex:
        reason = f"{type(ex).__name__}: {ex}"
        logger.debug("pg_config unusable: %s", reason)
        causes.append(
            f"pg_config could not be run either ({reason}), so it could not be used to locate the PostgreSQL binaries."
        )
        return None
    for name in PG_CTL_NAMES:
        candidate = os.path.join(bindir, name)
        _remember(checked, candidate)
        if _is_executable(candidate):
            logger.debug("Using pg_ctl from pg_config bindir: %s", candidate)
            return candidate
    causes.append(
        f"pg_config reports PostgreSQL binaries live in {bindir}, but there's no pg_ctl there. "
        f"That usually means only the PostgreSQL client libraries are installed "
        f"(Debian/Ubuntu's libpq-dev, for example), without the matching server package."
    )
    return None


def _from_windows_program_files(checked: list[str]) -> str | None:
    """Use the newest Windows installer copy of pg_ctl, if this is Windows."""
    if platform.system() != "Windows":
        return None
    logger.debug("Probing %s for pg_ctl", _windows_postgresql_root())
    for candidate in _windows_program_files_pg_ctls():
        _remember(checked, candidate)
        if _is_executable(candidate):
            logger.debug("Using pg_ctl from Program Files: %s", candidate)
            return candidate
    return None


def _from_path(checked: list[str]) -> str | None:
    """Use pg_ctl from PATH."""
    found = _which_pg_ctl()
    if found is None:
        logger.debug("shutil.which did not find pg_ctl on PATH")
        return None
    _remember(checked, found)
    if _is_executable(found):
        logger.debug("Using pg_ctl from PATH: %s", found)
        return found
    return None


def _pg_exe(executable: str | None, config: PostgreSQLConfig) -> str:
    """If executable is set, use it. Otherwise best effort to find the executable."""
    if executable is not None:
        logger.debug("Using factory executable: %s", executable)
        return executable
    checked: list[str] = []
    causes: list[str] = []
    found = (
        _from_config_exec(config, checked)
        or _from_pg_config(checked, causes)
        or _from_windows_program_files(checked)
        or _from_path(checked)
    )
    if found is not None:
        return found
    logger.debug("pg_ctl discovery failed; checked %s", checked)
    raise ExecutableMissingException.discovery_failed(causes, checked)
