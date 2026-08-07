"""Tests for the pg_ctl discovery performed by the process fixture factory."""

import platform
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from pytest_postgresql.config import PostgreSQLConfig
from pytest_postgresql.exceptions import ExecutableMissingException
from pytest_postgresql.factories import process

PROCESS = "pytest_postgresql.factories.process"


def make_config(exec_path: str) -> PostgreSQLConfig:
    """Config with the executable set, the rest being irrelevant for discovery."""
    return PostgreSQLConfig(
        exec=exec_path,
        host="127.0.0.1",
        port=None,
        port_search_count=5,
        user="postgres",
        password=None,
        options="",
        startparams="-w",
        unixsocketdir="/tmp",
        dbname="tests",
        maintenance_dbname="postgres",
        load=[],
        load_autocommit=False,
        postgres_options="",
        drop_test_database=False,
    )


@pytest.fixture
def config(tmp_path: Path) -> PostgreSQLConfig:
    """Config pointing its executable at a path that does not exist."""
    return make_config(str(tmp_path / "nonexistent" / "bin" / "pg_ctl"))


def make_pg_ctl(bindir: Path, name: str = "pg_ctl", mode: int = 0o755) -> Path:
    """Create a stand-in for pg_ctl in bindir, executable unless told otherwise."""
    bindir.mkdir(parents=True, exist_ok=True)
    pg_ctl = bindir / name
    pg_ctl.write_text("")
    pg_ctl.chmod(mode)
    return pg_ctl


def test_explicit_executable_is_not_second_guessed(config: PostgreSQLConfig) -> None:
    """An executable passed to the factory is returned even if it does not exist."""
    assert process._pg_exe("/nowhere/pg_ctl", config) == "/nowhere/pg_ctl"


def test_existing_configured_executable_wins(tmp_path: Path) -> None:
    """A configured executable that exists is used without consulting pg_config."""
    pg_ctl = make_pg_ctl(tmp_path / "bin")
    config = make_config(str(pg_ctl))
    with patch.object(process, "_pg_bindir") as bindir_mock:
        assert process._pg_exe(None, config) == str(pg_ctl)
    bindir_mock.assert_not_called()


def test_pg_config_bindir_is_used(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """pg_ctl found next to the binaries pg_config points at is used."""
    pg_ctl = make_pg_ctl(tmp_path / "pgconfig-bin")
    with patch.object(process, "_pg_bindir", return_value=str(pg_ctl.parent)):
        assert process._pg_exe(None, config) == str(pg_ctl)


def test_windows_executable_suffix_is_probed(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """On Windows the binary is found as pg_ctl.exe.

    Probing the filesystem does no PATHEXT resolution, so looking for a bare "pg_ctl"
    would miss every Windows install and turn discovery into a hard failure there.
    """
    pg_ctl = make_pg_ctl(tmp_path / "windows-bin", name="pg_ctl.exe")
    with (
        patch.object(process, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")),
        patch.object(process, "_pg_bindir", return_value=str(pg_ctl.parent)),
    ):
        assert process._pg_exe(None, config) == str(pg_ctl)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
def test_non_executable_configured_path_is_rejected(tmp_path: Path) -> None:
    """A configured pg_ctl that exists but cannot be run is not handed back.

    It used to be accepted, deferring the failure to the version check.
    """
    pg_ctl = make_pg_ctl(tmp_path / "bin", mode=0o644)
    config = make_config(str(pg_ctl))
    with patch(f"{PROCESS}.subprocess.check_output", side_effect=FileNotFoundError("pg_config")):
        with pytest.raises(ExecutableMissingException, match="Could not find pg_ctl"):
            process._pg_exe(None, config)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
def test_non_executable_discovered_path_is_rejected(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A pg_ctl in pg_config's bindir that cannot be run is not handed back either."""
    pg_ctl = make_pg_ctl(tmp_path / "pgconfig-bin", mode=0o644)
    with patch.object(process, "_pg_bindir", return_value=str(pg_ctl.parent)):
        with pytest.raises(ExecutableMissingException) as exc_info:
            process._pg_exe(None, config)
    # the rejected candidate is still worth reporting as somewhere we looked
    assert str(pg_ctl) in str(exc_info.value)


def test_client_only_install_raises_early(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A pg_config without a matching server fails at discovery time, not at version check.

    This is the libpq-dev-without-postgresql-server case from issue #1031, where the
    plugin used to hand back a path that does not exist and only blew up much later.
    """
    client_bindir = tmp_path / "client-bin"
    client_bindir.mkdir()
    with patch.object(process, "_pg_bindir", return_value=str(client_bindir)):
        with pytest.raises(ExecutableMissingException) as exc_info:
            process._pg_exe(None, config)
    message = str(exc_info.value)
    assert str(client_bindir) in message
    assert "libpq-dev" in message
    assert "postgresql_noproc" in message
    assert "--postgresql-exec" in message


@pytest.mark.parametrize(
    "error",
    (
        FileNotFoundError("pg_config"),
        PermissionError("pg_config"),
        subprocess.CalledProcessError(1, "pg_config"),
        subprocess.TimeoutExpired("pg_config", process.PG_CONFIG_TIMEOUT),
    ),
)
def test_broken_pg_config_surfaces_as_executable_missing(config: PostgreSQLConfig, error: Exception) -> None:
    """A missing, unusable, hanging or failing pg_config is reported as a missing executable.

    Only FileNotFoundError used to be handled, so a non-executable pg_config escaped as
    a bare PermissionError - the second symptom reported in issue #1031.
    """
    with patch(f"{PROCESS}.subprocess.check_output", side_effect=error):
        with pytest.raises(ExecutableMissingException) as exc_info:
            process._pg_exe(None, config)
    message = str(exc_info.value)
    assert "Could not find pg_ctl" in message
    # the configured path is still reported as somewhere we looked
    assert config.exec in message


def test_pg_bindir_returns_stripped_output() -> None:
    """The binaries directory is read from pg_config without surrounding whitespace."""
    with patch(f"{PROCESS}.subprocess.check_output", return_value="/usr/lib/postgresql/17/bin\n"):
        assert process._pg_bindir([]) == "/usr/lib/postgresql/17/bin"
