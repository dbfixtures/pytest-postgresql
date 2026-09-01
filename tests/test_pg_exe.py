"""Tests for the pg_ctl discovery performed by the process fixture factory."""

import platform
import subprocess
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pytest_postgresql.config import PostgreSQLConfig
from pytest_postgresql.exceptions import ExecutableMissingException
from pytest_postgresql.factories import _pg
from pytest_postgresql.plugin import _DEFAULT_POSTGRESQL_EXEC

_PG_PROCESS = "pytest_postgresql.factories._pg"


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


@pytest.fixture
def isolated_discovery() -> Generator[None, None, None]:
    """Disable PATH and Windows Program Files probes so tests isolate earlier strategies."""
    with (
        patch(f"{_PG_PROCESS}.platform.system", return_value="Linux"),
        patch.object(_pg, "_which_pg_ctl", return_value=None),
    ):
        yield


def make_pg_ctl(bindir: Path, name: str = "pg_ctl", mode: int = 0o755) -> Path:
    """Create a stand-in for pg_ctl in bindir, executable unless told otherwise."""
    bindir.mkdir(parents=True, exist_ok=True)
    pg_ctl = bindir / name
    pg_ctl.write_text("")
    pg_ctl.chmod(mode)
    return pg_ctl


def test_postgresql_exec_default_is_platform_aware() -> None:
    """Unix keeps the distro path; Windows does not advertise a Linux-only default."""
    if platform.system() == "Windows":
        assert _DEFAULT_POSTGRESQL_EXEC == ""
    else:
        assert _DEFAULT_POSTGRESQL_EXEC == "/usr/lib/postgresql/14/bin/pg_ctl"


def test_explicit_executable_is_not_second_guessed(config: PostgreSQLConfig) -> None:
    """An executable passed to the factory is returned even if it does not exist."""
    assert _pg._pg_exe("/nowhere/pg_ctl", config) == "/nowhere/pg_ctl"


def test_existing_configured_executable_wins(tmp_path: Path) -> None:
    """A configured executable that exists is used without consulting pg_config."""
    pg_ctl = make_pg_ctl(tmp_path / "bin")
    config = make_config(str(pg_ctl))
    with patch.object(_pg, "_pg_bindir") as bindir_mock:
        assert _pg._pg_exe(None, config) == str(pg_ctl)
    bindir_mock.assert_not_called()


@pytest.mark.usefixtures("isolated_discovery")
def test_empty_configured_executable_is_skipped() -> None:
    """The Windows default of an empty postgresql_exec is not listed as a checked path."""
    config = make_config("")
    with patch.object(_pg, "_pg_bindir", side_effect=FileNotFoundError("pg_config")):
        with pytest.raises(ExecutableMissingException) as exc_info:
            _pg._pg_exe(None, config)
    message = str(exc_info.value)
    assert "  - \n" not in message
    assert "Could not find pg_ctl" in message


def test_pg_config_bindir_is_used(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """pg_ctl found next to the binaries pg_config points at is used."""
    pg_ctl = make_pg_ctl(tmp_path / "pgconfig-bin")
    with patch.object(_pg, "_pg_bindir", return_value=str(pg_ctl.parent)):
        assert _pg._pg_exe(None, config) == str(pg_ctl)


def test_windows_executable_suffix_is_probed(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """On Windows the binary is found as pg_ctl.exe.

    Probing the filesystem does no PATHEXT resolution, so looking for a bare "pg_ctl"
    would miss every Windows install and turn discovery into a hard failure there.
    """
    pg_ctl = make_pg_ctl(tmp_path / "windows-bin", name="pg_ctl.exe")
    with (
        patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")),
        patch.object(_pg, "_pg_bindir", return_value=str(pg_ctl.parent)),
    ):
        assert _pg._pg_exe(None, config) == str(pg_ctl)


def test_windows_program_files_newest_version_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The Windows installer layout is scanned newest version directory first."""
    program_files = tmp_path / "Program Files"
    older = make_pg_ctl(program_files / "PostgreSQL" / "14" / "bin", name="pg_ctl.exe")
    newest = make_pg_ctl(program_files / "PostgreSQL" / "16" / "bin", name="pg_ctl.exe")
    make_pg_ctl(program_files / "PostgreSQL" / "15" / "bin", name="pg_ctl.exe")
    monkeypatch.setenv("ProgramFiles", str(program_files))
    with patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")):
        found = _pg._windows_program_files_pg_ctls()
    assert found[0] == str(newest)
    assert str(older) in found
    assert found == sorted(
        found,
        key=lambda path: _pg._windows_version_sort_key(Path(path).parents[1].name),
        reverse=True,
    )


def test_windows_program_files_missing_root_is_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing Program Files PostgreSQL directory yields no candidates."""
    monkeypatch.setenv("ProgramFiles", str(tmp_path / "no-such-dir"))
    assert _pg._windows_program_files_pg_ctls() == []


def test_windows_program_files_empty_tree_is_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty PostgreSQL directory under Program Files yields no candidates."""
    (tmp_path / "PostgreSQL").mkdir()
    monkeypatch.setenv("ProgramFiles", str(tmp_path))
    with patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")):
        assert _pg._windows_program_files_pg_ctls() == []


def test_windows_numeric_version_outranks_stray_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-numeric Program Files folders sort after real version directories."""
    program_files = tmp_path / "Program Files"
    stray = make_pg_ctl(program_files / "PostgreSQL" / "extra" / "bin", name="pg_ctl.exe")
    numeric = make_pg_ctl(program_files / "PostgreSQL" / "16" / "bin", name="pg_ctl.exe")
    monkeypatch.setenv("ProgramFiles", str(program_files))
    with patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")):
        found = _pg._windows_program_files_pg_ctls()
    assert found[0] == str(numeric)
    assert str(stray) in found


def test_windows_program_files_scan_oserror_returns_empty() -> None:
    """A Program Files tree that cannot be listed is treated as empty, not a crash."""
    fake_root = MagicMock()
    fake_root.is_dir.return_value = True
    fake_root.iterdir.side_effect = OSError("permission denied")
    with patch.object(_pg, "_windows_postgresql_root", return_value=fake_root):
        assert _pg._windows_program_files_pg_ctls() == []


def test_windows_postgresql_root_uses_fallback_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """When ProgramFiles is unset, the conventional Program Files location is used."""
    monkeypatch.delenv("ProgramFiles", raising=False)
    assert _pg._windows_postgresql_root() == Path(r"C:\Program Files") / "PostgreSQL"


def test_discovery_failed_without_causes_still_explains() -> None:
    """A discovery failure with no strategy-specific cause still tells the reader what happened."""
    exc = ExecutableMissingException.discovery_failed([], [])
    assert "No pg_ctl was found in any of the usual locations." in str(exc)


def test_pg_exe_uses_windows_program_files(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """When pg_config fails, a Program Files install is used on Windows."""
    pg_ctl = make_pg_ctl(tmp_path / "bin", name="pg_ctl.exe")
    with (
        patch(f"{_PG_PROCESS}.platform.system", return_value="Windows"),
        patch.object(_pg, "_pg_bindir", side_effect=FileNotFoundError("pg_config")),
        patch.object(_pg, "_windows_program_files_pg_ctls", return_value=[str(pg_ctl)]),
        patch.object(_pg, "_which_pg_ctl", return_value=None),
    ):
        assert _pg._pg_exe(None, config) == str(pg_ctl)


def test_pg_exe_uses_which_when_other_probes_miss(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """pg_ctl on PATH is used after pg_config and Program Files come up empty."""
    pg_ctl = make_pg_ctl(tmp_path / "path-bin")
    with (
        patch(f"{_PG_PROCESS}.platform.system", return_value="Linux"),
        patch.object(_pg, "_pg_bindir", side_effect=FileNotFoundError("pg_config")),
        patch.object(_pg, "_which_pg_ctl", return_value=str(pg_ctl)),
    ):
        assert _pg._pg_exe(None, config) == str(pg_ctl)


def test_which_pg_ctl_returns_first_executable_on_path(tmp_path: Path) -> None:
    """_which_pg_ctl asks shutil.which for each PG_CTL_NAMES entry."""
    pg_ctl = make_pg_ctl(tmp_path / "path-bin")
    with (
        patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl.exe", "pg_ctl")),
        patch(f"{_PG_PROCESS}.shutil.which", side_effect=[None, str(pg_ctl)]),
    ):
        assert _pg._which_pg_ctl() == str(pg_ctl)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
def test_which_pg_ctl_skips_non_executable(tmp_path: Path) -> None:
    """A PATH hit that cannot be executed is not returned."""
    pg_ctl = make_pg_ctl(tmp_path / "path-bin", mode=0o644)
    with (
        patch.object(_pg, "PG_CTL_NAMES", ("pg_ctl",)),
        patch(f"{_PG_PROCESS}.shutil.which", return_value=str(pg_ctl)),
    ):
        assert _pg._which_pg_ctl() is None


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
def test_pg_exe_rejects_non_executable_program_files_hit(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A Program Files pg_ctl that exists but cannot be run is skipped, then discovery fails."""
    pg_ctl = make_pg_ctl(tmp_path / "bin", mode=0o644)
    with (
        patch(f"{_PG_PROCESS}.platform.system", return_value="Windows"),
        patch.object(_pg, "_pg_bindir", side_effect=FileNotFoundError("pg_config")),
        patch.object(_pg, "_windows_program_files_pg_ctls", return_value=[str(pg_ctl)]),
        patch.object(_pg, "_which_pg_ctl", return_value=None),
    ):
        with pytest.raises(ExecutableMissingException, match="Could not find pg_ctl"):
            _pg._pg_exe(None, config)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
def test_pg_exe_rejects_non_executable_which_hit(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A PATH pg_ctl that cannot be run is not handed back either."""
    pg_ctl = make_pg_ctl(tmp_path / "path-bin", mode=0o644)
    with (
        patch(f"{_PG_PROCESS}.platform.system", return_value="Linux"),
        patch.object(_pg, "_pg_bindir", side_effect=FileNotFoundError("pg_config")),
        patch.object(_pg, "_which_pg_ctl", return_value=str(pg_ctl)),
    ):
        with pytest.raises(ExecutableMissingException, match="Could not find pg_ctl"):
            _pg._pg_exe(None, config)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
@pytest.mark.usefixtures("isolated_discovery")
def test_non_executable_configured_path_is_rejected(tmp_path: Path) -> None:
    """A configured pg_ctl that exists but cannot be run is not handed back.

    It used to be accepted, deferring the failure to the version check.
    """
    pg_ctl = make_pg_ctl(tmp_path / "bin", mode=0o644)
    config = make_config(str(pg_ctl))
    with patch(f"{_PG_PROCESS}.subprocess.check_output", side_effect=FileNotFoundError("pg_config")):
        with pytest.raises(ExecutableMissingException, match="Could not find pg_ctl"):
            _pg._pg_exe(None, config)


@pytest.mark.skipif(platform.system() == "Windows", reason="os.access ignores X_OK on Windows")
@pytest.mark.usefixtures("isolated_discovery")
def test_non_executable_discovered_path_is_rejected(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A pg_ctl in pg_config's bindir that cannot be run is not handed back either."""
    pg_ctl = make_pg_ctl(tmp_path / "pgconfig-bin", mode=0o644)
    with patch.object(_pg, "_pg_bindir", return_value=str(pg_ctl.parent)):
        with pytest.raises(ExecutableMissingException) as exc_info:
            _pg._pg_exe(None, config)
    # the rejected candidate is still worth reporting as somewhere we looked
    assert str(pg_ctl) in str(exc_info.value)


@pytest.mark.usefixtures("isolated_discovery")
def test_client_only_install_raises_early(tmp_path: Path, config: PostgreSQLConfig) -> None:
    """A pg_config without a matching server fails at discovery time, not at version check.

    This is the libpq-dev-without-postgresql-server case from issue #1031, where the
    plugin used to hand back a path that does not exist and only blew up much later.
    """
    client_bindir = tmp_path / "client-bin"
    client_bindir.mkdir()
    with patch.object(_pg, "_pg_bindir", return_value=str(client_bindir)):
        with pytest.raises(ExecutableMissingException) as exc_info:
            _pg._pg_exe(None, config)
    message = str(exc_info.value)
    assert str(client_bindir) in message
    assert "libpq-dev" in message
    assert "postgresql_noproc" in message
    assert "--postgresql-exec" in message


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError("pg_config"),
        PermissionError("pg_config"),
        subprocess.CalledProcessError(1, "pg_config"),
        subprocess.TimeoutExpired("pg_config", _pg.PG_CONFIG_TIMEOUT),
    ],
)
@pytest.mark.usefixtures("isolated_discovery")
def test_broken_pg_config_surfaces_as_executable_missing(config: PostgreSQLConfig, error: Exception) -> None:
    """A missing, unusable, hanging or failing pg_config is reported as a missing executable.

    Only FileNotFoundError used to be handled, so a non-executable pg_config escaped as
    a bare PermissionError - the second symptom reported in issue #1031.
    """
    with patch(f"{_PG_PROCESS}.subprocess.check_output", side_effect=error):
        with pytest.raises(ExecutableMissingException) as exc_info:
            _pg._pg_exe(None, config)
    message = str(exc_info.value)
    assert "Could not find pg_ctl" in message
    # the configured path is still reported as somewhere we looked
    assert config.exec in message
    # how pg_config failed is what separates "not installed" from "not executable"
    assert type(error).__name__ in message


def test_pg_bindir_returns_stripped_output() -> None:
    """The binaries directory is read from pg_config without surrounding whitespace."""
    with patch(f"{_PG_PROCESS}.subprocess.check_output", return_value="/usr/lib/postgresql/17/bin\n"):
        assert _pg._pg_bindir() == "/usr/lib/postgresql/17/bin"
