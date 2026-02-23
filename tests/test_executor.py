"""Test various executor behaviours."""

import platform
from typing import Any
from unittest.mock import patch

import psycopg
import pytest
from packaging.version import parse
from port_for import get_port
from psycopg import Connection
from pytest import FixtureRequest

import pytest_postgresql.factories.process as process
from pytest_postgresql.config import get_config
from pytest_postgresql.exceptions import PostgreSQLUnsupported
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.factories import postgresql, postgresql_proc
from pytest_postgresql.retry import retry


def assert_executor_start_stop(executor: PostgreSQLExecutor) -> None:
    """Check that the executor is working."""
    with executor:
        assert executor.running()
        psycopg.connect(
            dbname=executor.user,
            user=executor.user,
            password=executor.password,
            host=executor.host,
            port=executor.port,
        )
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(
                dbname=executor.user,
                user=executor.user,
                password="bogus",
                host=executor.host,
                port=executor.port,
            )
    assert not executor.running()


class PatchedPostgreSQLExecutor(PostgreSQLExecutor):
    """PostgreSQLExecutor that always says it's 8.9 version."""

    @property
    def version(self) -> Any:
        """Overwrite version, to always return highest unsupported version."""
        return parse("8.9")


def test_unsupported_version(request: FixtureRequest) -> None:
    """Check that the error gets raised on unsupported postgres version."""
    config = get_config(request)
    port = get_port(config.port)
    assert port is not None
    executor = PatchedPostgreSQLExecutor(
        executable=config.exec,
        host=config.host,
        port=port,
        datadir="/tmp/error",
        unixsocketdir=config.unixsocketdir,
        logfile="/tmp/version.error.log",
        startparams=config.startparams,
        dbname="random_name",
    )

    with pytest.raises(PostgreSQLUnsupported):
        executor.start()


@pytest.mark.xdist_group(name="executor_no_xdist_guard")
@pytest.mark.parametrize("locale", ("en_US.UTF-8", "de_DE.UTF-8", "nl_NO.UTF-8"))
def test_executor_init_with_password(
    request: FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    locale: str,
) -> None:
    """Test whether the executor initializes properly."""
    config = get_config(request)
    monkeypatch.setenv("LC_ALL", locale)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}")
    datadir, logfile_path = process._prepare_dir(tmpdir, port)
    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=config.unixsocketdir,
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="somepassword",
        dbname="somedatabase",
    )
    assert_executor_start_stop(executor)


def test_executor_init_bad_tmp_path(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    r"""Test init with \ and space chars in the path."""
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}") / r"a bad\path/"
    tmpdir.mkdir(exist_ok=True)
    datadir, logfile_path = process._prepare_dir(tmpdir, port)
    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=config.unixsocketdir,
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="some password",
        dbname="some database",
    )

    # Verify the correct template was selected based on platform
    current_platform = platform.system()
    if current_platform == "Windows":
        # Windows template should not have unix_socket_directories
        assert "unix_socket_directories" not in executor.command
        assert "log_destination=stderr" in executor.command
    else:
        # Unix/Darwin template should have unix_socket_directories with single quotes
        assert "unix_socket_directories='" in executor.command
        assert "log_destination='stderr'" in executor.command

    assert_executor_start_stop(executor)


@pytest.mark.parametrize(
    "platform_name",
    ["Windows", "Linux", "Darwin"],
)
def test_executor_platform_template_selection(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
    platform_name: str,
) -> None:
    """Test that correct template is selected for each platform.

    This parametrized test verifies that the executor selects the appropriate
    command template based on the platform.
    """
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}")
    datadir, logfile_path = process._prepare_dir(tmpdir, port)

    with patch("pytest_postgresql.executor.platform.system", return_value=platform_name):
        executor = PostgreSQLExecutor(
            executable=pg_exe,
            host=config.host,
            port=port,
            datadir=str(datadir),
            unixsocketdir=config.unixsocketdir,
            logfile=str(logfile_path),
            startparams=config.startparams,
            dbname="test",
        )

        # Verify correct template was selected
        if platform_name == "Windows":
            # Windows template
            assert "unix_socket_directories" not in executor.command
            assert "log_destination=stderr" in executor.command
        else:
            # Unix/Darwin template
            assert "unix_socket_directories='" in executor.command
            assert "log_destination='stderr'" in executor.command


def test_executor_with_special_chars_in_all_paths(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test executor with special characters in multiple paths simultaneously.

    This integration test verifies that the executor can handle special
    characters (spaces, Unicode) in datadir, logfile, unixsocketdir, and
    postgres_options all at the same time.
    """
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    # Create a tmpdir with spaces in the name
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}") / "my test dir"
    tmpdir.mkdir(exist_ok=True)
    datadir, logfile_path = process._prepare_dir(tmpdir, port)
    
    # Create the socket directory for Unix systems.
    # Use basetemp to keep the path short: Unix domain sockets have a 108-char
    # OS-level path limit, and the nested test temp path easily exceeds it.
    socket_dir = tmp_path_factory.getbasetemp() / "sock dir"
    socket_dir.mkdir(exist_ok=True)

    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=str(socket_dir),
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="test pass",
        dbname="test db",
        postgres_options="-N 50",
    )

    # Verify the command contains properly quoted paths
    command = executor.command
    assert str(datadir) in command or f'"{datadir}"' in command
    assert str(logfile_path) in command or f'"{logfile_path}"' in command

    # Verify correct template was selected based on actual platform
    current_platform = platform.system()
    if current_platform == "Windows":
        assert "unix_socket_directories" not in executor.command
    else:
        assert "unix_socket_directories='" in executor.command

    # Start and stop the executor to verify it works
    assert_executor_start_stop(executor)


postgres_with_password = postgresql_proc(password="hunter2")


def test_proc_with_password(
    postgres_with_password: PostgreSQLExecutor,
) -> None:
    """Check that password option to postgresql_proc factory is honored."""
    assert postgres_with_password.running() is True

    # no assertion necessary here; we just want to make sure it connects with
    # the password
    retry(
        lambda: psycopg.connect(
            dbname=postgres_with_password.user,
            user=postgres_with_password.user,
            password=postgres_with_password.password,
            host=postgres_with_password.host,
            port=postgres_with_password.port,
        ),
        possible_exception=psycopg.OperationalError,
    )

    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(
            dbname=postgres_with_password.user,
            user=postgres_with_password.user,
            password="bogus",
            host=postgres_with_password.host,
            port=postgres_with_password.port,
        )


postgresql_max_conns_proc = postgresql_proc(postgres_options="-N 42")
postgres_max_conns = postgresql("postgresql_max_conns_proc")


def test_postgres_options(postgres_max_conns: Connection) -> None:
    """Check that max connections (-N 42) is honored."""
    cur = postgres_max_conns.cursor()
    cur.execute("SHOW max_connections")
    assert cur.fetchone() == ("42",)


postgres_isolation_level = postgresql("postgresql_proc", isolation_level=psycopg.IsolationLevel.SERIALIZABLE)


def test_custom_isolation_level(postgres_isolation_level: Connection) -> None:
    """Check that a client fixture with a custom isolation level works."""
    cur = postgres_isolation_level.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)


@pytest.mark.skipif(platform.system() != "Windows", reason="Windows-specific test")
def test_actual_postgresql_start_windows(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test that PostgreSQL actually starts on Windows with the new template.

    This integration test verifies that the Windows-specific command template
    correctly starts PostgreSQL on actual Windows systems.
    """
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}")
    datadir, logfile_path = process._prepare_dir(tmpdir, port)

    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=config.unixsocketdir,
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="testpass",
        dbname="test",
    )

    # Verify Windows template is used
    assert "unix_socket_directories" not in executor.command
    assert "log_destination=stderr" in executor.command

    # Start and stop PostgreSQL to verify it works
    assert_executor_start_stop(executor)


@pytest.mark.skipif(
    platform.system() not in ("Linux", "FreeBSD"),
    reason="Unix/Linux-specific test",
)
def test_actual_postgresql_start_unix(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test that PostgreSQL actually starts on Unix/Linux with the new template.

    This integration test verifies that the Unix-specific command template
    correctly starts PostgreSQL on actual Unix/Linux systems.
    """
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}")
    datadir, logfile_path = process._prepare_dir(tmpdir, port)

    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=config.unixsocketdir,
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="testpass",
        dbname="test",
    )

    # Verify Unix template is used
    assert "unix_socket_directories=" in executor.command
    assert "log_destination='stderr'" in executor.command

    # Start and stop PostgreSQL to verify it works
    assert_executor_start_stop(executor)


@pytest.mark.skipif(platform.system() != "Darwin", reason="Darwin/macOS-specific test")
def test_actual_postgresql_start_darwin(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test that PostgreSQL actually starts on Darwin/macOS with the new template.

    This integration test verifies that the Unix template correctly starts
    PostgreSQL on actual Darwin/macOS systems and uses the correct locale.
    """
    config = get_config(request)
    pg_exe = process._pg_exe(None, config)
    port = process._pg_port(-1, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}")
    datadir, logfile_path = process._prepare_dir(tmpdir, port)

    executor = PostgreSQLExecutor(
        executable=pg_exe,
        host=config.host,
        port=port,
        datadir=str(datadir),
        unixsocketdir=config.unixsocketdir,
        logfile=str(logfile_path),
        startparams=config.startparams,
        password="testpass",
        dbname="test",
    )

    # Verify Unix template is used
    assert "unix_socket_directories=" in executor.command
    assert "log_destination='stderr'" in executor.command

    # Verify Darwin-specific locale is set
    assert executor.envvars["LC_ALL"] == "en_US.UTF-8"
    assert executor.envvars["LC_CTYPE"] == "en_US.UTF-8"
    assert executor.envvars["LANG"] == "en_US.UTF-8"

    # Start and stop PostgreSQL to verify it works
    assert_executor_start_stop(executor)
