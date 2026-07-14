"""Test various executor behaviours."""

import platform
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from packaging.version import parse
from port_for import PortForException, get_port
from psycopg import Connection
from pytest import FixtureRequest

import pytest_postgresql.factories.process as process
from pytest_postgresql.config import get_config
from pytest_postgresql.exceptions import PostgreSQLUnsupported
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.factories import postgresql, postgresql_async, postgresql_proc
from pytest_postgresql.retry import retry


def assert_executor_start_stop(executor: PostgreSQLExecutor) -> None:
    """Check that the executor is working."""
    with executor:
        assert executor.running()
        # Retry the connection: under parallel xdist runs the TCP port
        # becomes accessible before PostgreSQL finishes database recovery,
        # so an immediate connect may raise OperationalError with
        # "the database system is starting up".
        conn = retry(
            lambda: psycopg.connect(
                dbname=executor.user,
                user=executor.user,
                password=executor.password,
                host=executor.host,
                port=executor.port,
            ),
            possible_exception=psycopg.OperationalError,
        )
        conn.close()
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
    port = process._pg_port(None, config, [])
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
    port = process._pg_port(None, config, [])
    tmpdir = tmp_path_factory.mktemp(f"pytest-postgresql-{request.node.name}") / r"a bad\path/"
    tmpdir.mkdir(parents=True, exist_ok=True)
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
    ["Windows", "Linux", "Darwin", "FreeBSD"],
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
    port = process._pg_port(None, config, [])
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
    assert f'"{datadir}"' in command
    assert f'"{logfile_path}"' in command

    # Verify correct template was selected based on actual platform
    current_platform = platform.system()
    if current_platform == "Windows":
        assert "unix_socket_directories" not in executor.command
    else:
        assert f"unix_socket_directories='{socket_dir}'" in executor.command

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
    cur.execute("SHOW transaction_isolation")
    assert cur.fetchone() == ("serializable",)


postgres_async_isolation_level = postgresql_async(
    "postgresql_proc",
    isolation_level=psycopg.IsolationLevel.SERIALIZABLE,
)


@pytest.mark.asyncio
async def test_custom_async_isolation_level(postgres_async_isolation_level: psycopg.AsyncConnection) -> None:
    """Check that an async client fixture with a custom isolation level works."""
    async with postgres_async_isolation_level.cursor() as cur:
        await cur.execute("SHOW transaction_isolation")
        assert await cur.fetchone() == ("serializable",)


def test_postgresql_proc_removes_port_lock_on_teardown(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Port sentinel file is removed when the process fixture tears down."""
    fixture_func = postgresql_proc(port=None)
    raw_func = getattr(fixture_func, "__wrapped__", fixture_func)

    port_path = tmp_path_factory.getbasetemp()
    if hasattr(request.config, "workerinput"):
        port_path = tmp_path_factory.getbasetemp().parent
    pg_port = get_port(None)
    assert pg_port is not None

    executor_mock = MagicMock()
    executor_mock.start = MagicMock(return_value=executor_mock)
    executor_mock.stop = MagicMock(return_value=executor_mock)
    executor_mock.user = "postgres"
    executor_mock.host = "127.0.0.1"
    executor_mock.port = pg_port
    executor_mock.template_dbname = "template_tests"
    executor_mock.version = 14
    executor_mock.password = None
    executor_mock.wait_for_postgres = MagicMock()

    janitor_mock = MagicMock()
    janitor_mock.init = MagicMock()
    janitor_mock.drop = MagicMock()

    finalizers: list[Callable[[], None]] = []
    request.addfinalizer = finalizers.append  # type: ignore[method-assign]

    with (
        patch("pytest_postgresql.factories.process._pg_exe", return_value="/usr/bin/pg_ctl"),
        patch("pytest_postgresql.factories.process._pg_port", return_value=pg_port),
        patch("pytest_postgresql.factories.process.PostgreSQLExecutor", return_value=executor_mock),
        patch("pytest_postgresql.factories.process.DatabaseJanitor", return_value=janitor_mock),
        patch("pytest_postgresql.factories.process.get_config") as get_config_mock,
    ):
        config_mock = MagicMock()
        config_mock.dbname = "tests"
        config_mock.load = []
        config_mock.drop_test_database = False
        config_mock.port_search_count = 5
        get_config_mock.return_value = config_mock

        raw_func(request, tmp_path_factory)
        port_file = port_path / f"postgresql-{pg_port}.port"
        assert port_file.exists()
        for finalizer in finalizers:
            finalizer()

    assert not port_file.exists()


def test_postgresql_proc_removes_port_lock_on_setup_failure(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Port sentinel file is removed when fixture setup fails after claiming a port."""
    fixture_func = postgresql_proc(port=None)
    raw_func = getattr(fixture_func, "__wrapped__", fixture_func)

    port_path = tmp_path_factory.getbasetemp()
    if hasattr(request.config, "workerinput"):
        port_path = tmp_path_factory.getbasetemp().parent
    pg_port = get_port(None)
    assert pg_port is not None

    with (
        patch("pytest_postgresql.factories.process._pg_exe", return_value="/usr/bin/pg_ctl"),
        patch("pytest_postgresql.factories.process._pg_port", return_value=pg_port),
        patch("pytest_postgresql.factories.process.get_config") as get_config_mock,
        patch.object(tmp_path_factory, "mktemp", side_effect=OSError("setup failed")),
    ):
        config_mock = MagicMock()
        config_mock.dbname = "tests"
        config_mock.load = []
        config_mock.drop_test_database = False
        config_mock.port_search_count = 5
        get_config_mock.return_value = config_mock

        with pytest.raises(OSError, match="setup failed"):
            raw_func(request, tmp_path_factory)

    port_file = port_path / f"postgresql-{pg_port}.port"
    assert not port_file.exists()


def test_postgresql_proc_port_lock_safe_on_pg_port_failure(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Port lock cleanup must not raise UnboundLocalError when _pg_port fails early."""
    fixture_func = postgresql_proc(port=None)
    raw_func = getattr(fixture_func, "__wrapped__", fixture_func)

    port_path = tmp_path_factory.getbasetemp()
    if hasattr(request.config, "workerinput"):
        port_path = tmp_path_factory.getbasetemp().parent

    existing_ports = set(port_path.glob("postgresql-*.port"))

    with (
        patch("pytest_postgresql.factories.process._pg_exe", return_value="/usr/bin/pg_ctl"),
        patch(
            "pytest_postgresql.factories.process._pg_port",
            side_effect=PortForException("no free ports"),
        ),
        patch("pytest_postgresql.factories.process.get_config") as get_config_mock,
    ):
        config_mock = MagicMock()
        config_mock.dbname = "tests"
        config_mock.load = []
        config_mock.drop_test_database = False
        config_mock.port_search_count = 5
        get_config_mock.return_value = config_mock

        with pytest.raises(PortForException, match="no free ports"):
            raw_func(request, tmp_path_factory)

    assert set(port_path.glob("postgresql-*.port")) == existing_ports


def test_postgresql_proc_preserves_foreign_port_lock_on_exhausted_retries(
    request: FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Exhausted port retries must not delete another worker's port lock file."""
    fixture_func = postgresql_proc(port=None)
    raw_func = getattr(fixture_func, "__wrapped__", fixture_func)

    port_path = tmp_path_factory.getbasetemp()
    if hasattr(request.config, "workerinput"):
        port_path = tmp_path_factory.getbasetemp().parent

    excluded_ports: set[int] = set()
    pg_ports: list[int] = []
    for _ in range(3):
        pg_port = get_port(None, excluded_ports)
        assert pg_port is not None
        excluded_ports.add(pg_port)
        pg_ports.append(pg_port)

    foreign_locks = []
    for pg_port in pg_ports:
        foreign_lock = port_path / f"postgresql-{pg_port}.port"
        foreign_lock.write_text(f"pg_port {pg_port}\n", encoding="utf-8")
        foreign_locks.append(foreign_lock)

    with (
        patch("pytest_postgresql.factories.process._pg_exe", return_value="/usr/bin/pg_ctl"),
        patch("pytest_postgresql.factories.process._pg_port", side_effect=pg_ports),
        patch("pytest_postgresql.factories.process.get_config") as get_config_mock,
    ):
        config_mock = MagicMock()
        config_mock.dbname = "tests"
        config_mock.load = []
        config_mock.drop_test_database = False
        config_mock.port_search_count = 2
        get_config_mock.return_value = config_mock

        with pytest.raises(PortForException, match="Attempted"):
            raw_func(request, tmp_path_factory)

    for pg_port, foreign_lock in zip(pg_ports, foreign_locks, strict=True):
        assert foreign_lock.exists()
        assert foreign_lock.read_text(encoding="utf-8") == f"pg_port {pg_port}\n"


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
    port = process._pg_port(None, config, [])
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
    port = process._pg_port(None, config, [])
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
    assert "unix_socket_directories='" in executor.command
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
    port = process._pg_port(None, config, [])
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
    assert "unix_socket_directories='" in executor.command
    assert "log_destination='stderr'" in executor.command

    # Verify Darwin-specific locale is set
    assert executor.envvars["LC_ALL"] == "en_US.UTF-8"
    assert executor.envvars["LC_CTYPE"] == "en_US.UTF-8"
    assert executor.envvars["LANG"] == "en_US.UTF-8"

    # Start and stop PostgreSQL to verify it works
    assert_executor_start_stop(executor)


@pytest.mark.parametrize("platform_name", ["Linux", "Windows", "Darwin"])
def test_prepare_dir_does_not_create_datadir_on_non_freebsd(tmp_path: Path, platform_name: str) -> None:
    """Non-FreeBSD platforms defer data directory creation to initdb."""
    with patch("pytest_postgresql.factories.process.platform.system", return_value=platform_name):
        datadir, logfile_path = process._prepare_dir(tmp_path, 5432)

    assert datadir == tmp_path / "data-5432"
    assert logfile_path == tmp_path / "postgresql.5432.log"
    assert not datadir.exists()


def test_prepare_dir_creates_datadir_on_freebsd(tmp_path: Path) -> None:
    """FreeBSD needs pg_hba.conf appended before initdb runs."""
    with patch("pytest_postgresql.factories.process.platform.system", return_value="FreeBSD"):
        datadir, _ = process._prepare_dir(tmp_path, 5432)

    assert datadir.exists()
    assert (datadir / "pg_hba.conf").read_text(encoding="utf-8").endswith("host all all 0.0.0.0/0 trust\n")
