"""Database cloning strategy configuration and real-server contracts."""

import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from packaging.version import Version
from psycopg import AsyncConnection, Connection

from pytest_postgresql.executors import NoopExecutor, PostgreSQLExecutor
from pytest_postgresql.factories import postgresql, postgresql_async, postgresql_noproc, postgresql_proc
from pytest_postgresql.janitor import AsyncDatabaseJanitor, DatabaseJanitor
from tests.loader import load_database


@pytest.mark.parametrize(
    ("ini_strategy", "cli_strategy", "expected"),
    [
        (None, None, None),
        ("clone", None, "clone"),
        (None, "file_copy", "file_copy"),
        ("clone", "wal_log", "wal_log"),
    ],
)
def test_create_strategy_configuration(
    pytester: pytest.Pytester, ini_strategy: str | None, cli_strategy: str | None, expected: str | None
) -> None:
    """The CLI overrides ini configuration, and the unset default is None."""
    if ini_strategy:
        pytester.makeini(f"[pytest]\npostgresql_create_strategy = {ini_strategy}\n")
    pytester.makepyfile(
        "from pytest_postgresql.config import get_config\n"
        "def test_strategy(request):\n"
        f"    assert get_config(request).create_strategy == {expected!r}\n"
    )
    args = [f"--postgresql-create-strategy={cli_strategy}"] if cli_strategy else []
    pytester.runpytest(*args).assert_outcomes(passed=1)


@pytest.mark.parametrize("source", ["cli", "ini"])
def test_invalid_create_strategy_configuration(pytester: pytest.Pytester, source: str) -> None:
    """Invalid CLI and ini values produce a clear configuration error."""
    pytester.makepyfile(
        "from pytest_postgresql.config import get_config\ndef test_strategy(request):\n    get_config(request)\n"
    )
    if source == "cli":
        result = pytester.runpytest("--postgresql-create-strategy=invalid")
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines(["*invalid choice: 'invalid'*"])
    else:
        pytester.makeini("[pytest]\npostgresql_create_strategy = invalid\n")
        result = pytester.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*UsageError: Invalid PostgreSQL create_strategy 'invalid'*"])


@pytest.mark.parametrize("factory", [postgresql_proc, postgresql_noproc])
def test_invalid_factory_create_strategy(factory: Callable[..., object]) -> None:
    """Factory arguments reject misspelled strategies immediately."""
    with pytest.raises(ValueError, match="Invalid PostgreSQL create_strategy 'invalid'"):
        factory(create_strategy="invalid")


@pytest.mark.parametrize("factory_name", ["postgresql_proc", "postgresql_noproc"])
@pytest.mark.parametrize("factory_strategy", [None, "clone"])
def test_factory_create_strategy_precedence(
    postgresql_proc: PostgreSQLExecutor,
    pytester: pytest.Pytester,
    factory_name: str,
    factory_strategy: str | None,
) -> None:
    """Both factories propagate the chosen setting to clients, with arguments taking precedence."""
    pytester.makeini("[pytest]\npostgresql_create_strategy = file_copy\n")
    kwargs: dict[str, object] = {"create_strategy": factory_strategy}
    if factory_name == "postgresql_noproc":
        kwargs.update(
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            user=postgresql_proc.user,
            dbname="strategy_noproc",
        )
    expected = factory_strategy or "wal_log"
    pytester.makeconftest(
        f"from pytest_postgresql import factories\nconfigured_proc = factories.{factory_name}(**{kwargs!r})\n"
    )
    pytester.makepyfile(
        f"def test_strategy(configured_proc):\n    assert configured_proc.create_strategy == {expected!r}\n"
    )
    pytester.runpytest("--postgresql-create-strategy=wal_log").assert_outcomes(passed=1)


strategy_server = postgresql_proc(
    load=[load_database],
    create_strategy="wal_log",
    postgres_options="-c log_statement=all -c log_line_prefix=PID:%p:",
)


@pytest.fixture(scope="session")
def strategy_template(strategy_server: PostgreSQLExecutor) -> Iterator[DatabaseJanitor]:
    """Seed a quoted template without applying a client-only creation strategy."""
    with DatabaseJanitor(
        user=strategy_server.user,
        host=strategy_server.host,
        port=strategy_server.port,
        password=strategy_server.password,
        dbname='strategy"template',
        as_template=True,
        create_strategy="clone",
    ) as template:
        with template.cursor(template.dbname) as cursor:
            cursor.execute("CREATE TABLE strategy_data (id integer PRIMARY KEY, value text)")
            cursor.execute("INSERT INTO strategy_data VALUES (1, 'template')")
        yield template


def _server_statements(process: PostgreSQLExecutor, offset: int = 0) -> list[tuple[str, str]]:
    """Read PostgreSQL's statement records with their actual backend PIDs."""
    records = []
    for line in Path(process.logfile).read_text(encoding="utf-8")[offset:].splitlines():
        match = re.match(r"PID:(\d+):.*?(?:statement|execute [^:]+): (.*)", line)
        if match:
            records.append((match[1], match[2]))
    return records


def _assert_creation(
    process: PostgreSQLExecutor, dbname: str, expected: str, strategy: str | None = None, offset: int = 0
) -> None:
    """Check the server-observed SQL and session-local setting for one database."""
    records = _server_statements(process, offset)
    quoted_name = dbname.replace('"', '""')
    creates = [(pid, query) for pid, query in records if query.startswith(f'CREATE DATABASE "{quoted_name}"')]
    assert len(creates) == 1
    create_pid, query = creates[0]
    assert query == expected
    settings = [query for pid, query in records if pid == create_pid and query.startswith("SET file_copy_method")]
    if strategy in {"file_copy", "clone"}:
        setting = "SET file_copy_method = copy" if strategy == "file_copy" else "SET file_copy_method = clone"
        assert settings == [setting]
        assert records.index((create_pid, setting)) < records.index((create_pid, expected))
    else:
        assert settings == []


def _unsupported_strategy_error(
    template: DatabaseJanitor, process: PostgreSQLExecutor, strategy: str | None
) -> type[psycopg.Error] | None:
    """Select the PostgreSQL error expected when a feature is unavailable."""
    if strategy == "wal_log" and process.version < Version("15"):
        return psycopg.errors.SyntaxError
    if strategy in {"file_copy", "clone"} and process.version < Version("18"):
        return psycopg.errors.UndefinedObject
    if strategy == "clone":
        with template.cursor() as cursor:
            cursor.execute("SELECT enumvals FROM pg_settings WHERE name = 'file_copy_method'")
            setting = cursor.fetchone()
        assert setting is not None
        if "clone" not in setting[0]:
            return psycopg.errors.InvalidParameterValue
    return None


@pytest.mark.parametrize("strategy", [None, "wal_log", "file_copy", "clone"])
@pytest.mark.parametrize("client_kind", ["sync", "async"])
@pytest.mark.asyncio
async def test_janitor_creation_strategy(
    strategy_server: PostgreSQLExecutor,
    strategy_template: DatabaseJanitor,
    strategy: str | None,
    client_kind: str,
) -> None:
    """Real sync/async clones select the requested mode and keep template data isolated."""
    dbname = f'strategy"{client_kind}'
    arguments: dict[str, Any] = {
        "user": strategy_server.user,
        "host": strategy_server.host,
        "port": strategy_server.port,
        "password": strategy_server.password,
        "dbname": dbname,
        "template_dbname": strategy_template.dbname,
        "create_strategy": strategy,
    }
    error = _unsupported_strategy_error(strategy_template, strategy_server, strategy)
    offset = len(Path(strategy_server.logfile).read_text(encoding="utf-8"))
    if client_kind == "sync":
        janitor = DatabaseJanitor(**arguments)
        if error is not None:
            with pytest.raises(error):
                janitor.init()
            return
        with janitor:
            with janitor.cursor(dbname) as cursor:
                cursor.execute("SELECT value FROM strategy_data WHERE id = 1")
                assert cursor.fetchone() == ("template",)
                cursor.execute("UPDATE strategy_data SET value = 'child' WHERE id = 1")
    else:
        async_janitor = AsyncDatabaseJanitor(**arguments)
        if error is not None:
            with pytest.raises(error):
                await async_janitor.init()
            return
        async with async_janitor:
            async with async_janitor.cursor(dbname) as cursor:
                await cursor.execute("SELECT value FROM strategy_data WHERE id = 1")
                assert await cursor.fetchone() == ("template",)
                await cursor.execute("UPDATE strategy_data SET value = 'child' WHERE id = 1")
    with strategy_template.cursor(strategy_template.dbname) as cursor:
        cursor.execute("SELECT value FROM strategy_data WHERE id = 1")
        assert cursor.fetchone() == ("template",)
    suffix = {
        None: "",
        "wal_log": " STRATEGY WAL_LOG",
        "file_copy": " STRATEGY FILE_COPY",
        "clone": " STRATEGY FILE_COPY",
    }
    quoted_name = dbname.replace('"', '""')
    expected = f'CREATE DATABASE "{quoted_name}" TEMPLATE "strategy""template"{suffix[strategy]}'
    _assert_creation(strategy_server, dbname, expected, strategy, offset)
    with strategy_template.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        assert cursor.fetchone() is None


@pytest.mark.parametrize(
    ("template_dbname", "as_template", "expected"),
    [
        (None, False, 'CREATE DATABASE "strategy_guard"'),
        (None, True, 'CREATE DATABASE "strategy_guard" IS_TEMPLATE = true'),
        (
            'strategy"template',
            True,
            'CREATE DATABASE "strategy_guard" TEMPLATE "strategy""template" IS_TEMPLATE = true',
        ),
    ],
)
def test_creation_strategy_only_changes_template_children(
    strategy_server: PostgreSQLExecutor,
    strategy_template: DatabaseJanitor,
    template_dbname: str | None,
    *,
    as_template: bool,
    expected: str,
) -> None:
    """Template creation and databases without templates keep the server default."""
    offset = len(Path(strategy_server.logfile).read_text(encoding="utf-8"))
    with DatabaseJanitor(
        user=strategy_server.user,
        host=strategy_server.host,
        port=strategy_server.port,
        password=strategy_server.password,
        dbname="strategy_guard",
        template_dbname=template_dbname,
        as_template=as_template,
        create_strategy="clone",
    ) as janitor:
        with janitor.cursor() as cursor:
            cursor.execute("SELECT datistemplate FROM pg_database WHERE datname = %s", (janitor.dbname,))
            assert cursor.fetchone() == (as_template,)
    _assert_creation(strategy_server, "strategy_guard", expected, offset=offset)


@pytest.mark.parametrize("janitor_class", [DatabaseJanitor, AsyncDatabaseJanitor])
def test_invalid_janitor_creation_strategy(janitor_class: type[DatabaseJanitor | AsyncDatabaseJanitor]) -> None:
    """Janitors reject unsupported strategy names before opening a connection."""
    with pytest.raises(ValueError, match="Invalid PostgreSQL create_strategy 'invalid'"):
        janitor_class(user="user", host="host", port=1234, dbname="dbname", create_strategy="invalid")


@pytest.fixture(scope="session")
def supported_strategy_server(strategy_server: PostgreSQLExecutor) -> PostgreSQLExecutor:
    """Skip the WAL_LOG fixture integration when PostgreSQL predates STRATEGY."""
    if strategy_server.version < Version("15"):
        pytest.skip("Explicit CREATE DATABASE strategies require PostgreSQL 15+")
    return strategy_server


strategy_noproc = postgresql_noproc(
    depends_on="supported_strategy_server", dbname="strategy_layer", create_strategy="wal_log"
)
strategy_client = postgresql("supported_strategy_server", dbname='factory"sync')
strategy_async_client = postgresql_async("strategy_noproc", dbname='factory"async')


def test_process_client_uses_creation_strategy(
    strategy_server: PostgreSQLExecutor, strategy_client: Connection
) -> None:
    """A process factory forwards its strategy to the synchronous client clone."""
    with strategy_client.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM stories")
        assert cursor.fetchone() == (4,)
    _assert_creation(
        strategy_server,
        strategy_client.info.dbname,
        f'CREATE DATABASE "factory""sync" TEMPLATE "{strategy_server.template_dbname}" STRATEGY WAL_LOG',
    )


@pytest.mark.asyncio
async def test_layered_noproc_client_uses_creation_strategy(
    strategy_server: PostgreSQLExecutor, strategy_noproc: NoopExecutor, strategy_async_client: AsyncConnection
) -> None:
    """A layered noproc factory forwards its strategy to the asynchronous client clone."""
    async with strategy_async_client.cursor() as cursor:
        await cursor.execute("SELECT count(*) FROM stories")
        assert await cursor.fetchone() == (4,)
    _assert_creation(
        strategy_server,
        strategy_async_client.info.dbname,
        f'CREATE DATABASE "factory""async" TEMPLATE "{strategy_noproc.template_dbname}" STRATEGY WAL_LOG',
    )
