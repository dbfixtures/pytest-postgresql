"""Database Janitor."""

import asyncio
import inspect
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from types import TracebackType
from typing import AsyncIterator, Callable, Iterator, Type, TypeVar

import psycopg
import psycopg.sql as sql
from packaging.version import parse
from psycopg import AsyncCursor, Connection, Cursor

from pytest_postgresql.loader import build_loader, sql_async
from pytest_postgresql.retry import retry, retry_async

Version = type(parse("1"))


DatabaseJanitorType = TypeVar("DatabaseJanitorType", bound="DatabaseJanitor")
AsyncDatabaseJanitorType = TypeVar("AsyncDatabaseJanitorType", bound="AsyncDatabaseJanitor")


class BaseDatabaseJanitor:
    """Common base class for database janitors."""

    user: str
    password: str | None
    host: str
    port: str | int
    dbname: str
    template_dbname: str | None
    as_template: bool
    _connection_timeout: int
    isolation_level: "psycopg.IsolationLevel | None"
    version: Version  # type: ignore[valid-type]

    def __init__(
        self,
        *,
        user: str,
        host: str,
        port: str | int,
        version: str | float | Version,  # type: ignore[valid-type]
        dbname: str,
        template_dbname: str | None = None,
        as_template: bool = False,
        password: str | None = None,
        isolation_level: "psycopg.IsolationLevel | None" = None,
        connection_timeout: int = 60,
    ) -> None:
        """Initialize janitor.

        :param user: postgresql username
        :param host: postgresql host
        :param port: postgresql port
        :param dbname: database name
        :param template_dbname: template database name to clone from
        :param as_template: whether to mark the database as a template
        :param version: postgresql version number
        :param password: optional postgresql password
        :param isolation_level: optional postgresql isolation level
            defaults to server's default
        :param connection_timeout: how long to retry connection before
            raising a TimeoutError
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.template_dbname = template_dbname
        self.as_template = as_template
        self._connection_timeout = connection_timeout
        self.isolation_level = isolation_level
        if not isinstance(version, Version):
            self.version = parse(str(version))
        else:
            self.version = version

    def is_template(self) -> bool:
        """Determine whether the janitor maintains template or database."""
        return self.as_template

    def _build_create_database_sql(self) -> sql.Composed:
        """Build the CREATE DATABASE statement for janitor init."""
        query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbname))
        if self.template_dbname:
            query = query + sql.SQL(" TEMPLATE {}").format(sql.Identifier(self.template_dbname))
        if self.is_template():
            query = query + sql.SQL(" IS_TEMPLATE = true")
        return query


class DatabaseJanitor(BaseDatabaseJanitor):
    """Manage database state for specific tasks."""

    def init(self) -> None:
        """Create database in postgresql."""
        with self.cursor() as cur:
            if self.template_dbname:
                # And make sure no-one is left connected to the template database.
                # Otherwise, Creating database from template will fail
                self._terminate_connection(cur, self.template_dbname)
            query = self._build_create_database_sql()
            cur.execute(query)

    @staticmethod
    def _database_exists(cur: Cursor, dbname: str) -> bool:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        return cur.fetchone() is not None

    def drop(self) -> None:
        """Drop database in postgresql."""
        # We cannot drop the database while there are connections to it, so we
        # terminate all connections first while not allowing new connections.
        with self.cursor() as cur:
            if not self._database_exists(cur, self.dbname):
                return
            self._dont_datallowconn(cur, self.dbname)
            self._terminate_connection(cur, self.dbname)
            if self.is_template():
                cur.execute(sql.SQL("ALTER DATABASE {} WITH is_template false").format(sql.Identifier(self.dbname)))
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self.dbname)))

    @staticmethod
    def _dont_datallowconn(cur: Cursor, dbname: str) -> None:
        cur.execute(sql.SQL("ALTER DATABASE {} WITH allow_connections false").format(sql.Identifier(dbname)))

    @staticmethod
    def _terminate_connection(cur: Cursor, dbname: str) -> None:
        cur.execute(
            "SELECT pg_terminate_backend(pg_stat_activity.pid) "
            "FROM pg_stat_activity "
            "WHERE pg_stat_activity.datname = %s;",
            (dbname,),
        )

    def load(self, load: Callable | str | Path) -> None:
        """Load data into a database.

        Expects:

            * a Path to sql file, that'll be loaded
            * an import path to import callable
            * a callable that expects: host, port, user, dbname and password arguments.

        """
        _loader = build_loader(load)
        _loader(
            host=self.host,
            port=self.port,
            user=self.user,
            dbname=self.dbname,
            password=self.password,
        )

    @contextmanager
    def cursor(self, dbname: str = "postgres") -> Iterator[Cursor]:
        """Return postgresql cursor."""

        def connect() -> Connection:
            return psycopg.connect(
                dbname=dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

        conn = retry(connect, timeout=self._connection_timeout, possible_exception=psycopg.OperationalError)
        if self.isolation_level is not None:
            conn.isolation_level = self.isolation_level
        # We must not run a transaction since we create a database.
        conn.autocommit = True
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
            conn.close()

    def __enter__(self: DatabaseJanitorType) -> DatabaseJanitorType:
        """Initialize Database Janitor."""
        self.init()
        return self

    def __exit__(
        self: DatabaseJanitorType,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit from Database janitor context cleaning after itself."""
        self.drop()


class AsyncDatabaseJanitor(BaseDatabaseJanitor):
    """Manage database state asynchronously for specific tasks."""

    async def init(self) -> None:
        """Create database in postgresql."""
        async with self.cursor() as cur:
            if self.template_dbname:
                # And make sure no-one is left connected to the template database.
                # Otherwise, Creating database from template will fail
                await self._terminate_connection(cur, self.template_dbname)
            query = self._build_create_database_sql()
            await cur.execute(query)

    @staticmethod
    async def _database_exists(cur: AsyncCursor, dbname: str) -> bool:
        await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        return await cur.fetchone() is not None

    async def drop(self) -> None:
        """Drop database in postgresql."""
        # We cannot drop the database while there are connections to it, so we
        # terminate all connections first while not allowing new connections.
        async with self.cursor() as cur:
            if not await self._database_exists(cur, self.dbname):
                return
            await self._dont_datallowconn(cur, self.dbname)
            await self._terminate_connection(cur, self.dbname)
            if self.is_template():
                await cur.execute(
                    sql.SQL("ALTER DATABASE {} WITH is_template false").format(sql.Identifier(self.dbname))
                )
            await cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self.dbname)))

    @staticmethod
    async def _dont_datallowconn(cur: AsyncCursor, dbname: str) -> None:
        await cur.execute(sql.SQL("ALTER DATABASE {} WITH allow_connections false").format(sql.Identifier(dbname)))

    @staticmethod
    async def _terminate_connection(cur: AsyncCursor, dbname: str) -> None:
        await cur.execute(
            "SELECT pg_terminate_backend(pg_stat_activity.pid) "
            "FROM pg_stat_activity "
            "WHERE pg_stat_activity.datname = %s;",
            (dbname,),
        )

    async def load(self, load: Callable | str | Path) -> None:
        """Load data into a database.

        Expects:

            * a Path to sql file, that'll be loaded
            * an import path to import callable
            * a callable that expects: host, port, user, dbname and password arguments.

        """
        _loader = build_loader(load, sql_loader=sql_async)
        loader_kwargs = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "dbname": self.dbname,
            "password": self.password,
        }
        loader_func = getattr(_loader, "func", _loader)
        if inspect.iscoroutinefunction(loader_func):
            result = _loader(**loader_kwargs)
            if inspect.isawaitable(result):
                await result
        else:
            result = await asyncio.to_thread(_loader, **loader_kwargs)
            if inspect.isawaitable(result):
                await result

    @asynccontextmanager
    async def cursor(self, dbname: str = "postgres") -> AsyncIterator[AsyncCursor]:
        """Return postgresql async cursor."""

        async def connect() -> psycopg.AsyncConnection:
            return await psycopg.AsyncConnection.connect(
                dbname=dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

        conn = await retry_async(connect, timeout=self._connection_timeout, possible_exception=psycopg.OperationalError)
        try:
            if self.isolation_level is not None:
                await conn.set_isolation_level(self.isolation_level)
            await conn.set_autocommit(True)
            # We must not run a transaction since we create a database.
            async with conn.cursor() as cur:
                yield cur
        finally:
            await conn.close()

    async def __aenter__(self: AsyncDatabaseJanitorType) -> AsyncDatabaseJanitorType:
        """Initialize Async Database Janitor."""
        await self.init()
        return self

    async def __aexit__(
        self: AsyncDatabaseJanitorType,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit from Async Database Janitor context cleaning after itself."""
        await self.drop()
