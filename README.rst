.. image:: https://raw.githubusercontent.com/dbfixtures/pytest-postgresql/main/logo.png
    :width: 100px
    :height: 100px

pytest-postgresql
=================

.. image:: https://img.shields.io/pypi/v/pytest-postgresql.svg
    :target: https://pypi.python.org/pypi/pytest-postgresql/
    :alt: Latest PyPI version

.. image:: https://img.shields.io/pypi/wheel/pytest-postgresql.svg
    :target: https://pypi.python.org/pypi/pytest-postgresql/
    :alt: Wheel Status

.. image:: https://img.shields.io/pypi/pyversions/pytest-postgresql.svg
    :target: https://pypi.python.org/pypi/pytest-postgresql/
    :alt: Supported Python Versions

.. image:: https://img.shields.io/pypi/l/pytest-postgresql.svg
    :target: https://pypi.python.org/pypi/pytest-postgresql/
    :alt: License

What is this?
=============

This is a pytest plugin, that enables you to test your code that relies on a running PostgreSQL Database.
It allows you to specify fixtures for PostgreSQL process and client.

How to use
==========

.. warning::

    Tested on PostgreSQL versions >= 14. See tests for more details.

Install with:

.. code-block:: sh

    pip install pytest-postgresql

You will also need to install ``psycopg``. See `its installation instructions <https://www.psycopg.org/psycopg3/docs/basic/install.html>`_.
Note that this plugin requires ``psycopg`` version 3. It is possible to simultaneously install version 3
and version 2 for libraries that require the latter (see `those instructions <https://www.psycopg.org/docs/install.html>`_).

How does it work
----------------

.. image:: https://raw.githubusercontent.com/dbfixtures/pytest-postgresql/main/docs/images/architecture.svg
    :alt: Project Architecture Diagram
    :align: center


Plugin contains three fixtures:

* **postgresql** - it's a client fixture that has functional scope.
  After each test it ends all leftover connections, and drops test database
  from PostgreSQL ensuring repeatability.
  This fixture returns already connected psycopg connection.

* **postgresql_proc** - session scoped fixture, that starts PostgreSQL instance
  at it's first use and stops at the end of the tests.
* **postgresql_noproc** - a noprocess fixture, that's connecting to already
  running postgresql instance.
  For example on dockerized test environments, or CI providing postgresql services

Simply include one of these fixtures into your tests fixture list.

You can also create additional postgresql client and process fixtures if you'd need to:


.. code-block:: python

    from pytest_postgresql import factories

    postgresql_my_proc = factories.postgresql_proc(
        port=None, unixsocketdir='/var/run')
    postgresql_my = factories.postgresql('postgresql_my_proc')

.. note::

    Each PostgreSQL process fixture can be configured in a different way than the others through the fixture factory arguments.

Sample test

.. code-block:: python

    def test_example_postgres(postgresql):
        """Check main postgresql fixture."""
        cur = postgresql.cursor()
        cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        postgresql.commit()
        cur.close()

Pre-populating the database for tests
-------------------------------------

If you want the database fixture to be automatically pre-populated with your schema and data, there are two lewels you can achieve it:

#. per test in a client fixture, by an intermediary fixture between client and your test (or other fixtures)
#. per session in a process fixture

The process fixture accepts a load parameter, which accepts these loaders:

* sql file path - which will load and execute sql files
* loading functions - either by string import path, actual callable.
  Loading functions will receive **host**, **port**, **user**, **dbname** and **password** arguments and will have to perform
  connection to the database inside. Or start session in the ORM of your choice to perform actions with given ORM.
  This way, you'd be able to trigger ORM based data manipulations, or even trigger database migrations programmatically.

The process fixture pre-populates the database once per test session (at the start of the process fixture),
and loads the schema and data into the template database. Client fixture then creates test database out of the template database each test,
which significantly **speeds up the tests**.

.. code-block:: python

    from pathlib import Path
    postgresql_my_proc = factories.postgresql_proc(
        load=[Path("schemafile.sql"), Path("otherschema.sql"), "import.path.to.function", "import.path.to:otherfunction", load_this]
    )

Additional benefit, is that test code might safely use separate database connection, and can safely test it's behaviour with transactions and rollbacks,
as tests and code will work on separate database connections.

Defining pre-populate on command line:

.. code-block:: sh

    pytest --postgresql-populate-template=path.to.loading_function --postgresql-populate-template=path.to.other:loading_function --postgresql-populate-template=path/to/file.sql

Connecting to already existing postgresql database
--------------------------------------------------

Some projects are using already running postgresql servers (ie on docker instances).
In order to connect to them, one would be using the ``postgresql_noproc`` fixture.

.. code-block:: python

    postgresql_external = factories.postgresql('postgresql_noproc')

By default the  ``postgresql_noproc`` fixture would connect to postgresql instance using **5432** port. Standard configuration options apply to it.

These are the configuration options that are working on all levels with the ``postgresql_noproc`` fixture:

Configuration
=============

You can define your settings in three ways, it's fixture factory argument, command line option and pytest.ini configuration option.
You can pick which you prefer, but remember that these settings are handled in the following order:

    * ``Fixture factory argument``
    * ``Command line option``
    * ``Configuration option in your pytest.ini file``


.. list-table:: Configuration options
   :header-rows: 1

   * - PostgreSQL option
     - Fixture factory argument
     - Command line option
     - pytest.ini option
     - Noop process fixture
     - Default
   * - Path to executable
     - executable
     - --postgresql-exec
     - postgresql_exec
     - -
     - /usr/lib/postgresql/13/bin/pg_ctl
   * - host
     - host
     - --postgresql-host
     - postgresql_host
     - yes
     - 127.0.0.1
   * - port
     - port
     - --postgresql-port
     - postgresql_port
     - yes (5432)
     - random
   * - Port search count
     -
     - --postgresql-port-search-count
     - postgresql_port_search_count
     - -
     - 5
   * - postgresql user
     - user
     - --postgresql-user
     - postgresql_user
     - yes
     - postgres
   * - password
     - password
     - --postgresql-password
     - postgresql_password
     - yes
     -
   * - Starting parameters (extra pg_ctl arguments)
     - startparams
     - --postgresql-startparams
     - postgresql_startparams
     - -
     - -w
   * - Postgres exe extra arguments (passed via pg_ctl's -o argument)
     - postgres_options
     - --postgresql-postgres-options
     - postgresql_postgres_options
     - -
     -
   * - Location for unixsockets
     - unixsocket
     - --postgresql-unixsocketdir
     - postgresql_unixsocketdir
     - -
     - $TMPDIR
   * - Database name which will be created by the fixtures
     - dbname
     - --postgresql-dbname
     - postgresql_dbname
     - yes, however with xdist an index is being added to name, resulting in test0, test1 for each worker.
     - test
   * - Default Schema either in sql files or import path to function that will load it (list of values for each)
     - load
     - --postgresql-load
     - postgresql_load
     - yes
     -
   * - PostgreSQL connection options
     - options
     - --postgresql-options
     - postgresql_options
     - yes
     -
   * - Drop test database on start.

       .. warning::

           Use carefully as it might lead to unexpected results within your test suite.
     -
     - --postgresql-drop-test-database
     -
     - false
     -




Example usage:

* pass it as an argument in your own fixture

    .. code-block:: python

        postgresql_proc = factories.postgresql_proc(
            port=8888)

* use ``--postgresql-port`` command line option when you run your tests

    .. code-block:: sh

        pytest tests --postgresql-port=8888


* specify your port as ``postgresql_port`` in your ``pytest.ini`` file.

    To do so, put a line like the following under the ``[pytest]`` section of your ``pytest.ini``:

    .. code-block:: ini

        [pytest]
        postgresql_port = 8888

Examples
========

Populating database for tests
-----------------------------

With SQLAlchemy
+++++++++++++++

This example shows how to populate database and create an SQLAlchemy's ORM connection:

Sample below is simplified session fixture from
`pyramid_fullauth <https://github.com/fizyk/pyramid_fullauth/>`_ tests:

.. code-block:: python

    from psycopg import Connection
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker, Session
    from sqlalchemy.pool import NullPool
    from zope.sqlalchemy import register


    @pytest.fixture
    def db_session(postgresql: Connection) -> Iterator[Session]:
        """Session for SQLAlchemy."""
        from pyramid_fullauth.models import Base

        connection = f'postgresql+psycopg://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'

        engine = create_engine(connection, echo=False, poolclass=NullPool)
        pyramid_basemodel.Session = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
        pyramid_basemodel.bind_engine(
            engine, pyramid_basemodel.Session, should_create=True, should_drop=True)

        yield pyramid_basemodel.Session

        transaction.commit()
        Base.metadata.drop_all(engine)


    @pytest.fixture
    def user(db_session: Session) -> User:
        """Test user fixture."""
        from pyramid_fullauth.models import User
        from tests.tools import DEFAULT_USER

        new_user = User(**DEFAULT_USER)
        db_session.add(new_user)
        transaction.commit()
        return new_user


    def test_remove_last_admin(db_session: pyramid_basemodel.Session, user: User) -> None:
        """
        Sample test checks internal login, but shows usage in tests with SQLAlchemy
        """
        user = db_session.merge(user)
        user.is_admin = True
        transaction.commit()
        user = db_session.merge(user)

        with pytest.raises(AttributeError):
            user.is_admin = False
.. note::

    See the original code at `pyramid_fullauth's conftest file <https://github.com/fizyk/pyramid_fullauth/blob/2950e7f4a397b313aaf306d6d1a763ab7d8abf2b/tests/conftest.py#L35>`_.
    Depending on your needs, that in between code can fire alembic migrations in case of sqlalchemy stack or any other code

Maintaining database state outside of the fixtures
--------------------------------------------------

It is possible and appears it's used in other libraries for tests,
to maintain database state with the use of the ``pytest-postgresql`` database
managing functionality:

For this import DatabaseJanitor and use its init and drop methods:


.. code-block:: python

    from typing import Iterator

    import psycopg
    from psycopg import Connection
    import pytest
    from pytest_postgresql.janitor import DatabaseJanitor
    from pytest_postgresql.executor import PostgreSQLExecutor

    @pytest.fixture
    def database(postgresql_proc: PostgreSQLExecutor) -> Iterator[Connection]:
        # variable definition

        janitor = DatabaseJanitor(
            user=postgresql_proc.user,
            host=postgresql_proc.host,
            proc=postgresql_proc.port,
            testdb="my_test_database",
            version=postgresql_proc.version,
            password="secret_password",
        )
        janitor.init()
        yield psycopg.connect(
            dbname="my_test_database",
            user=postgresql_proc.user,
            password="secret_password",
            host=postgresql_proc.host,
            port=postgresql_proc.port,
        )
        janitor.drop()

or use it as a context manager:

.. code-block:: python

    from typing import Iterator

    import psycopg
    from psycopg import Connection
    import pytest
    from pytest_postgresql.janitor import DatabaseJanitor
    from pytest_postgresql.executor import PostgreSQLExecutor

    @pytest.fixture
    def database(postgresql_proc: PostgreSQLExecutor) -> Iterator[Connection]:
        # variable definition

        with DatabaseJanitor(
            user=postgresql_proc.user,
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            dbname="my_test_database",
            version=postgresql_proc.version,
            password="secret_password",
        ):
            yield psycopg.connect(
                dbname="my_test_database",
                user=postgresql_proc.user,
                password="secret_password",
                host=postgresql_proc.host,
                port=postgresql_proc.port,
            )

.. note::

    DatabaseJanitor manages the state of the database, but you'll have to create
    connection to use in test code yourself.

    You can optionally pass in a recognized postgresql ISOLATION_LEVEL for
    additional control.

.. note::

    See DatabaseJanitor usage in python's warehouse test code https://github.com/pypa/warehouse/blob/5d15bfe/tests/conftest.py#L127

Connecting to Postgresql (in a docker)
--------------------------------------

To connect to a docker run postgresql and run test on it, use noproc fixtures.

.. code-block:: sh

    docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres

This will start postgresql in a docker container, however using a postgresql installed locally is not much different.

In tests, make sure that all your tests are using **postgresql_noproc** fixture like that:

.. code-block:: python

    from psycopg import Connection
    from pytest_postgresql import factories

    postgresql_in_docker = factories.postgresql_noproc()
    postgresql = factories.postgresql("postgresql_in_docker", dbname="test")


    def test_postgres_docker(postgresql: Connection) -> None:
        """Run test."""
        cur = postgresql.cursor()
        cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        postgresql.commit()
        cur.close()

And run tests:

.. code-block:: sh

    pytest --postgresql-host=172.17.0.2 --postgresql-password=mysecretpassword

Basic database state for all tests
----------------------------------

If you've got several tests that require common initialisation, you can to define a `load` and pass it to
your custom postgresql process fixture:

.. code-block:: python

    import psycopg
    import pytest_postgresql.factories
    def load_database(**kwargs: str) -> None:
        db_connection: psycopg.Connection = psycopg.connect(**kwargs)
        with db_connection.cursor() as cur:
            cur.execute("CREATE TABLE stories (id serial PRIMARY KEY, name varchar);")
            cur.execute(
                "INSERT INTO stories (name) VALUES"
                "('Silmarillion'), ('Star Wars'), ('The Expanse'), ('Battlestar Galactica')"
            )
            db_connection.commit()

    postgresql_proc = factories.postgresql_proc(
        load=[load_database],
    )

    postgresql = factories.postgresql(
        "postgresql_proc",
    )

The way this will work is that the process fixture will populate template database,
which in turn will be used automatically by client fixture to create a test database from scratch.
Fast, clean and no dangling transactions, that could be accidentally rolled back.

Same approach will work with noproces fixture, while connecting to already running postgresql instance whether
it'll be on a docker machine or running remotely or locally.

Using SQLAlchemy to initialise basic database state
+++++++++++++++++++++++++++++++++++++++++++++++++++

How to use SQLAlchemy for common initialisation:

.. code-block:: python

    from typing import Iterator

    import psycopg
    from sqlalchemy.orm import Session

    def load_database(**kwargs: str) -> None:
        from your_package import Base
        connection = f"postgresql+psycopg://{kwargs['user']}:@{kwargs['host']}:{kwargs['port']}/{kwargs['dbname']}"
        engine = create_engine(connection)
        Base.metadata.create_all(engine)
        session = scoped_session(sessionmaker(bind=engine))
        # add things to session
        session.commit()

    postgresql_proc = factories.postgresql_proc(load=[load_database])

    postgresql = factories.postgresql('postgresql_proc') # still need to check if this is actually needed or not

    @pytest.fixture
    def dbsession(postgresql: psycopg.Connection) -> Iterator[Session]:
        connection = f'postgresql+psycopg://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'
        engine = create_engine(connection)

        session = scoped_session(sessionmaker(bind=engine))

        yield session
        # 'Base.metadata.drop_all(engine)' here specifically does not work. It is also not needed. If you leave out the session.close()
        # all the tests still run, but you get a warning/error at the end of the tests.
        session.close()


Release
=======

Install pipenv and --dev dependencies first, Then run:

.. code-block:: sh

    pipenv run tbump [NEW_VERSION]
