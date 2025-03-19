CHANGELOG
=========

.. towncrier release notes start

7.0.1 (2025-03-19)
==================

Bugfixes
--------

- Close postgresql connection after loading sql files.

  Fixes possible `ResourceWarning`
- Fixed an issue, where defining load paths in ini/toml configuration would fail
  due to pytest's LocalPath not having an endswith method.


7.0.0 (2025-02-23)
==================

Breaking changes
----------------

- Drop support for load parameter from client fixtures. This can be easily replaced by creating intermediary fixture between test and client fixture. (`#1087 <https://github.com/dbfixtures/pytest-postgresql/issues/1087>`__)
- Stop supporting Python 3.8 as it already reached EOL


Bugfixes
--------

- Passing all environment variables to the initdb.

  This helps the cases where og_ctl is replaced by custom shell script
  making additional calls, and needs all the variables, that ie server process gets. (`#1076 <https://github.com/dbfixtures/pytest-postgresql/issues/1076>`__)


Features
--------

- If a test run ended in an error that prevented proper test cleanup,
  developer can now use `--postgresql-drop-test-database` command line flag,
  to delete database from noproc fixture at the start. (`#265 <https://github.com/dbfixtures/pytest-postgresql/issues/265>`__)
- DatabaseJanitor.cursor now accepts optional parameter dbname, which defaults to `postgres`

  This database name is used to make connection to and return a cursor. (`#265 <https://github.com/dbfixtures/pytest-postgresql/issues/265>`__)
- When running tests with xdist, pytest-postgresql now attempts to detect random ports
  selected by other nodes by writing down a .port file in session temporary directory.

  The number of tries it attempts to select unused port is configurable,
  and defaults to 0.

  In case pytest-postgresql won't be able to select unused port,
  PortForException is thrown with appropriate message. (`#872 <https://github.com/dbfixtures/pytest-postgresql/issues/872>`__)
- Mark Python 3.13 as supported.


Miscellaneus
------------

- `#707 <https://github.com/dbfixtures/pytest-postgresql/issues/707>`__, `#895 <https://github.com/dbfixtures/pytest-postgresql/issues/895>`__, `#1047 <https://github.com/dbfixtures/pytest-postgresql/issues/1047>`__, `#1048 <https://github.com/dbfixtures/pytest-postgresql/issues/1048>`__, `#1055 <https://github.com/dbfixtures/pytest-postgresql/issues/1055>`__, `#1061 <https://github.com/dbfixtures/pytest-postgresql/issues/1061>`__, `#1062 <https://github.com/dbfixtures/pytest-postgresql/issues/1062>`__, `#1063 <https://github.com/dbfixtures/pytest-postgresql/issues/1063>`__, `#1081 <https://github.com/dbfixtures/pytest-postgresql/issues/1081>`__
- Add PostgreSQL 17 to CI and drop 12
- Add [bot] suffix to the pre-commit actor.
- Adjust links after repository transfer
- Adjust workflows for actions-reuse 3.0.1
- Fix DatabaseJanitor documentation.
- In Python 3.13, usage of a positional argument for `maxsplit` has been
  formally deprecated.
  Update code to use keyword argument `maxsplit` instead of positional.
- Skip local hooks from ci
- Skip newsfragment checks for pre-commit actor.
- Update pre-commit configuration to use ruff instead of flake8/pydocstyle


6.1.1 (2024-09-05)
==================

Bugfixes
--------

- Set minimum version of `port-for` dependecy in pyproject.toml for client installations. (`#1015 <https://github.com/dbfixtures/pytest-postgresql/issues/1015>`__)


6.1.0 (2024-09-04)
==================

Features
--------

- add support for \\ character in pytest temporary path (`#982 <https://github.com/dbfixtures/pytest-postgresql/issues/982>`__)


Miscellaneus
------------

- Add test for `PR #965 <https://github.com/dbfixtures/pytest-postgresql/pull/965>`_
- refactors test_executor.py to enable Mac


6.0.1 (2024-08-14)
==================

Bugfixes
--------

- Fixed a long-standing bug, where calls to pg_ctl weren't getting `LC_*` and `LANG` envvars,
  which caused issues on some systems not recognizing --auth parameter. (`#343 <https://github.com/dbfixtures/pytest-postgresql/issues/343>`__)


Miscellaneus
------------

- `#945 <https://github.com/dbfixtures/pytest-postgresql/issues/945>`__


6.0.0 (2024-03-11)
==================

Breaking changes
----------------

- DatabaseJanitor class now accepts only keyword arguments. (`#899 <https://github.com/dbfixtures/pytest-postgresql/issues/899>`__)


Bugfixes
--------

- Fix the remaining `DepcrecationWarning` for `datetime.datetime.utcnow` on Python 3.12. (`#896 <https://github.com/dbfixtures/pytest-postgresql/issues/896>`__)


Deprecations
------------

- Deprecated load parameter on a client fixture.
  Developers are encouraged to either use the load function/parameter
  for process fixture, or create an intermediate fixture placed between client
  and tests themselves to fill in the data. (`#850 <https://github.com/dbfixtures/pytest-postgresql/issues/850>`__)


Features
--------

- Now all sql files used to initialise database for tests, has to be passed as pathlib.Path instance.

  This helps the DatabaseJanitor choose correct behaviour based on parameter. (`#638 <https://github.com/dbfixtures/pytest-postgresql/issues/638>`__)
- Have separate parameters for template database name and database name in DatabaseJanitor.
  It'll make it much clearer to understand the code and Janitor's behaviour. (`#672 <https://github.com/dbfixtures/pytest-postgresql/issues/672>`__)
- Template databases are now created with is_template flag turned on, and not by setting allow_connections to false.

  The allow_connections flag being set to false is used strictly right before we attempt to drop the database. (`#914 <https://github.com/dbfixtures/pytest-postgresql/issues/914>`__)


Miscellaneus
------------

- `#865 <https://github.com/dbfixtures/pytest-postgresql/issues/865>`__, `#882 <https://github.com/dbfixtures/pytest-postgresql/issues/882>`__, `#893 <https://github.com/dbfixtures/pytest-postgresql/issues/893>`__, `#900 <https://github.com/dbfixtures/pytest-postgresql/issues/900>`__


5.1.1 (2024-03-07)
==================

Miscellaneus
------------

- Add missing python 3.12 classifier to pythin package. (`#915 <https://github.com/dbfixtures/pytest-postgresql/issues/915>`__)


5.1.0 (2024-01-29)
==================

Bugfixes
--------

- Update postgresql_load default value for it to fallback into the empty list (`#881 <https://github.com/dbfixtures/pytest-postgresql/issues/881>`__)


Features
--------

- Support for Python 3.12 (`#835 <https://github.com/dbfixtures/pytest-postgresql/issues/835>`__)


Miscellaneus
------------

- Removed misleading parapgraph from one example in a README (`#688 <https://github.com/dbfixtures/pytest-postgresql/issues/688>`__)
- Fixed typing exceptions for mypy 1.4 (`#765 <https://github.com/dbfixtures/pytest-postgresql/issues/765>`__)
- README: fix section markup (`#771 <https://github.com/dbfixtures/pytest-postgresql/issues/771>`__, `#776 <https://github.com/dbfixtures/pytest-postgresql/issues/776>`__)
- Add SQLAlchemy example for initialising common database state. (`#797 <https://github.com/dbfixtures/pytest-postgresql/issues/797>`__)
- Rely on cached/artifacted Pipfile.lock.
  That way, it will be specifically defined for given python version/system. (`#834 <https://github.com/dbfixtures/pytest-postgresql/issues/834>`__)
- CI Change - Add PostgreSQL 16, drop PostgreSQL 11 (`#836 <https://github.com/dbfixtures/pytest-postgresql/issues/836>`__)
- Replace usage of `pkg_resources.parse_version` with `packaging.version.parse` (`#858 <https://github.com/dbfixtures/pytest-postgresql/issues/858>`__)


5.0.0 (2023-05-20)
==================

Breaking changes
----------------

- Drop support for Python 3.7 (`#706 <https://github.com/dbfixtures/pytest-postgresql/issues/706>`__)
- psycopg is now a mandatory requirement.
  With pyscop2 it was optional due to the different ways it could be installed:

  * psycopg2 - that built itself
  * psycopg2-binary - binary distribution
  * psycopg2cffi - pypy enabled version

  Since psycopg version 3 there's only one package to install. (`#744 <https://github.com/dbfixtures/pytest-postgresql/issues/744>`__)
- Dropped --postgresql-logsprefix/postgresql_logsprefix options. All fixture data is already distinguished by tmpdir itself. (`#748 <https://github.com/dbfixtures/pytest-postgresql/issues/748>`__)


Features
--------

- Re-rise FileNotFound errors with more meaningful messages. (`#598 <https://github.com/dbfixtures/pytest-postgresql/issues/598>`__)
- Support Python 3.11 (`#678 <https://github.com/dbfixtures/pytest-postgresql/issues/678>`__)


Miscellaneus
------------

- Drop PyPy from CI (`#669 <https://github.com/dbfixtures/pytest-postgresql/issues/669>`__)
- pytest-postgresql will now recognise and use development postgresql versions (`#691 <https://github.com/dbfixtures/pytest-postgresql/issues/691>`__)
- Use towncrier to maintain project's newsfragments/changelog (`#700 <https://github.com/dbfixtures/pytest-postgresql/issues/700>`__)
- Move project dependency management to pipenv (`#701 <https://github.com/dbfixtures/pytest-postgresql/issues/701>`__)
- Migrate to shared automerge workflow for automatic tests dependency updates (`#702 <https://github.com/dbfixtures/pytest-postgresql/issues/702>`__)
- Use tbump instead of bumpversion to manage versioning and releases. (`#703 <https://github.com/dbfixtures/pytest-postgresql/issues/703>`__)
- Move most of package configuration to pyproject.toml (`#704 <https://github.com/dbfixtures/pytest-postgresql/issues/704>`__)
- Introduce Typed config dict (`#706 <https://github.com/dbfixtures/pytest-postgresql/issues/706>`__)
- Use ankane/setup-postgres@v1 to setup postgresql in CI instead of custom scripts. (`#708 <https://github.com/dbfixtures/pytest-postgresql/issues/708>`__)
- Pass codecov_token to codecov action to upload coverage. (`#721 <https://github.com/dbfixtures/pytest-postgresql/issues/721>`__)
- Replaced flake8 and pydocstyle with ruff, turned on isort rules (`#735 <https://github.com/dbfixtures/pytest-postgresql/issues/735>`__)
- Split single bid test job into smaller running each after another. (`#740 <https://github.com/dbfixtures/pytest-postgresql/issues/740>`__)


4.1.1
=====

Misc
----

- Error message typo fix
- Docker documentation example typo fixes
- Have setuptools required as package dependency. pkg_resources.parse_version is used in code
  but setuptools was only used as build requirements

4.1.0
=====

Features
--------

- Import FixtureRequest from pytest, not private _pytest.
  Require at least pytest 6.2
- Replace tmpdir_factory with tmp_path_factory

4.0.0
=====

Features
--------

- Upgrade to psycopg 3.
- Xdist running test connecting to already existing postgresql,
  will now create separate databases for each worker.

Backward Incompatibilities
--------------------------

- Use psycopg 3 and return its connections in client fixtures.
- Drop support for postgresql 9.6
- client fixture will no longer utilize configuration's load param
- client fixture will no longer utilize configuration's dbanme parameter

Misc
----

- Add Postgresql 14 to the CI

3.1.2
=====

Bugfix
------

- Database can be created by DatabaseJanitor or the client fixture when an isolation
  level is specified.

3.1.1
=====

Misc
----

- rely on `get_port` functionality delivered by `port_for`

3.1.0
=====

Features
--------

- Added type annotations and compatibitlity with PEP 561

Misc
----

- pre-commit configuration

3.0.2
=====

Bugfix
------

- Changed `UPDATE pg_database SET` to `ALTER`. System tables should not be updated.

3.0.1
=====

Bugfix
------

- Fixed DatabaseJanitor port type hint to int from str
- Changed retry definition to not fail if psycopg2 is not installed.
  Now the default is Exception.

Misc
----

- Support python 3.7 and up

3.0.0
=====

Features
--------

- Ability to create template database once for the process fixture and
  re-recreate a clean database out of it every test. Not only it does provide some
  common db initialisation between tests but also can speed up tests significantly,
  especially if the initialisation has lots of operations to perform.
- DatabaseJanitor can now define a `connection_timeout` parameter.
  How long will it try to connect to database before raising a TimeoutError
- Updated supported python versions
- Unified temporary directory handling in fixture. Settled on tmpdir_factory.
- Fully moved to the Github Actions as CI/CD pipeline

Deprecations
------------

- Deprecated support for `logs_prefix` process fixture factory argument,
  `--postgresql-logsprefix` pytest command line option and `postgresql_logsprefix`
  ini configuration option. tmpdir_factory now builds pretty unique temporary directory structure.

Backward Incompatibilities
--------------------------

- Dropped support for postgresql 9.5 and down
- Removed init_postgresql_database and drop_postgresql_database functions.
  They were long deprecated and their role perfectly covered by DatabaseJanitor class.
- `pytest_postgresql.factories.get_config` was moved to `pytest_postgresql.config.get_config`
- all `db_name` keywords and attributes were renamed to `dbname`
- postgresql_nooproc fixture was renamed to postgresql_noproc

Bugfix
------

- Use `postgresql_logsprefix` and `--postgresql-logsprefix` again.
  They were stopped being used somewhere along the way.
- Sometimes pytest-postrgesql would fail to start postgresql with
  "FATAL:  the database system is starting up" message. It's not really a fatal error,
  but a message indicating that the process still starts. Now pytest-postgresql will wait properly in this cases.

2.6.1
=====

- [bugfix] To not fail loading code if no postgresql version is installed.
  Fallback for janitor and process fixture only, if called upon.

2.6.0
=====

- [enhancement] add ability to pass options to pg_ctl's -o flag to send arguments to the underlying postgres executable
  Use `postgres_options` as fixture argument, `--postgresql-postgres-options` as pytest starting option or
  `postgresql_postgres_options` as pytest.ini configuration option

2.5.3
=====

- [enhancement] Add ability to set up isolation level for fixture and janitor

2.5.2
=====

- [fix] Status checks for running postgres depend on pg_ctl status code,
  not on pg_ctl log language. Fixes starting on systems without C locale.
  Thanks @Martin Meyries.


2.5.1
=====

- [fix] Added LC_* env vars to running initdb and other utilities.
  Now all tools and server are using same, C locale


2.5.0
=====

- [feature] Ability to define default schema to initialize database with
- [docs] Added more examples to readme on how to use the plugin


2.4.1
=====

- [enhancement] extract NoopExecutor into it's own submodule
- [bugfix] Ignore occasional `ProcessFinishedWithError` error on executor exit.
- [bugfix] Fixed setting custom password for process fixture
- [bugfix] Fix version detection, to allow for two-digit minor version part

2.4.0
=====

- [feature] Drop support for python 3.5
- [enhancement] require at least mirakuru 2.3.0 (executor's stop method parameter's change)
- [bug] pass password to DatabaseJanitor in client's factory

2.3.0
=====

- [feature] Allow to set password for postgresql. Use it throughout the flow.
- [bugfix] Default Janitor's connections to postgres database. When using custom users,
  postgres attempts to use user's database and it might not exist.
- [bugfix] NoopExecutor connects to read version by context manager to properly handle cases
  where it can't connect to the server.

2.2.1
=====

- [bugfix] Fix drop_postgresql_database to actually use DatabaseJanitor.drop instead of an init

2.2.0
=====

- [feature] ability to properly connect to already existing postgresql server using ``postgresql_nooproc`` fixture.

2.1.0
=====

- [enhancement] Gather helper functions maintaining postgresql database in DatabaseJanitor class.
- [deprecate] Deprecate ``init_postgresql_database`` in favour of ``DatabaseJanitor.init``
- [deprecate] Deprecate ``drop_postgresql_database`` in favour of ``DatabaseJanitor.drop``

2.0.0
=====

- [feature] Drop support for python 2.7. From now on, only support python 3.5 and up
- [feature] Ability to configure database name through plugin options
- [enhancement] Use tmpdir_factory. Drop ``logsdir`` parameter
- [ehnancement] Support only Postgresql 9.0 and up
- [bugfix] Always start postgresql with LC_ALL, LC_TYPE and LANG set to C.UTF-8.
  It makes postgresql start in english.

1.4.1
=====

- [bugfix] Allow creating test database with hyphens

1.4.0
=====

- [enhancements] Ability to configure additional options for postgresql process and connection
- [bugfix] - removed hard dependency on ``psycopg2``, allowing any of its alternative packages, like
  ``psycopg2-binary``, to be used.
- [maintenance] Drop support for python 3.4 and use 3.7 instead

1.3.4
=====

- [bugfix] properly detect if executor running and clean after executor is being stopped

    .. note::

        Previously if a test failed, there was a possibility of the executor being removed when python was closing,
        causing it to print ignored errors on already unloaded modules.

1.3.3
=====

- [enhancement] use executor's context manager to start/stop postrgesql server in a fixture

1.3.2
=====

- [bugfix] version regexp to correctly catch postgresql 10

1.3.1
=====

- [enhancement] explicitly turn off logging_collector

1.3.0
=====

- [feature] pypy compatibility

1.2.0
=====

- [bugfix] - disallow connection to database before it gets dropped.

    .. note::

        Otherwise it caused random test subprocess to connect again and this the drop was unsuccessful which resulted in many more test fails on setup.

- [cleanup] - removed path.py dependency

1.1.1
=====

- [bugfix] - Fixing the default pg_ctl path creation

1.1.0
=====

- [feature] - migrate usage of getfuncargvalue to getfixturevalue. require at least pytest 3.0.0

1.0.0
=====

- create command line and pytest.ini configuration options for postgresql starting parameters
- create command line and pytest.ini configuration options for postgresql username
- make the port random by default
- create command line and pytest.ini configuration options for executable
- create command line and pytest.ini configuration options for host
- create command line and pytest.ini configuration options for port
- Extracted code from pytest-postgresql
