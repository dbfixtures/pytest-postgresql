Scope the ``test_postgres_terminate_connection`` (and async) connection check
to the test's own database via ``datname = current_database()``.

So a transient, already-closed janitor connection on the ``postgres`` maintenance
database no longer causes spurious timeouts on macOS CI.
