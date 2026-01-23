"""Chaining noprocess fixtures tests for pytest-postgresql."""

import psycopg

from pytest_postgresql import factories
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.executor_noop import NoopExecutor


def load_schema(host: str, port: int, user: str, dbname: str, password: str | None) -> None:
    """Load schema into the database."""
    with psycopg.connect(host=host, port=port, user=user, dbname=dbname, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE schema_table (id serial PRIMARY KEY, name varchar);")
            conn.commit()


def load_data(host: str, port: int, user: str, dbname: str, password: str | None) -> None:
    """Load the first layer of data into the database."""
    with psycopg.connect(host=host, port=port, user=user, dbname=dbname, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO schema_table (name) VALUES ('data_layer');")
            cur.execute("CREATE TABLE data_table (id serial PRIMARY KEY, val varchar);")
            conn.commit()


def load_more_data(host: str, port: int, user: str, dbname: str, password: str | None) -> None:
    """Load the second layer of data into the database."""
    with psycopg.connect(host=host, port=port, user=user, dbname=dbname, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO schema_table (name) VALUES ('more_data_layer');")
            cur.execute("CREATE TABLE more_data_table (id serial PRIMARY KEY, extra varchar);")
            conn.commit()


# Chaining: proc -> noproc -> client
base_proc = factories.postgresql_proc(load=[load_schema])
seeded_noproc = factories.postgresql_noproc(depends_on="base_proc", load=[load_data])
client_layered = factories.postgresql("seeded_noproc")

# Deeper chaining: proc -> noproc -> noproc -> client
more_seeded_noproc = factories.postgresql_noproc(depends_on="seeded_noproc", load=[load_more_data])
client_deep_layered = factories.postgresql("more_seeded_noproc")


def test_chaining_two_layers(client_layered: psycopg.Connection) -> None:
    """Test that data from both proc and noproc layers is present."""
    with client_layered.cursor() as cur:
        # From base_proc (load_schema)
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'schema_table';")
        res = cur.fetchone()
        assert res
        assert res[0] == 1

        # From seeded_noproc (load_data)
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'data_table';")
        res = cur.fetchone()
        assert res
        assert res[0] == 1

        # Data inserted in seeded_noproc
        cur.execute("SELECT name FROM schema_table;")
        res = cur.fetchone()
        assert res
        assert res[0] == "data_layer"


def test_chaining_three_layers(client_deep_layered: psycopg.Connection) -> None:
    """Test that data from all three layers is present."""
    with client_deep_layered.cursor() as cur:
        # From base_proc
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'schema_table';")
        res = cur.fetchone()
        assert res
        assert res[0] == 1

        # From seeded_noproc
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'data_table';")
        res = cur.fetchone()
        assert res
        assert res[0] == 1

        # From more_seeded_noproc
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'more_data_table';")
        res = cur.fetchone()
        assert res
        assert res[0] == 1

        # Data from multiple layers
        cur.execute("SELECT name FROM schema_table ORDER BY id;")
        results = cur.fetchall()
        assert results[0][0] == "data_layer"
        assert results[1][0] == "more_data_layer"


def test_inheritance(base_proc: PostgreSQLExecutor, seeded_noproc: NoopExecutor) -> None:
    """Verify that connection parameters are inherited from the base fixture."""
    assert seeded_noproc.host == base_proc.host
    assert seeded_noproc.port == base_proc.port
    assert seeded_noproc.user == base_proc.user
    assert seeded_noproc.password == base_proc.password
