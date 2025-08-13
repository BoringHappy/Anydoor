from anydoor.dbs.postgres import Postgres
from anydoor.utils.cache import cache_db


def test_cache_db():
    schema = "test"
    table = "test_cache"
    pg = Postgres(database="postgres", schema=schema, secret_name="postgres")
    pg.execute(f"create schema if not exists {schema}")
    pg.execute(f"drop table if exists {schema}.{table}")

    @cache_db(engine=pg.engine, schema=schema, table=table)
    def cache_test(a, b, c=5):
        return {"a": a, "b": b, "c": c}

    assert cache_test(2, 2, c=3) == {"a": 2, "b": 2, "c": 3}
    assert pg.execute(f"select * from {schema}.{table}").shape == (1, 5)

    assert cache_test(1, 2, c=3) == {"a": 1, "b": 2, "c": 3}
    assert cache_test(4, 3, c=3) == {"a": 4, "b": 3, "c": 3}
    assert cache_test(4, 3, 3) == {"a": 4, "b": 3, "c": 3}

    assert pg.execute(f"select * from {schema}.{table}").shape == (4, 5)
    pg.truncate(table=table, schema=schema)
    pg.execute(f"drop table  {schema}.{table}")
    pg.execute(f"drop schema {schema}")


if __name__ == "__main__":
    test_cache_db()
    test_cache_db()
