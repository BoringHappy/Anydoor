from anydoor.utils.cache import cache_db
from anydoor.dbs.postgres import Postgres
from functools import partial


def test_cache_db():
    schema = "test"
    table = "test_cache"
    pg = Postgres(database="postgres", schema=schema, secret_name="postgres")
    pg.execute(f"drop table if exists {schema}.{table}")

    @cache_db(schema=schema, table=table, conn=pg)
    def cache_test(a, b, c=5):
        return {"a": a, "b": b, "c": c}

    assert cache_test(2, 2, c=3) == {"a": 2, "b": 2, "c": 3}
    assert pg.execute(f"select * from {schema}.{table}").shape == (1, 4)

    assert cache_test(1, 2, c=3) == {"a": 1, "b": 2, "c": 3}
    assert cache_test(4, 3, c=3) == {"a": 4, "b": 3, "c": 3}
    assert cache_test(4, 3, 3) == {"a": 4, "b": 3, "c": 3}

    assert pg.execute(f"select * from {schema}.{table}").shape == (4, 4)
    pg.truncate(table=table, schema=schema)
    pg.execute(f"drop table  {schema}.{table}")


if __name__ == "__main__":
    test_cache_db()
