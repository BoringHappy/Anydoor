from anydoor.utils import cache_db
from anydoor.dbs import Postgres
from functools import partial


def test_cache_db():
    schema = "test"
    table = "test_cache"
    pg = Postgres(database="postgres", schema=schema, secret_name="postgres")

    @cache_db(schema=schema, table=table, conn=pg)
    def cache_test(a, b, c=5):
        return {"a": a, "b": b, "c": c}

    assert cache_test(2, 2, c=3) == {"a": 2, "b": 2, "c": 3}

    df2 = pg.execute(f"select * from {schema}.{table}")
    assert df2.shape == (1, 4)


if __name__ == "__main__":
    test_cache_db()
