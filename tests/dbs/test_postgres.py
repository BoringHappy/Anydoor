from anydoor.dbs import Postgres
from types import SimpleNamespace
from sqlalchemy import Engine
import pandas as pd


def test_create_engine():
    engine = Postgres.create_engine(
        secret=SimpleNamespace(
            user="postgres",
            password="<deprecated>",
            host="127.0.0.1",
            port=5432,
        ),
        database="postgres",
        schema="public",
    )

    assert isinstance(engine, Engine)


def test_to_sql():
    schema = "test"
    table = "test_unit"
    pg = Postgres(database="postgres", schema=schema, secret_name="postgres")

    pg.execute(f"drop table if exists {schema}.{table}")

    sample_data = [["Alex", 11, 120.5], ["Bob", 12, 153.7], ["Clarke", 13, 165.0]]
    df = pd.DataFrame(sample_data, columns=["Name", "Age", "Weight"])
    pg.to_sql(
        df=df,
        schema=schema,
        table=table,
        primary_keys=["Name"],
    )
    df2 = pg.execute(f"select * from {schema}.{table}")
    assert df2.shape == (3, 3)
    assert set(df2["Name"].to_list()) == set(["Alex", "Bob", "Clarke"])
    assert set(df2["Age"].to_list()) == set([11, 12, 13])

    increment_data = [
        ["Alex", 11, 120.5, 1, "2012-01-01 00:00:00"],
        ["Bob", 13, 153.7, 2, "2012-01-02 00:00:00"],
        [
            "SmithSmithSmithSmith",
            15,
            165.0,
            3,
            "2012-01-03 00:00:00",
        ],
    ]
    pg.to_sql(
        df=pd.DataFrame(
            increment_data, columns=["Name", "Age", "Weight", "Sort", "create_time"]
        ),
        schema=schema,
        table=table,
        primary_keys=["Name"],
    )
    increment_df = pg.execute(f"select * from {schema}.{table}")
    assert increment_df.shape == (4, 5)
    assert set(increment_df["Name"].to_list()) == set(
        [i[0] for i in increment_data + sample_data]
    )
    assert set(increment_df["Age"].to_list()) == set([11, 13, 15])

    assert pg.is_table_exists(table=table, schema=schema) is True

    sqlal_table = pg.get_table(table=table, schema=schema)
    assert len(sqlal_table.columns) == 5

    pg.truncate(table=table, schema=schema)
    pg.execute(f"drop table  {schema}.{table}")


if __name__ == "__main__":
    test_to_sql()
