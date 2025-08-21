from anydoor.dbs.sqlite import Sqlite

import pandas as pd


def test_to_sql():
    schema = "main"
    table = "test_unit"
    pg = Sqlite(db_path=":memory:")

    pg.execute(f"drop table if exists {schema}.{table}")

    sample_data = [["Alex", 11, 120.5], ["Bob", 12, 153.7], ["Clarke", 13, 165.0]]
    df = pd.DataFrame(sample_data, columns=["Name", "Age", "Weight"])
    pg.to_sql(
        df=df,
        schema=schema,
        table=table,
    )
    df2 = pg.execute(f"select * from {schema}.{table}")
    assert df2.shape == (3, 3)
    assert set(df2["Name"].to_list()) == set(["Alex", "Bob", "Clarke"])
    assert set(df2["Age"].to_list()) == set([11, 12, 13])

    assert pg.is_table_exists(table=table, schema=schema) is True

    sqlal_table = pg.get_table(table=table, schema=schema)
    assert len(sqlal_table.columns) == 3

    pg.execute(f"drop table  {schema}.{table}")


if __name__ == "__main__":
    test_to_sql()
