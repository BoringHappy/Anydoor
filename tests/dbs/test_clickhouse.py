from anydoor.dbs.clickhouse import Clickhouse
import pandas as pd


def test_to_sql():
    schema = "default"
    table = "test_unit"
    ck = Clickhouse(database="default", schema=schema, secret_name="clickhouse")
    ck.execute(f"drop table if exists {schema}.{table}")

    sample_data = [["Alex", 11, 120.5], ["Bob", 12, 153.7], ["Clarke", 13, 165.0]]
    df = pd.DataFrame(sample_data, columns=["Name", "Age", "Weight"])
    ck.ensure_table(
        table=table,
        schema=schema,
        dtype=ck.get_df_dtypes(df=df),
        primary_keys=["Name"],
    )
    ck.to_sql(
        df=df,
        schema=schema,
        table=table,
    )
    df2 = ck.execute(f"select * from {schema}.{table}")
    assert df2.shape == (3, 3)
    assert set(df2["Name"].to_list()) == set(["Alex", "Bob", "Clarke"])
    assert set(df2["Age"].to_list()) == set([11, 12, 13])

    assert ck.is_table_exists(table=table, schema=schema) is True

    sqlal_table = ck.get_table(table=table, schema=schema)
    assert len(sqlal_table.columns) == 3

    ck.truncate(table=table, schema=schema)
    ck.execute(f"drop table  {schema}.{table}")
