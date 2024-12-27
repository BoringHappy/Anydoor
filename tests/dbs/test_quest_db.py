import pandas as pd
from anydoor.dbs import QuestDB, Postgres
import time

def test_to_sql():
    questdb_writer = QuestDB()
    table_name = "unttest_dfs"
    questdb_writer.execute(sql=f"drop table if exists {table_name}")
    df = pd.DataFrame(
        {
            "id": pd.Categorical(["toronto1", "paris3"]),
            "temperature": [20.0, 21.0],
            "humidity": [0.5, 0.6],
            "timestamp": pd.to_datetime(["2021-01-01", "2021-01-02"]),
        }
    )

    questdb_writer.to_sql(df=df, table_name=table_name, at="timestamp")
    time.sleep(5)
    df2 = questdb_writer.execute(sql=f"select * from {table_name}")
    questdb_writer.execute(sql=f"drop table if exists {table_name}")

    print(df2)
    assert len(df2) == 2


if __name__ == "__main__":
    test_to_sql()
