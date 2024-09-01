from anydoor.dbs.delta import Delta
from anydoor.utils.vault import Secret
import pandas as pd
import polars as ps
from deltalake import write_deltalake, DeltaTable


def test_delta():
    assert isinstance(Delta.secret(), Secret)
    assert Delta.bucket() == "data-lake"
    assert Delta.get_table_path(table_name="test") == "s3://data-lake/default/test"
    assert (
        Delta.get_table_path(table_name="test", schema_name="lol")
        == "s3://data-lake/lol/test"
    )
    assert Delta.get_table_path("test", schema_name="lol") == "s3://data-lake/lol/test"
    assert Delta.get_table_path("test") == "s3://data-lake/default/test"


def test_delta_with_pandas():
    df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    write_deltalake(
        Delta.get_table_path(table_name="pandas_test"),
        df,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=Delta.secret().json(AWS_S3_ALLOW_UNSAFE_RENAME="true"),
    )
    assert Delta.get_table("pandas_test").to_pandas().equals(df)

    write_deltalake(
        Delta.get_table_path(table_name="pandas_test"),
        df,
        mode="append",
        storage_options=Delta.secret().json(AWS_S3_ALLOW_UNSAFE_RENAME="true"),
    )

    assert not Delta.get_table("pandas_test").to_pandas().equals(df)
    assert (
        Delta.get_table("pandas_test")
        .to_pandas()
        .drop_duplicates()
        .reset_index(drop=True)
        .equals(df.reset_index(drop=True))
    )
    Delta.get_table("polars_test").vacuum()


def test_delta_with_polars():
    df = ps.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    df.write_delta(
        Delta.get_table_path(table_name="polars_test"),
        mode="overwrite",
        storage_options=Delta.secret().json(AWS_S3_ALLOW_UNSAFE_RENAME="true"),
    )
    assert Delta.get_table("polars_test").to_pandas().equals(df.to_pandas())

    df.write_delta(
        Delta.get_table_path(table_name="polars_test"),
        mode="append",
        storage_options=Delta.secret().json(AWS_S3_ALLOW_UNSAFE_RENAME="true"),
    )

    assert not Delta.get_table("polars_test").to_pandas().equals(df.to_pandas())
    assert (
        Delta.get_table("polars_test")
        .to_pandas()
        .drop_duplicates()
        .reset_index(drop=True)
        .equals(df.to_pandas().reset_index(drop=True))
    )

    Delta.get_table("polars_test").vacuum()


if __name__ == "__main__":
    # test_delta()
    # test_delta_with_pandas()
    # test_delta_with_polars()
    ...
