import pandas as pd
import polars as ps
import pytest
from deltalake import write_deltalake

from anydoor.dbs.delta import Delta


@pytest.mark.skip(reason="No Delta")
def test_delta_with_pandas():
    df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    dt = Delta(table_name="pandas_test")
    write_deltalake(
        dt.path,
        df,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=dt.storage_options,
    )
    assert dt.table.to_pandas().equals(df)

    write_deltalake(
        dt.path,
        df,
        mode="append",
        storage_options=dt.storage_options,
    )

    assert not dt.table.to_pandas().equals(df)
    assert (
        dt.table.to_pandas()
        .drop_duplicates()
        .reset_index(drop=True)
        .equals(df.reset_index(drop=True))
    )
    dt.table.vacuum()


@pytest.mark.skip(reason="No Delta")
def test_delta_with_polars():
    df = ps.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    dt = Delta(table_name="polars_test")

    df.write_delta(
        dt.path,
        mode="overwrite",
        storage_options=dt.storage_options,
    )
    assert dt.table.to_pandas().equals(df.to_pandas())

    df.write_delta(
        dt.path,
        mode="append",
        storage_options=dt.storage_options,
    )

    assert not dt.table.to_pandas().equals(df.to_pandas())
    assert (
        dt.table.to_pandas()
        .drop_duplicates()
        .reset_index(drop=True)
        .equals(df.to_pandas().reset_index(drop=True))
    )

    dt.table.vacuum()
