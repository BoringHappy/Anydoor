from anydoor.dbs.delta import Delta
from anydoor.utils.vault import Secret


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
