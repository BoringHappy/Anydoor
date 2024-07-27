from anydoor.dbs.delta import Delta
from anydoor.utils.vault import Secret


def test_delta():
    assert isinstance(Delta.secret(), Secret)
    assert Delta.bucket() == "data-lake"
    assert Delta.get_table_path("test") == "s3://data-lake/test"
