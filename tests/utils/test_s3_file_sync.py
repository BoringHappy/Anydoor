import os

import pytest

from anydoor.utils.s3_file_sync import S3FileSync


@pytest.mark.skip(reason="Call API")
def test_s3_file_sync():
    local_test_path = os.path.join(os.path.dirname(__file__), "tmp")
    s3_path = "s3://test-bucket/file_sync_test"

    with S3FileSync(s3_path, local_test_path):
        assert os.path.exists(local_test_path)
        with open(os.path.join(local_test_path, "test.txt"), "w") as f:
            f.write("test")
    assert not os.path.exists(local_test_path)

    with S3FileSync(s3_path, local_test_path, clean=False):
        assert os.path.exists(os.path.join(local_test_path, "test.txt"))
    assert os.path.exists(local_test_path)

    with S3FileSync(s3_path, local_test_path, clean=True):
        assert os.path.exists(os.path.join(local_test_path, "test.txt"))
    assert not os.path.exists(local_test_path)
