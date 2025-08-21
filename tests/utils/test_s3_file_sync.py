from pathlib import Path

import pytest

from anydoor.utils.s3_file_sync import S3FileSync


@pytest.mark.skip(reason="No S3")
def test_s3_file_sync():
    local_test_path = Path(__file__).parent / "tmp"
    local_test_path.mkdir(parents=True, exist_ok=True)
    s3_path = "s3://test-bucket/file_sync_test"

    with S3FileSync(s3_path, local_test_path):
        assert local_test_path.exists()
        with open(local_test_path / "test.txt", "w") as f:
            f.write("test")
    assert not local_test_path.exists()

    with S3FileSync(s3_path, local_test_path, clean=False):
        assert (local_test_path / "test.txt").exists()
    assert local_test_path.exists()

    with S3FileSync(s3_path, local_test_path, clean=True):
        assert (local_test_path / "test.txt").exists()
    assert not local_test_path.exists()
