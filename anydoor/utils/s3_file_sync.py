from .vault import Vault
import os
from cloudpathlib import S3Client
import shutil
from contextlib import contextmanager
from functools import lru_cache


@contextmanager
def S3FileSync(s3_path, local_path, secret_name: str = None, clean: bool = True):
    secret = Vault().get(secret_name or os.environ["MINIO_TOKEN_NAME"])
    client = S3Client(
        aws_access_key_id=secret.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secret.AWS_SECRET_ACCESS_KEY,
        endpoint_url=secret.AWS_ENDPOINT,
    )
    cloud_path = client.CloudPath(s3_path)
    print("Downloading from s3...")
    cloud_path.download_to(local_path)
    try:
        yield
    finally:
        print("Uploading to s3...")
        cloud_path.upload_from(local_path)
        if clean:
            shutil.rmtree(local_path)


@lru_cache
def get_s3_client(secret_name: str = None):
    secret = Vault().get(secret_name or os.environ["MINIO_TOKEN_NAME"])
    return S3Client(
        aws_access_key_id=secret.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secret.AWS_SECRET_ACCESS_KEY,
        endpoint_url=secret.AWS_ENDPOINT,
    )
