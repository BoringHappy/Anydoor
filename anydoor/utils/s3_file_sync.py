import os
import shutil
from contextlib import contextmanager

from cloudpathlib import S3Client
from loguru import logger

from .vault import Vault


@contextmanager
def S3FileSync(s3_path, local_path, secret_name: str = None, clean: bool = True):
    """
    Syncs a local path with an s3 path.
    Args:
        s3_path: The s3 path to sync to.
        local_path: The local path to sync from.
        secret_name: The name of the secret to use for the s3 client.
        clean: Whether to clean the local path after syncing.
    """
    secret = Vault().get(secret_name or os.environ["S3_TOKEN_NAME"])
    client = S3Client(
        aws_access_key_id=secret.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secret.AWS_SECRET_ACCESS_KEY,
        endpoint_url=secret.AWS_ENDPOINT,
    )
    cloud_path = client.CloudPath(s3_path)
    cloud_path.mkdir(parents=True, exist_ok=True)
    if cloud_path.exists():
        logger.info("Downloading from s3...")
        cloud_path.download_to(local_path)
    try:
        yield
    finally:
        logger.info("Uploading to s3...")
        cloud_path.mkdir(parents=True, exist_ok=True)
        cloud_path.upload_from(local_path)
        if clean:
            shutil.rmtree(local_path)
