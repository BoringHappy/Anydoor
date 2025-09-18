"""
S3 file synchronization utilities.

This module provides a context manager for synchronizing local directories
with S3 storage, with automatic upload/download and cleanup capabilities.
"""

import os
import shutil
from contextlib import contextmanager

from cloudpathlib import S3Client
from loguru import logger

from .vault import Vault


@contextmanager
def S3FileSync(s3_path, local_path, secret_name: str = None, clean: bool = True):
    """
    Context manager for synchronizing local directories with S3 storage.

    This context manager handles bidirectional synchronization between local
    directories and S3 paths. It downloads existing S3 content before entering
    the context and uploads local changes when exiting the context.

    Args:
        s3_path (str): The S3 path to synchronize with (e.g., "s3://bucket/path/")
        local_path (str): The local directory path to synchronize
        secret_name (str, optional): Name of the Vault secret containing S3 credentials.
                                   If not provided, uses S3_TOKEN_NAME environment variable.
        clean (bool): Whether to remove the local directory after uploading (default: True)

    Environment Variables:
        S3_TOKEN_NAME: Name of the Vault secret containing S3 credentials (if secret_name not provided)

    Vault Secret Format:
        The secret should contain the following keys:
        - AWS_ACCESS_KEY_ID: AWS access key
        - AWS_SECRET_ACCESS_KEY: AWS secret key
        - AWS_ENDPOINT: S3 endpoint URL

    Yields:
        None: The context manager yields control to the caller

    Example:
        >>> with S3FileSync("s3://my-bucket/data/", "/tmp/local_data/"):
        ...     # Work with local files
        ...     with open("/tmp/local_data/file.txt", "w") as f:
        ...         f.write("Hello World")
        >>> # Files are automatically uploaded to S3 and local directory cleaned up
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
