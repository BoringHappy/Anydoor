import os

from deltalake import DeltaTable

from ..utils.vault import Secret, Vault


class Delta:
    S3_TOKEN = "S3_TOKEN_NAME"
    DELTA_LAKE_BUCKET = "DELTA_LAKE_BUCKET"

    def __init__(
        self,
        table_name: str,
        schema_name: str = "default",
        catalog_name: str = "default",
        bucket: str = None,
        storage_options: dict = None,
        secret_name: str = None,
    ):
        self.table_name = table_name
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.bucket = bucket
        self.storage_options = self.get_storage_options(storage_options, secret_name)

    def get_storage_options(self, storage_options, secret_name):
        if storage_options:
            return storage_options
        if secret_name:
            return Vault().get(secret_name).json()
        return Vault().get(os.environ[self.S3_TOKEN]).json()

    @property
    def path(self):
        return os.path.join(
            "s3://",
            self.bucket or os.environ[self.DELTA_LAKE_BUCKET],
            self.catalog_name,
            self.schema_name,
            self.table_name,
        )

    @property
    def table(self):
        return DeltaTable(self.path, storage_options=self.storage_options)
