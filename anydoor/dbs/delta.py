from ..utils.vault import Vault, Secret
import os
from deltalake import DataCatalog, DeltaTable


class Delta:
    MINIO_TOKEN = "MINIO_TOKEN_NAME"
    DELTA_LAKE_BUCKET = "DELTA_LAKE_BUCKET"

    @classmethod
    def secret(cls) -> Secret:
        return Vault().get(os.environ[cls.MINIO_TOKEN])

    @classmethod
    def bucket(cls) -> str:
        return os.environ[cls.DELTA_LAKE_BUCKET]

    @classmethod
    def get_table_path(
        cls,
        table_name: str,
        schema_name: str = "default",
    ) -> str:
        return os.path.join("s3://", cls.bucket(), schema_name, table_name)

    @classmethod
    def get_table(
        cls,
        table_name: str,
        schema_name: str = "default",
    ) -> DeltaTable:
        return DeltaTable(
            cls.get_table_path(table_name=table_name, schema_name=schema_name),
            storage_options=cls.secret().json(),
        )
