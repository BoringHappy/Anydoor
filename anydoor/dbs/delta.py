from ..utils.vault import Vault, Secret
import os


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
    def get_table_path(cls, table_name: str) -> str:
        return os.path.join("s3://", cls.bucket(), table_name)
