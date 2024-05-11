import os
from ...utils import Vault


class Token:
    @classmethod
    def init_openai_from_str(cls, token: str, api_base: str = None):
        os.environ["OPENAI_API_KEY"] = token
        if api_base:
            os.environ["OPENAI_API_BASE"] = api_base

    @classmethod
    def init_openai_vault(cls):
        openai_key = Vault().get(os.get_env("OPENAI_VAULT_KEY"))
        cls.init_openai_from_str(
            token=openai_key.OPENAI_API_KEY, api_base=openai_key.OPENAI_API_BASE
        )
