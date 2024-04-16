import os


class Token:
    @classmethod
    def init_openai_from_str(cls, token: str, api_base: str = None):
        os.environ["OPENAI_API_KEY"] = token
        if api_base:
            os.environ["OPENAI_API_BASE"] = api_base
