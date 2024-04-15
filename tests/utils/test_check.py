import os

os.environ["SECRETS_FOLDER"] = os.path.join(os.path.dirname(__file__), ".secret")
os.environ["FERNET_KEY"] = os.path.join(
    os.path.dirname(__file__), ".secret", "frenet_key"
)
from anydoor.utils import check
import pytest


def test_check():
    with pytest.raises(EnvironmentError):

        @check.env(envs=["HHHHHHH"])
        def print_env(): ...

        print_env()
