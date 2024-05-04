# import os

# os.environ["SECRETS_FOLDER"] = os.path.join(os.path.dirname(__file__), ".secret")
# os.environ["FERNET_KEY"] = os.path.join(
#     os.path.dirname(__file__), ".secret", "frenet_key"
# )
from anydoor.utils import Secret
from types import SimpleNamespace
import os


def test_secret():
    test_secret = {
        "api_key": "12345",
        "api_secret": "12345",
    }
    # os.environ["FERNET_KEY"] = Secret.generate()
    Secret.add("test_sec", test_secret)
    result = Secret.get("test_sec")
    assert result.__dict__ == SimpleNamespace(**test_secret).__dict__
    Secret.delete("test_sec")
