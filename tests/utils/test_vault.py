import os

from anydoor.utils.vault import Secret, Vault


def test_vault():
    test_secret = {
        "api_key": "12345",
        "api_secret": "12345",
    }
    mount_point = "lcoaltest"
    Vault().add(path="test_sec", secret=test_secret, mount_point=mount_point)
    result = Vault().get("test_sec", mount_point=mount_point)
    assert isinstance(result, Secret)
    assert result.__dict__ == test_secret
    Vault().delete("test_sec", mount_point=mount_point)

    assert Vault.get_mount_point(mount_point) == mount_point
    assert Vault.get_mount_point(None) == "secret"
    os.environ["VAULT_DEFAULT_MOUNT_POINT"] = "lol"
    assert Vault.get_mount_point(None) == "lol"
    assert Vault.get_mount_point(None) != "lol2"
    assert Vault.get_mount_point(None) != "lol2"
