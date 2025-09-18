import os
from copy import deepcopy
from types import SimpleNamespace
from typing import Dict

import hvac
from tenacity import retry, stop_after_attempt, wait_exponential

from .singleton import SingletonType


class Secret(SimpleNamespace):
    """
    Simple namespace object for storing secret data from Vault.

    Provides a convenient interface for accessing secret values with
    JSON serialization capabilities and safe attribute access.

    Attributes:
        All attributes are dynamically set based on the secret data from Vault.
    """

    def __getattr__(self, name: str):
        """
        Safe attribute access that returns None for missing attributes.

        Args:
            name (str): Attribute name to access

        Returns:
            Attribute value or None if not found
        """
        if name in self.__dict__:
            return getattr(self, name)
        else:
            return None

    def __bool__(self):
        """
        Boolean evaluation based on whether any attributes exist.

        Returns:
            bool: True if any attributes are set, False otherwise
        """
        return bool(vars(self))

    def json(self, **kwargs):
        """
        Convert secret data to JSON-serializable dictionary.

        Args:
            **kwargs: Additional key-value pairs to include

        Returns:
            dict: JSON-serializable dictionary with secret data
        """
        json_str = deepcopy(self.__dict__)
        json_str.update(kwargs)
        return json_str


class Vault(metaclass=SingletonType):
    """
    HashiCorp Vault client with singleton pattern for secure secret management.

    Provides a thread-safe singleton interface to HashiCorp Vault for storing
    and retrieving secrets. Automatically handles Vault unsealing and authentication.

    Environment Variables Required:
        VAULT_ADDR: Vault server address
        VAULT_TOKEN: Vault authentication token
        VAULT_UNSEAL_KEY: Key to unseal Vault (if sealed)
        VAULT_DEFAULT_MOUNT_POINT: Default mount point for secrets (defaults to "secret")

    Attributes:
        client: hvac.Client instance for Vault operations
    """

    def __init__(self, url=None, token=None):
        """
        Initialize Vault client with authentication and unsealing.

        Args:
            url (str, optional): Vault server URL (uses VAULT_ADDR env var if not provided)
            token (str, optional): Vault token (uses VAULT_TOKEN env var if not provided)

        Raises:
            AssertionError: If Vault is sealed after unsealing attempt or authentication fails
        """
        self.client = hvac.Client(url=url, token=token)
        if self.client.sys.is_sealed():
            self.client.sys.submit_unseal_key(key=os.getenv("VAULT_UNSEAL_KEY"))
            assert not self.client.sys.is_sealed()

        assert self.client.is_authenticated()
        assert self.client.sys.is_initialized()

    @staticmethod
    def get_mount_point(mount_point):
        """
        Get mount point for secret operations.

        Args:
            mount_point (str, optional): Specific mount point to use

        Returns:
            str: Mount point to use (defaults to VAULT_DEFAULT_MOUNT_POINT env var or "secret")
        """
        return mount_point or os.getenv("VAULT_DEFAULT_MOUNT_POINT", "secret")

    def add(
        self,
        path: str,
        secret: Dict[str, str],
        mount_point: str = None,
    ):
        """
        Store secret in Vault.

        Args:
            path (str): Secret path in Vault
            secret (Dict[str, str]): Secret data as key-value pairs
            mount_point (str, optional): Mount point to use
        """
        self.client.secrets.kv.v2.create_or_update_secret(
            path=path, secret=secret, mount_point=self.get_mount_point(mount_point)
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=20),
    )
    def get(self, path: str, mount_point: str = None) -> Secret:
        """
        Retrieve secret from Vault with retry logic.

        Args:
            path (str): Secret path in Vault
            mount_point (str, optional): Mount point to use

        Returns:
            Secret: Secret object with retrieved data

        Raises:
            Exception: If path is not a string or retrieval fails after retries
        """
        assert isinstance(path, str), Exception(f"Non exists {path}")
        return Secret(
            **self.client.secrets.kv.read_secret(
                path=path,
                mount_point=self.get_mount_point(mount_point),
                raise_on_deleted_version=False,
            )["data"]["data"]
        )

    def delete(self, path: str, mount_point=None):
        """
        Delete secret and all its versions from Vault.

        Args:
            path (str): Secret path in Vault
            mount_point (str, optional): Mount point to use

        Returns:
            Response from Vault delete operation
        """
        return self.client.secrets.kv.delete_metadata_and_all_versions(
            path=path,
            mount_point=self.get_mount_point(mount_point),
        )
