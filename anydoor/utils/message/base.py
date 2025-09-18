"""
Base messaging service implementation.

This module provides the base class for messaging services with Vault integration
and singleton pattern support.
"""

import os

from ..singleton import SingletonType
from ..vault import Secret, Vault


class UserError(Exception):
    """
    Custom exception for user-related errors in messaging services.

    This exception is raised when there are issues with user-specific
    messaging operations or configurations.
    """

    pass


class BaseMsg(metaclass=SingletonType):
    """
    Base class for messaging services with Vault integration.

    This abstract base class provides common functionality for messaging
    services including Vault secret management and singleton pattern support.
    Subclasses should implement the send method for specific messaging platforms.

    Attributes:
        PASSWD_NAME_ENV (str): Environment variable name for password/secret name
        secret_name (str): Default secret name for Vault lookup
        secret_instance (Secret): Cached secret instance
    """

    PASSWD_NAME_ENV = None
    secret_name: str = None

    def __init__(self, secret: Secret = None):
        """
        Initialize the messaging service with optional secret instance.

        Args:
            secret (Secret, optional): Pre-loaded secret instance. If not provided,
                                     secrets will be loaded from Vault as needed.
        """
        self.secret_instance = secret

    @property
    def secret(self):
        """
        Get the secret configuration for the messaging service.

        Returns the cached secret instance or loads it from Vault using
        the environment variable or default secret name.

        Returns:
            Secret: Secret object containing messaging service credentials
        """
        return self.secret_instance or Vault().get(
            os.environ.get(self.PASSWD_NAME_ENV, self.secret_name)
        )

    def send(self, message: str, msgtype: str = "text", raise_exception=False):
        """
        Send a message using the messaging service.

        This is an abstract method that should be implemented by subclasses
        to provide specific messaging functionality.

        Args:
            message (str): The message content to send
            msgtype (str): Type of message (default: "text")
            raise_exception (bool): Whether to raise exceptions on failure

        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement the send method")

    @classmethod
    def cls_send(cls, *args, **kwargs):
        """
        Class method to send a message using the singleton instance.

        This convenience method creates or retrieves the singleton instance
        and calls the send method on it.

        Args:
            *args: Positional arguments passed to the send method
            **kwargs: Keyword arguments passed to the send method

        Returns:
            Result from the send method of the singleton instance
        """
        return cls().send(*args, **kwargs)
