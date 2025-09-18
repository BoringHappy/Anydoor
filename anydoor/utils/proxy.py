"""
Proxy configuration management utilities.

This module provides utilities for managing HTTP/HTTPS proxy configurations
retrieved from Vault secrets with caching support.
"""

import os
from functools import lru_cache

from .vault import Vault


@lru_cache
def get_proxy():
    """
    Get proxy configuration from Vault with caching.

    Retrieves proxy configuration from Vault using the secret name specified
    in VAULT_PROXY_PATH environment variable or defaults to "local_proxy".
    Results are cached using lru_cache for performance.

    Returns:
        dict: Proxy configuration dictionary from Vault

    Environment Variables:
        VAULT_PROXY_PATH: Name of the Vault secret containing proxy configuration

    Raises:
        Exception: If the proxy secret cannot be retrieved from Vault
    """
    return Vault().get("local_proxy" or os.environ["VAULT_PROXY_PATH"]).json()


def Proxy():
    """
    Get HTTP/HTTPS proxy configuration for requests.

    Filters the proxy configuration to only include HTTP and HTTPS
    proxy settings suitable for use with requests library.

    Returns:
        dict: Dictionary containing only 'http' and 'https' proxy settings

    Example:
        >>> proxy_config = Proxy()
        >>> # Returns: {'http': 'http://proxy:8080', 'https': 'http://proxy:8080'}
    """
    return {k: v for k, v in get_proxy().items() if k in ["http", "https"]}
