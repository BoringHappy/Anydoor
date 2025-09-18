"""
Container environment detection utilities.

This module provides utilities for detecting whether the application
is running inside containerized environments such as Docker or Kubernetes.
"""

import os


class Container:
    """
    Utility class for detecting containerized environments.

    Provides methods to determine if the application is running
    inside Docker containers or Kubernetes pods.
    """

    @classmethod
    def is_docker(cls) -> bool:
        """
        Check if the application is running inside a Docker container.

        Returns:
            bool: True if running inside Docker, False otherwise

        Note:
            This method checks for the presence of /.dockerenv file,
            which is created by Docker in all containers.
        """
        return os.path.exists("/.dockerenv")

    @classmethod
    def is_pod(cls) -> bool:
        """
        Check if the application is running inside a Kubernetes pod.

        Returns:
            bool: True if running inside a Kubernetes pod, False otherwise

        Note:
            This method checks for environment variables that start with
            "KUBERNETES", which are automatically set by Kubernetes.
        """
        return bool([i for i in os.environ.keys() if i.startswith("KUBERNETES")])
