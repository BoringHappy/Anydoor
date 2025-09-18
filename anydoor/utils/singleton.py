"""
Thread-safe singleton pattern implementation.

This module provides a thread-safe singleton metaclass that ensures only
one instance of a class exists across the entire application lifecycle.
"""

import threading


class SingletonType(type):
    """
    Thread-safe singleton metaclass.

    This metaclass ensures that only one instance of a class is created
    throughout the application lifecycle. The implementation uses double-checked
    locking to provide thread safety while maintaining good performance.

    Example:
        >>> class MyClass(metaclass=SingletonType):
        ...     def __init__(self):
        ...         self.value = 42
        >>> instance1 = MyClass()
        >>> instance2 = MyClass()
        >>> instance1 is instance2  # True - same instance
    """

    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        """
        Create or return the singleton instance of the class.

        Uses double-checked locking pattern to ensure thread safety
        while avoiding unnecessary locking overhead.

        Args:
            *args: Positional arguments for class constructor
            **kwargs: Keyword arguments for class constructor

        Returns:
            The singleton instance of the class
        """
        if not hasattr(cls, "_instance"):
            with SingletonType._instance_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instance
