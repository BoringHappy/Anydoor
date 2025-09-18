"""
Messaging service integrations.

This module provides integrations with various messaging platforms including
Feishu (Lark) and QYWX (WeChat Work) for sending notifications and alerts.

Available Services:
    msgqywx: QYWX (WeChat Work) messaging service
    msgfs: Feishu (Lark) messaging service
"""

from .feishu import msgfs
from .qywx import msgqywx

__all__ = ["msgqywx", "msgfs"]
