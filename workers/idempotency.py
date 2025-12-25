"""
workers.idempotency
===================

Idempotency utilities for Celery workers.

This module provides Redis-backed helpers to ensure that events
are processed exactly once, even in the presence of retries,
redeliveries, or worker restarts.

Design goals:
- Prevent duplicate processing
- Remain simple and fast
- Be safe under concurrent workers
"""

import redis
from typing import Optional

from core.settings import settings

_redis = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=2,
    decode_responses=True,
)


def already_processed(key: str) -> bool:
    """
    Check whether a given idempotency key has already been processed.

    Parameters
    ----------
    key : str
        Unique idempotency key (e.g. derived from nodeRef + version).

    Returns
    -------
    bool
        True if the key already exists, otherwise False.
    """
    return _redis.exists(key) == 1


def mark_processed(key: str, ttl_seconds: Optional[int] = None) -> None:
    """
    Mark an idempotency key as processed.

    Parameters
    ----------
    key : str
        Unique idempotency key.
    ttl_seconds : int, optional
        Optional TTL in seconds. If provided, the key will
        automatically expire after the given duration.

    Notes
    -----
    TTL is useful when:
    - Reprocessing is allowed after a period
    - Storage growth must be bounded
    """
    if ttl_seconds:
        _redis.setex(key, ttl_seconds, "1")
    else:
        _redis.set(key, "1")
