"""
workers.celery_app
==================

Celery application bootstrap for Alfresco AI consumers.

This module initializes and configures the Celery application used
by worker processes to execute asynchronous tasks such as auto-tagging,
metadata extraction, and vector generation.

Design principles:
- Explicit configuration
- Environment-driven settings
- JSON-only task serialization
- Safe defaults for distributed execution
"""

from celery import Celery

from core.settings import settings

celery_app = Celery(
    "alfresco_ai",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    timezone="UTC",
    enable_utc=True,

    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

celery_app.autodiscover_tasks(
    [
        "workers",
    ]
)
