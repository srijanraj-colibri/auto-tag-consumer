"""
core.settings
=============

Application configuration for the auto-tag consumer service.

All configuration values are loaded from environment variables
using Pydantic Settings. This module defines all runtime parameters
required for ActiveMQ consumption, Celery execution, Redis usage,
and Alfresco API access.

Design principles (current state):
- Explicit configuration
- Environment-first (12-factor app)

NOTE:
Fail-fast validation is intentionally relaxed (Optional fields)
to allow worker-only processes to start without ActiveMQ settings.
"""

from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Consumer service settings.

    All fields map directly to environment variables.

    NOTE:
    Some fields are optional for now to support worker-only execution.
    These will be made mandatory once settings are split by role.
    """

    # ------------------------------------------------------------------
    # ActiveMQ (queue consumer)
    # ------------------------------------------------------------------
    ACTIVEMQ_HOST: Optional[str] = Field(
        default=None,
        description="ActiveMQ broker host",
    )
    ACTIVEMQ_PORT: int = Field(
        default=61613,
        description="ActiveMQ STOMP port",
    )
    ACTIVEMQ_USER: Optional[str] = Field(
        default=None,
        description="ActiveMQ username",
    )
    ACTIVEMQ_PASSWORD: Optional[str] = Field(
        default=None,
        description="ActiveMQ password",
        repr=False,
    )
    ACTIVEMQ_QUEUE: Optional[str] = Field(
        default=None,
        description="Queue to consume auto-tag events from",
    )

    ACTIVEMQ_PREFETCH: int = Field(
        default=1,
        ge=1,
        description="ActiveMQ prefetch size",
    )
    ACTIVEMQ_HEARTBEAT_OUT: int = Field(
        default=10_000,
        description="Outgoing STOMP heartbeat (ms)",
    )
    ACTIVEMQ_HEARTBEAT_IN: int = Field(
        default=10_000,
        description="Incoming STOMP heartbeat (ms)",
    )

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------
    REDIS_HOST: Optional[str] = Field(
        default=None,
        description="Redis host for Celery backend",
    )
    REDIS_PORT: int = Field(
        default=6379,
        description="Redis port",
    )

    # ------------------------------------------------------------------
    # Celery
    # ------------------------------------------------------------------
    CELERY_BROKER_URL: Optional[str] = Field(
        default=None,
        description="Celery broker URL",
    )
    CELERY_RESULT_BACKEND: Optional[str] = Field(
        default=None,
        description="Celery result backend URL",
    )

    # ------------------------------------------------------------------
    # Worker behavior
    # ------------------------------------------------------------------
    WORKER_TIMEOUT: int = Field(
        default=600,
        ge=1,
        description="Maximum time (seconds) to wait for worker result",
    )

    # ------------------------------------------------------------------
    # Alfresco API
    # ------------------------------------------------------------------
    ALFRESCO_BASE_URL: Optional[str] = Field(
        default=None,
        description="Base URL of Alfresco repository",
    )
    ALFRESCO_USERNAME: Optional[str] = Field(
        default=None,
        description="Alfresco service username",
    )
    ALFRESCO_PASSWORD: Optional[str] = Field(
        default=None,
        description="Alfresco service password",
        repr=False,
    )

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Application log level",
    )

    # ------------------------------------------------------------------
    # Pydantic configuration
    # ------------------------------------------------------------------
    model_config = {
        "extra": "ignore",
        "case_sensitive": True,
    }


# Singleton settings instance
settings = Settings()
