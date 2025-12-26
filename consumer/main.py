"""
consumer.main
=============

Application entry point for the queue-based auto-tag consumer.

This service consumes messages from a feature-specific ActiveMQ queue,
delegates processing to Celery workers, and manages lifecycle concerns
such as startup, shutdown, and broker connectivity.
"""

import logging
import signal
import sys
import time
from typing import Optional

import stomp

from core.settings import settings
from core.logging_config import setup_logging
from consumer.listener import QueueEventListener

logger = logging.getLogger("autotag.consumer.main")

_shutdown_requested: bool = False


def _handle_shutdown(signum, frame) -> None:
    """
    Handle termination signals.

    Parameters
    ----------
    signum : int
        Signal number.
    frame : frame
        Current stack frame.
    """
    global _shutdown_requested
    logger.warning("Shutdown signal received", extra={"signal": signum})
    _shutdown_requested = True


def _create_connection() -> stomp.Connection12:
    """
    Create and configure a STOMP connection.

    Returns
    -------
    stomp.Connection12
        Configured STOMP connection.
    """
    return stomp.Connection12(
        [(settings.ACTIVEMQ_HOST, settings.ACTIVEMQ_PORT)],
        heartbeats=(
            settings.ACTIVEMQ_HEARTBEAT_OUT,
            settings.ACTIVEMQ_HEARTBEAT_IN,
        ),
    )


def main() -> None:
    """
    Application entry point.
    """
    setup_logging(settings.LOG_LEVEL)

    logger.info("Starting queue event consumer")

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    conn: Optional[stomp.Connection12] = None

    try:
        conn = _create_connection()

        conn.set_listener(
            "queue-consumer",
            QueueEventListener(conn),
        )

        conn.connect(
            login=settings.ACTIVEMQ_USER,
            passcode=settings.ACTIVEMQ_PASSWORD,
            wait=True,
        )

        logger.info(
            "Connected to ActiveMQ",
            extra={
                "host": settings.ACTIVEMQ_HOST,
                "port": settings.ACTIVEMQ_PORT,
            },
        )

        conn.subscribe(
            destination=settings.ACTIVEMQ_QUEUE,
            id="queue-consumer",
            ack="client-individual",
            headers={
                "activemq.prefetchSize": str(settings.ACTIVEMQ_PREFETCH),
            },
        )

        logger.info(
            "Subscribed to queue",
            extra={
                "queue": settings.ACTIVEMQ_QUEUE,
                "prefetch": settings.ACTIVEMQ_PREFETCH,
            },
        )

        while not _shutdown_requested:
            time.sleep(1)

    except Exception:
        logger.exception("Fatal consumer error")
        sys.exit(1)

    finally:
        if conn and conn.is_connected():
            logger.info("Disconnecting from ActiveMQ")
            conn.disconnect()

        logger.info("Queue consumer stopped cleanly")


if __name__ == "__main__":
    main()
