"""
consumer.listener
=================

STOMP queue listener for auto-tagging events.

This listener consumes events routed from the Alfresco Event Router,
validates them against the canonical schema, and delegates processing
to Celery workers.

Design principles:
- Fail fast on invalid messages
- ACK only after successful processing
- NO ACK on recoverable failures (broker redelivery)
- No business logic in the listener
"""

import json
import logging
import stomp

from core.schema import RepoEvent
from core.settings import settings
from workers.tasks import auto_tag_node

logger = logging.getLogger(__name__)


class QueueEventListener(stomp.ConnectionListener):
    """
    STOMP listener for queue messages.

    Responsibilities:
    - Deserialize incoming messages
    - Validate schema
    - Filter unsupported events
    - Dispatch work to Celery
    - Control ACK / NO-ACK semantics
    """
    def __init__(self, conn):
        self.conn = conn

    def on_message(self, frame):
        """
        Handle an incoming STOMP message.

        Processing flow:
        1. Parse JSON payload
        2. Validate against RepoEvent schema
        3. Filter unsupported event types
        4. Dispatch to Celery worker
        5. ACK on success, NO ACK on failure

        Parameters
        ----------
        frame : Any
            STOMP frame containing headers and body.
        """
        ack_id = frame.headers["ack"]
        sub_id = frame.headers["subscription"]

        try:
            payload = json.loads(frame.body)

            event = RepoEvent.model_validate(payload)

            if event.eventType != "BINARY_CHANGED":
                self._ack(ack_id, sub_id)
                return

            result = auto_tag_node.apply_async(
                args=[payload]
            ).get(timeout=settings.WORKER_TIMEOUT)

            if result is True:
                self._ack(ack_id, sub_id)
                logger.info("ACKed %s", ack_id)
            else:
                raise RuntimeError("Worker failed")

        except Exception:
            logger.exception("Processing failed – NO ACK")

    def _ack(self, ack_id, sub_id):
        self.conn.send_frame(
            "ACK",
            headers={"id": ack_id, "subscription": sub_id},
        )
