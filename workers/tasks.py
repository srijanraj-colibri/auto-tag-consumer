import logging
from pydantic import ValidationError

from workers.celery_app import celery_app
from workers.idempotency import already_processed, mark_processed

from core.schema import RepoEvent

from services.rule_discovery_service import find_nearest_rule_csv
from services.rule_loader_service import load_rules
from services.rule_matcher import match_rule
from services.tag_service import apply_tags

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=10,
    retry_kwargs={"max_retries": 3},
)
def auto_tag_node(self, payload: dict) -> bool:
    """
    Rule-based auto-tagging worker task.
    Schema-validated using core.schema.RepoEvent
    """

    try:
        event = RepoEvent.model_validate(payload)

    except ValidationError as e:
        logger.error(
            "Invalid RepoEvent schema received in worker",
            extra={"payload": payload},
            exc_info=e,
        )
        return True  # ACK upstream (do not retry)

    key = f"{event.nodeRef}:{event.modifiedAt}"

    if already_processed(key):
        logger.info("Skipping duplicate event", extra={"key": key})
        return True

    if not event.path:
        logger.info(
            "Event has no path, skipping",
            extra={"nodeRef": event.nodeRef},
        )
        mark_processed(key)
        return True

    logger.info(
        "Starting auto-tag task",
        extra={
            "nodeRef": event.nodeRef,
            "path": event.path,
            "eventType": event.eventType,
        },
    )

    logger.info("finding nearest rule csv file")
    csv_node_id = find_nearest_rule_csv(event.path)
    
    if not csv_node_id:
        logger.info("No rule CSV found", extra={"path": event.path})
        mark_processed(key)
        return True
    
    logger.info("found rule csv", extra={"nodeID": csv_node_id})

    rules = load_rules(csv_node_id)

    if not rules:
        logger.info(
            "Rule CSV empty or invalid",
            extra={"csvNodeId": csv_node_id},
        )
        mark_processed(key)
        return True

    tags = match_rule(event.path, rules)    

    if not tags:
        logger.info("No matching rules", extra={"path": event.path})
        mark_processed(key)
        return True

    apply_tags(event.nodeRef, tags)

    logger.info(
        "Auto-tagging completed",
        extra={
            "nodeRef": event.nodeRef,
            "tags": tags,
        },
    )

    mark_processed(key)
    return True
