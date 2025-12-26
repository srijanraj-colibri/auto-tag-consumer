import logging
import requests
from typing import Optional

from core.settings import settings

logger = logging.getLogger(__name__)

ALFRESCO_BASE = settings.ALFRESCO_BASE_URL.rstrip("/")
AUTH = (settings.ALFRESCO_USERNAME, settings.ALFRESCO_PASSWORD)

COMPANY_HOME_ID = "-root-"


def resolve_path(path: str) -> Optional[str]:
    """
    Resolve repository path to nodeId by walking the folder tree.

    Works with:
    - spaces
    - Community Edition
    - Nodes v1 API
    """

    if not path or path == "/":
        return COMPANY_HOME_ID

    parts = [p for p in path.strip("/").split("/") if p]
    current_node_id = COMPANY_HOME_ID

    logger.info("Resolving path", extra={"path": path})

    for part in parts:
        child_id = _find_child_by_name(current_node_id, part)
        if not child_id:
            logger.info(
                "Path segment not found",
                extra={
                    "parentNodeId": current_node_id,
                    "segment": part,
                },
            )
            return None

        current_node_id = child_id

    return current_node_id


def _find_child_by_name(parent_node_id: str, name: str) -> Optional[str]:
    """
    Find direct child by name under a parent node
    by listing children and filtering client-side.
    """

    url = (
        f"{ALFRESCO_BASE}"
        f"/api/-default-/public/alfresco/versions/1"
        f"/nodes/{parent_node_id}/children"
        f"?maxItems=1000"
    )

    resp = requests.get(url, auth=AUTH, timeout=10)
    resp.raise_for_status()

    entries = resp.json().get("list", {}).get("entries", [])

    for entry in entries:
        if entry["entry"]["name"] == name:
            return entry["entry"]["id"]

    return None
