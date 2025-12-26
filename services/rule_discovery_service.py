import logging
import requests
from typing import Optional

from core.settings import settings
from services.alfresco_path_service import resolve_path

logger = logging.getLogger(__name__)

ALFRESCO_BASE = settings.ALFRESCO_BASE_URL.rstrip("/")
AUTH = (settings.ALFRESCO_USERNAME, settings.ALFRESCO_PASSWORD)

RULE_ROOT = "/RULE_BASED_TAGS"


def find_nearest_rule_csv(event_path: str) -> Optional[str]:
    """
    Progressive rule discovery.

    For Example:
      /Company Home/Courses/Real Estate/Module1/file.png

    Checks:
      RULE_BASED_TAGS/Courses
      RULE_BASED_TAGS/Courses/Real Estate

    First *_tags.csv found wins.
    """

    content_parts = _extract_content_parts(event_path)
    if not content_parts:
        return None

    # progressively descend
    for depth in range(1, len(content_parts)):
        rule_folder_path = f"{RULE_ROOT}/" + "/".join(content_parts[:depth])
        rule_folder_id = resolve_path(rule_folder_path)
        
        if not rule_folder_id:
            continue

        csv_id = _find_first_rule_csv(rule_folder_id)
        if csv_id:
            logger.info(
                "Rule CSV discovered",
                extra={
                    "ruleFolder": rule_folder_path,
                    "csvNodeId": csv_id,
                },
            )
            return csv_id

    return None



def _extract_content_parts(event_path: str) -> list[str]:
    """
    From:
      /Company Home/Courses/Real Estate/Module1/file.png

    Return:
      ["Courses", "Real Estate", "Module1"]
    """

    parts = [p for p in event_path.strip("/").split("/") if p]

    # remove Company Home
    if parts and parts[0] == "Company Home":
        parts = parts[1:]

    # remove filename
    if parts and "." in parts[-1]:
        parts = parts[:-1]

    return parts


def _find_first_rule_csv(folder_id: str) -> Optional[str]:
    """
    Find first *_tags.csv inside a folder.
    """

    url = (
        f"{ALFRESCO_BASE}"
        f"/api/-default-/public/alfresco/versions/1"
        f"/nodes/{folder_id}/children"
        f"?maxItems=1000"
    )

    resp = requests.get(url, auth=AUTH, timeout=10)
    resp.raise_for_status()

    for entry in resp.json().get("list", {}).get("entries", []):
        name = entry["entry"]["name"]
        if name.lower().endswith("_tags.csv"):
            return entry["entry"]["id"]

    return None
