import csv
import io
import logging
import requests
from typing import Dict, List

from core.settings import settings

logger = logging.getLogger(__name__)

ALFRESCO_BASE = settings.ALFRESCO_BASE_URL.rstrip("/")
AUTH = (settings.ALFRESCO_USERNAME, settings.ALFRESCO_PASSWORD)


def load_rules(csv_node_id: str) -> Dict[str, List[str]]:
    """
    Download and parse rule CSV.

    Supported formats:
      module1/file.png, ECM | Real Estate
      module2/file.png, module2 | ECM2 | Real Estate
    """

    url = (
        f"{ALFRESCO_BASE}"
        f"/api/-default-/public/alfresco/versions/1"
        f"/nodes/{csv_node_id}/content"
    )

    resp = requests.get(url, auth=AUTH, timeout=10)
    resp.raise_for_status()

    rules: Dict[str, List[str]] = {}
    reader = csv.reader(io.StringIO(resp.text))

    for line_no, row in enumerate(reader, start=1):
        if not row:
            continue

        if row[0].strip().startswith("#"):
            continue

        if len(row) < 2:
            logger.warning(
                "Invalid rule row (too few columns)",
                extra={"line": line_no, "row": row},
            )
            continue

        relative_path = _normalize_path(row[0])
        tags = _parse_tags(row[1])

        if not relative_path or not tags:
            logger.warning(
                "Invalid rule row (empty path or tags)",
                extra={"line": line_no, "row": row},
            )
            continue

        rules[relative_path] = tags

    logger.info(
        "Loaded %d rules from CSV",
        len(rules),
        extra={"csvNodeId": csv_node_id},
    )

    return rules



def _normalize_path(path: str) -> str:
    return path.strip().lstrip("/")


def _parse_tags(raw: str) -> List[str]:
    """
    Parse tags from:
      "ECM | Real Estate" → ["ECM", "Real Estate"]
      "module2 | ECM2 | Real Estate" → ["module2", "ECM2", "Real Estate"]
    """
    if not raw:
        return []

    return [
        tag.strip()
        for tag in raw.split("|")
        if tag.strip()
    ]
