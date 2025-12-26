import logging
import requests
from requests.auth import HTTPBasicAuth
from typing import List

from core.settings import settings

logger = logging.getLogger(__name__)


class AlfrescoTagService:
    """
    Thin Alfresco REST client focused only on tagging.

    Optimized to batch-apply tags in a single request.
    """

    def __init__(self):
        self.base_url = settings.ALFRESCO_BASE_URL.rstrip("/")
        self.auth = HTTPBasicAuth(
            settings.ALFRESCO_USERNAME,
            settings.ALFRESCO_PASSWORD,
        )

    def apply_tags(self, node_ref: str, tags: List[str]) -> None:
        """
        Attach tags to a node.

        - Fetches existing tags
        - Applies only missing tags
        - Sends tags in a single POST request
        """
        if not tags:
            return

        node_id = self._extract_node_id(node_ref)

        existing = set(self._get_existing_tags(node_id))
        to_add = [t for t in tags if t not in existing]

        if not to_add:
            logger.info(
                "All tags already present, skipping",
                extra={"nodeRef": node_ref},
            )
            return

        self._add_tags_bulk(node_id, to_add)


    def _extract_node_id(self, node_ref: str) -> str:
        """
        workspace://SpacesStore/<uuid> → <uuid>
        """
        return node_ref.split("/")[-1]

    def _get_existing_tags(self, node_id: str) -> List[str]:
        url = (
            f"{self.base_url}"
            f"/api/-default-/public/alfresco/versions/1"
            f"/nodes/{node_id}/tags"
        )

        r = requests.get(url, auth=self.auth, timeout=10)
        r.raise_for_status()

        return [
            e["entry"]["tag"]
            for e in r.json().get("list", {}).get("entries", [])
        ]

    def _add_tags_bulk(self, node_id: str, tags: List[str]) -> None:
        """
        Add multiple tags in a single API call.
        """
        url = (
            f"{self.base_url}"
            f"/api/-default-/public/alfresco/versions/1"
            f"/nodes/{node_id}/tags"
        )

        payload = [{"tag": tag} for tag in tags]

        r = requests.post(
            url,
            json=payload,
            auth=self.auth,
            timeout=10,
        )

        # Alfresco returns 200/201 and may return a list payload
        if r.status_code in (200, 201):
            logger.info(
                "Tags applied",
                extra={
                    "nodeId": node_id,
                    "tags": tags,
                },
            )
            return

        # 409 means tags already exist (safe in retries)
        if r.status_code == 409:
            logger.info(
                "Tags already exist (409)",
                extra={"nodeId": node_id},
            )
            return

        r.raise_for_status()


_tag_service = AlfrescoTagService()


def apply_tags(node_ref: str, tags: List[str]) -> None:
    """
    Public API used by worker tasks.
    """
    _tag_service.apply_tags(node_ref, tags)
