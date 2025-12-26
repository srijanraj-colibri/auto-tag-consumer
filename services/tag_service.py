import logging
import requests
from requests.auth import HTTPBasicAuth
from typing import List

from core.settings import settings

logger = logging.getLogger(__name__)


class AlfrescoTagService:
    """
    Thin Alfresco REST client focused only on tagging.

    Design choice:
    - Tags are added one by one
    - Existing tags are NOT pre-fetched
    - HTTP 409 (Conflict) is treated as success
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

        Idempotent behavior:
        - 201 / 200 → tag created
        - 409 → tag already exists (ignored)
        """
        if not tags:
            return

        node_id = self._extract_node_id(node_ref)

        for tag in tags:
            self._add_tag(node_id, tag)


    def _extract_node_id(self, node_ref: str) -> str:
        """
        workspace://SpacesStore/<uuid> → <uuid>
        """
        return node_ref.split("/")[-1]

    def _add_tag(self, node_id: str, tag: str) -> None:
        url = (
            f"{self.base_url}"
            f"/api/-default-/public/alfresco/versions/1"
            f"/nodes/{node_id}/tags"
        )

        payload = {"tag": tag}

        r = requests.post(
            url,
            json=payload,
            auth=self.auth,
            timeout=10,
        )

        # Success or already exists → safe
        if r.status_code in (200, 201, 409):
            logger.info(
                "Tag applied or already exists",
                extra={"nodeId": node_id, "tag": tag},
            )
            return

        r.raise_for_status()


_tag_service = AlfrescoTagService()


def apply_tags(node_ref: str, tags: List[str]) -> None:
    """
    Public function imported by Celery worker.
    """
    _tag_service.apply_tags(node_ref, tags)
