"""
core.schema
===========

Canonical event schema for messages consumed from ActiveMQ queues.

This module defines the normalized repository event model shared
between the Alfresco Event Router and downstream consumers.

Design goals:
- Strict but flexible validation
- Backward compatibility with evolving producers
- Clear separation between transport schema and business logic
"""

from typing import Optional, Union
from pydantic import BaseModel, field_validator


class RepoEvent(BaseModel):
    # -------- Core envelope --------
    schemaVersion: int
    eventType: str
    timestamp: int

    # -------- Node identity --------
    nodeRef: str
    storeRef: str
    parentNodeRef: Optional[str] = None

    # -------- Path & naming --------
    name: Optional[str] = None
    path: Optional[str] = None

    # -------- Content metadata --------
    mimeType: Optional[str] = None
    size: Optional[int] = None
    encoding: Optional[str] = None
    versionLabel: Optional[str] = None

    # -------- Audit metadata --------
    creator: Optional[str] = None
    modifier: Optional[str] = None

    # Accept BOTH epoch millis and ISO strings
    createdAt: Optional[Union[int, str]] = None
    modifiedAt: Optional[Union[int, str]] = None

    # -------- Type info --------
    nodeType: Optional[str] = None


    # -------- Normalization (optional but clean) --------
    @field_validator("createdAt", "modifiedAt", mode="before")
    @classmethod
    def normalize_timestamps(cls, v):
        """
        Accept epoch millis or ISO strings.
        Keep as-is; consumers can normalize further.
        """
        if v is None:
            return None
        if isinstance(v, (int, str)):
            return v
        raise ValueError("Invalid timestamp format")

    class Config:
        extra = "ignore"
