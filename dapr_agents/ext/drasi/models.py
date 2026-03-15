"""
Pydantic models for Drasi change and control events.

These models represent exactly what the Drasi Agent Router publishes to Dapr Pub/Sub.
They are vendored here (not imported from drasi-reaction-sdk) because:
- The SDK is alpha and not on PyPI
- The models are stable (~60 lines, generated from YAML schemas)
- Vendoring avoids an alpha dependency

Source: drasi-platform/reactions/sdk/python/drasi/reaction/models/

Two kinds of events:
1. ChangeEvent (packed): the router sends the entire change as one message
2. DrasiChangeNotification (unpacked): the router sends one message per changed record

Choose based on how your router is configured (format: packed or unpacked).
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ResultEvent(BaseModel):
    """Base class for all events that come from Drasi queries."""
    kind: str
    queryId: str = Field(description="The Drasi query that produced this event")
    sequence: int = Field(description="Monotonically increasing event number per query")
    sourceTimeMs: int = Field(description="When the database change happened (Unix ms)")
    metadata: Optional[Dict[str, Any]] = None


class UpdatePayload(BaseModel):
    """Holds a database record before and after it was changed."""
    before: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The record's values before the update"
    )
    after: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The record's values after the update"
    )


class ChangeEvent(ResultEvent):
    """
    A packed change event - contains ALL changes from one query result update.

    Use this when the router is configured with format: packed.
    One event contains all rows that were added, updated, or deleted at once.

    Example:
        event.addedResults  -> list of new rows matching the query
        event.updatedResults -> list of (before, after) pairs for changed rows
        event.deletedResults -> list of rows that no longer match the query
    """
    kind: Literal["change"] = "change"
    addedResults: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Rows that newly match the query (trigger the agent)"
    )
    updatedResults: List[UpdatePayload] = Field(
        default_factory=list,
        description="Rows that changed while still matching the query"
    )
    deletedResults: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Rows that no longer match the query"
    )


class ControlSignalKind(str, Enum):
    """Lifecycle signals from the Drasi query engine."""
    BOOTSTRAP_STARTED = "bootstrapStarted"      # Query is loading initial data
    BOOTSTRAP_COMPLETED = "bootstrapCompleted"  # Initial data loaded, now watching live
    RUNNING = "running"                          # Query is active
    STOPPED = "stopped"                          # Query was paused
    DELETED = "deleted"                          # Query was removed


class ControlSignal(BaseModel):
    """A lifecycle signal from the Drasi query engine."""
    kind: str  # One of ControlSignalKind values


class ControlEvent(ResultEvent):
    """
    A control event - tells you about the query's lifecycle, not data changes.

    Most agents don't need this (use skipControlSignals: true in your router config).
    Useful if your agent needs to know when Drasi finishes loading initial data.
    """
    kind: Literal["control"] = "control"
    controlSignal: ControlSignal


# ---- Unpacked delivery models ----
# These are used when the router is configured with format: unpacked
# Each individual add/update/delete arrives as a separate DrasiChangeNotification.

class ChangeOp(str, Enum):
    """The type of change operation."""
    INSERT = "I"  # A row was added (now matches the query)
    UPDATE = "U"  # A row changed (still matches the query)
    DELETE = "D"  # A row was removed (no longer matches the query)


class ChangePayload(BaseModel):
    """The data for one individual record change."""
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Metadata: queryId and timestamp"
    )
    before: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The record before the change (None for inserts)"
    )
    after: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The record after the change (None for deletes)"
    )


class DrasiChangeNotification(BaseModel):
    """
    An unpacked change notification - one message per changed database record.

    Use this when the router is configured with format: unpacked.
    Each notification is one insert, update, or delete.

    Example:
        if notification.op == ChangeOp.INSERT:
            new_record = notification.payload.after
        elif notification.op == ChangeOp.UPDATE:
            old_record = notification.payload.before
            new_record = notification.payload.after
        elif notification.op == ChangeOp.DELETE:
            deleted_record = notification.payload.before
    """
    op: ChangeOp
    queryId: str
    sequence: int
    tsMs: int = Field(description="When the change happened (Unix ms)")
    payload: ChangePayload
