"""
Tests for Drasi event Pydantic models.
No external services needed.
"""
import pytest
from dapr_agents.ext.drasi.models import (
    ChangeEvent,
    ChangeOp,
    ControlEvent,
    DrasiChangeNotification,
    UpdatePayload,
)


def test_change_event_parses_packed_event():
    raw = {
        "kind": "change",
        "queryId": "sla-breaches",
        "sequence": 42,
        "sourceTimeMs": 1700000000000,
        "addedResults": [{"ticket_id": "T001", "customer_id": "C123"}],
        "updatedResults": [],
        "deletedResults": [],
    }
    event = ChangeEvent.model_validate(raw)
    assert event.queryId == "sla-breaches"
    assert event.sequence == 42
    assert len(event.addedResults) == 1
    assert event.addedResults[0]["ticket_id"] == "T001"


def test_change_event_with_update():
    raw = {
        "kind": "change",
        "queryId": "my-query",
        "sequence": 1,
        "sourceTimeMs": 1000,
        "addedResults": [],
        "updatedResults": [
            {
                "before": {"status": "open", "hours": 23},
                "after": {"status": "open", "hours": 25},
            }
        ],
        "deletedResults": [],
    }
    event = ChangeEvent.model_validate(raw)
    assert len(event.updatedResults) == 1
    update = event.updatedResults[0]
    assert isinstance(update, UpdatePayload)
    assert update.before["hours"] == 23
    assert update.after["hours"] == 25


def test_control_event_parses():
    raw = {
        "kind": "control",
        "queryId": "my-query",
        "sequence": 0,
        "sourceTimeMs": 1000,
        "controlSignal": {"kind": "bootstrapCompleted"},
    }
    event = ControlEvent.model_validate(raw)
    assert event.controlSignal.kind == "bootstrapCompleted"


def test_drasi_change_notification_insert():
    raw = {
        "op": "I",
        "queryId": "sla-breaches",
        "sequence": 1,
        "tsMs": 1700000000000,
        "payload": {
            "source": {"queryId": "sla-breaches", "tsMs": 1700000000000},
            "before": None,
            "after": {"ticket_id": "T001"},
        },
    }
    notification = DrasiChangeNotification.model_validate(raw)
    assert notification.op == ChangeOp.INSERT
    assert notification.payload.after["ticket_id"] == "T001"
    assert notification.payload.before is None


def test_drasi_change_notification_update():
    raw = {
        "op": "U",
        "queryId": "my-query",
        "sequence": 2,
        "tsMs": 1700000000000,
        "payload": {
            "before": {"status": "pending"},
            "after": {"status": "confirmed"},
        },
    }
    n = DrasiChangeNotification.model_validate(raw)
    assert n.op == ChangeOp.UPDATE
    assert n.payload.before["status"] == "pending"
    assert n.payload.after["status"] == "confirmed"
