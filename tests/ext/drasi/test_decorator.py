"""
Tests for @drasi_trigger decorator.
Verifies it sets the right metadata attributes for the registration pipeline.
"""
import pytest
from dapr_agents.ext.drasi.decorator import drasi_trigger
from dapr_agents.ext.drasi.models import ChangeEvent, DrasiChangeNotification


def test_drasi_trigger_sets_message_router_data():
    @drasi_trigger(query_id="sla-breaches")
    async def handle(self, ctx, message: ChangeEvent):
        pass

    data = getattr(handle, "_message_router_data", None)
    assert data is not None
    assert data["pubsub"] == "agent-pubsub"
    assert data["topic"] == "drasi.sla-breaches"
    assert ChangeEvent in data["message_schemas"]


def test_drasi_trigger_marks_as_drasi():
    @drasi_trigger(query_id="my-query")
    async def handle(self, ctx, message: ChangeEvent):
        pass

    assert getattr(handle, "_is_drasi_trigger", False) is True


def test_drasi_trigger_marks_as_message_handler():
    @drasi_trigger(query_id="my-query")
    async def handle(self, ctx, message: ChangeEvent):
        pass

    assert getattr(handle, "_is_message_handler", False) is True


def test_drasi_trigger_explicit_topic():
    @drasi_trigger(topic="custom.topic", pubsub="my-pubsub")
    async def handle(self, ctx, message: ChangeEvent):
        pass

    data = getattr(handle, "_message_router_data", {})
    assert data["topic"] == "custom.topic"
    assert data["pubsub"] == "my-pubsub"


def test_drasi_trigger_unpacked_uses_notification_model():
    @drasi_trigger(query_id="my-query", format="unpacked")
    async def handle(self, ctx, message: DrasiChangeNotification):
        pass

    data = getattr(handle, "_message_router_data", {})
    assert DrasiChangeNotification in data["message_schemas"]


def test_drasi_trigger_requires_query_id_or_topic():
    with pytest.raises(ValueError, match="requires either"):
        @drasi_trigger()
        async def handle(self, ctx, message: ChangeEvent):
            pass


def test_drasi_trigger_no_parens_raises():
    # @drasi_trigger without args should raise because query_id/topic not set
    with pytest.raises((TypeError, ValueError)):
        @drasi_trigger
        async def handle(self, ctx, message: ChangeEvent):
            pass
