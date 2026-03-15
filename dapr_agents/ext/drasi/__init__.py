"""
dapr_agents.ext.drasi - Drasi integration for Dapr Agents.

Lets Dapr Agents subscribe to Drasi change events using a single decorator.

Usage:
    from dapr_agents.ext.drasi import drasi_trigger, ChangeEvent

    @drasi_trigger(query_id="sla-breaches")
    async def handle_breach(self, ctx, message: ChangeEvent):
        for breach in message.addedResults:
            customer_id = breach.get("customer_id")
            # The agent wakes up here and processes the breach
"""
from dapr_agents.ext.drasi.config import DrasiSubscriptionConfig
from dapr_agents.ext.drasi.decorator import drasi_trigger
from dapr_agents.ext.drasi.models import (
    ChangeEvent,
    ChangeOp,
    ChangePayload,
    ControlEvent,
    ControlSignal,
    ControlSignalKind,
    DrasiChangeNotification,
    ResultEvent,
    UpdatePayload,
)

__all__ = [
    # Decorator - the main user-facing API
    "drasi_trigger",
    # Config
    "DrasiSubscriptionConfig",
    # Models - packed delivery
    "ChangeEvent",
    "ResultEvent",
    "UpdatePayload",
    "ControlEvent",
    "ControlSignal",
    "ControlSignalKind",
    # Models - unpacked delivery
    "ChangeOp",
    "ChangePayload",
    "DrasiChangeNotification",
]
