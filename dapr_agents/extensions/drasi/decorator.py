"""
The @drasi_trigger decorator.

This is the main user-facing API for connecting Dapr Agents to Drasi change events.
It is syntactic sugar over @message_router with Drasi-specific defaults.

How it works:
1. @drasi_trigger calls @message_router internally with:
   - The correct pubsub component (agent-pubsub by default)
   - The correct topic (derived from query_id, or explicit)
   - The correct Pydantic model (ChangeEvent for packed, DrasiChangeNotification for unpacked)
2. It also sets _is_drasi_trigger=True on the function so AgentRunner knows
   not to override the topic with the agent's default agent_topic.

Usage:
    # Packed (default): one message per Drasi query update
    @drasi_trigger(query_id="sla-breaches")
    async def handle_breach(self, ctx, message: ChangeEvent):
        for breach in message.addedResults:
            ...

    # Unpacked: one message per changed database record
    @drasi_trigger(query_id="new-orders", format="unpacked")
    async def handle_order(self, ctx, message: DrasiChangeNotification):
        if message.op == ChangeOp.INSERT:
            order = message.payload.after
            ...

    # Explicit topic (if your router uses a non-default topic name)
    @drasi_trigger(topic="custom.topic.name", pubsub="my-pubsub")
    async def handle_custom(self, ctx, message: ChangeEvent):
        ...
"""
from __future__ import annotations

from typing import Any, Callable, Literal, Optional

from dapr_agents.workflow.decorators.decorators import message_router

from .models import ChangeEvent, DrasiChangeNotification


def drasi_trigger(
    func: Optional[Callable[..., Any]] = None,
    *,
    query_id: Optional[str] = None,
    pubsub: str = "agent-pubsub",
    topic: Optional[str] = None,
    format: Literal["packed", "unpacked"] = "packed",
    dead_letter_topic: Optional[str] = None,
) -> Callable:
    resolved_topic = topic or (f"drasi.{query_id}" if query_id else None)
    if not resolved_topic:
        raise ValueError("@drasi_trigger requires either 'topic' or 'query_id'. "
            "Example: @drasi_trigger(query_id='sla-breaches')")
    message_model = ChangeEvent if format == "packed" else DrasiChangeNotification

    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        # Delegate to @message_router for all the subscription wiring
        decorated = message_router(f, pubsub=pubsub, topic=resolved_topic, message_model=message_model, dead_letter_topic=dead_letter_topic,)
        setattr(decorated, "_is_drasi_trigger", True)       # flag tells the agent runner to preserve this topic, not override it
        return decorated
    if func is not None:
        raise TypeError("@drasi_trigger requires keyword arguments. Use @drasi_trigger(query_id='...') not @drasi_trigger.")
    
    
    return decorator