"""
Proactive Support Agent - Example 09: Drasi Ambient Agent

This agent wakes up ONLY when a Drasi change event arrives on its pub/sub topic.
It processes each SLA breach and drafts a customer response using an LLM.

Architecture:
    Drasi change event
        --> Router Reaction publishes to [agent-pubsub: support.sla-breach]
        --> @drasi_trigger fires, starts a Dapr workflow
        --> Workflow calls LLM to draft a response
        --> Agent goes back to sleep (scale-to-zero)

To test locally (no real Drasi needed):

    Terminal 1 - Start the agent:
        dapr run --app-id proactive-support --app-port 8009 \\
            --resources-path ./components -- python agent.py

    Terminal 2 - Simulate a Drasi event:
        dapr publish --publish-app-id proactive-support \\
            --pubsub agent-pubsub --topic support.sla-breach \\
            --data '{
              "kind":"change","queryId":"sla-breaches","sequence":1,
              "sourceTimeMs":1700000000000,
              "addedResults":[{"ticket_id":"T001","customer_id":"C123","sla_hours":26}],
              "updatedResults":[],"deletedResults":[]
            }'

    Watch Terminal 1 - the agent wakes up, calls the LLM, logs the response.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal

import dapr.ext.workflow as wf
from dapr.clients import DaprClient
from dotenv import load_dotenv

from dapr_agents.extensions.drasi import ChangeEvent, drasi_trigger
from dapr_agents.llm.openai import OpenAIChatClient
from dapr_agents.workflow.utils.registration import register_message_routes

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── LLM setup ──────────────────────────────────────────────────────────────────
llm = OpenAIChatClient(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))


# ── Workflow definition ─────────────────────────────────────────────────────────
def handle_sla_breach_workflow(ctx: wf.DaprWorkflowContext, wf_input: dict):
    """
    Dapr workflow: runs when a Drasi SLA-breach event is received.
    wf_input is the ChangeEvent payload as a dict.
    """
    logger.info("Workflow started for SLA breach event")

    event = ChangeEvent.model_validate(wf_input)
    logger.info(
        "Processing: queryId=%s, sequence=%d, breaches=%d",
        event.queryId, event.sequence, len(event.addedResults),
    )

    results = []
    for breach in event.addedResults:
        result = yield ctx.call_activity(draft_response_activity, input=breach)
        results.append(result)
        logger.info("Drafted response for ticket %s", breach.get("ticket_id"))

    return results


def draft_response_activity(ctx, breach: dict) -> str:
    """
    Dapr activity: calls the LLM to draft one response per SLA breach record.
    Activities run in a thread pool - safe for blocking LLM calls.
    """
    ticket_id = breach.get("ticket_id", "unknown")
    customer_id = breach.get("customer_id", "unknown")
    sla_hours = breach.get("sla_hours", 0)

    prompt = (
        f"Customer {customer_id} has ticket {ticket_id} that is {sla_hours} hours past SLA. "
        "Draft a brief (2-3 sentence), empathetic apology and suggest one concrete next step."
    )

    messages = [{"role": "user", "content": prompt}]
    response = llm.generate(messages=messages)
    text = str(response)
    logger.info("LLM response for ticket %s: %s", ticket_id, text)
    return text


# ── Drasi trigger ───────────────────────────────────────────────────────────────
# @drasi_trigger subscribes this workflow to the 'support.sla-breach' topic.
# When an event arrives, register_message_routes() starts a new workflow instance.

@drasi_trigger(query_id="sla-breaches", topic="support.sla-breach")
def handle_sla_breach(ctx: wf.DaprWorkflowContext, wf_input: dict):
    """
    Entry point registered with @drasi_trigger.

    The decorator wires the pub/sub subscription. When a message arrives on
    'support.sla-breach', the pipeline validates it as a ChangeEvent and
    starts handle_sla_breach_workflow as a new Dapr workflow instance.
    """
    return handle_sla_breach_workflow(ctx, wf_input)


# ── Shutdown helper ─────────────────────────────────────────────────────────────
async def _wait_for_shutdown() -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _set_stop(*_):
        stop.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _set_stop)
        loop.add_signal_handler(signal.SIGTERM, _set_stop)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda *_: _set_stop())

    await stop.wait()


# ── Main ────────────────────────────────────────────────────────────────────────
async def main() -> None:
    # Register the workflow and activity with Dapr's workflow runtime
    runtime = wf.WorkflowRuntime()
    runtime.register_workflow(handle_sla_breach_workflow)
    runtime.register_workflow(handle_sla_breach)
    runtime.register_activity(draft_response_activity)
    runtime.start()

    logger.info("Starting Proactive Support Agent...")
    logger.info("Listening for SLA breaches on topic 'support.sla-breach'")
    logger.info("Agent is ready. It will wake up only when Drasi sends an event.")

    try:
        with DaprClient() as client:
            # Wire @drasi_trigger subscription - one call activates the agent
            closers = register_message_routes(
                targets=[handle_sla_breach],
                dapr_client=client,
            )
            try:
                await _wait_for_shutdown()
            finally:
                for close in closers:
                    try:
                        close()
                    except Exception:
                        logger.exception("Error closing subscription")
    finally:
        runtime.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
