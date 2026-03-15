"""
Proactive Support Agent - Example 09: Drasi Ambient Agent

This agent wakes up ONLY when Drasi detects an SLA breach in the database.
It processes the breach and drafts a customer response.

To test locally (no real Drasi needed):
    # Terminal 1: Start the agent
    dapr run --app-id proactive-support --app-port 8009 \\
        --resources-path ./components -- python agent.py

    # Terminal 2: Simulate a Drasi event
    dapr publish --publish-app-id proactive-support \\
        --pubsub agent-pubsub --topic support.sla-breach \\
        --data '{"kind":"change","queryId":"sla-breaches","sequence":1,
                 "sourceTimeMs":1700000000000,
                 "addedResults":[{"ticket_id":"T001","customer_id":"C123","sla_hours":26}],
                 "updatedResults":[],"deletedResults":[]}'

See README.md for full setup instructions.
"""
import logging
import os

from dapr_agents import DurableAgent, OpenAIChatClient
from dapr_agents.agents.configs import AgentPubSubConfig, AgentStateConfig
from dapr_agents.ext.drasi import ChangeEvent, drasi_trigger
from dapr_agents.storage.daprstores.stateservice import StateStoreService
from dapr_agents.workflow.decorators import workflow_entry
from dapr_agents.workflow.runners.agent import AgentRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── LLM setup ──────────────────────────────────────────────────────────────────
llm = OpenAIChatClient(
    model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
)

# ── Agent definition ────────────────────────────────────────────────────────────
agent = DurableAgent(
    name="proactive-support",
    role="Proactive Customer Support Agent",
    goal=(
        "When notified of an SLA breach, acknowledge it, draft a brief personalized "
        "apology, and suggest next steps. Be empathetic and concise."
    ),
    instructions=[
        "Always address the customer by their customer ID until you have their real name.",
        "Keep responses under 3 sentences.",
        "Escalate if breach is more than 48 hours.",
    ],
    llm=llm,
    pubsub=AgentPubSubConfig(
        pubsub_name="agent-pubsub",
        agent_topic="support.agent",          # Agent's own direct-message topic
    ),
    state=AgentStateConfig(store=StateStoreService(store_name="statestore")),
)


# ── Drasi trigger ───────────────────────────────────────────────────────────────
@drasi_trigger(query_id="sla-breaches", topic="support.sla-breach")
@workflow_entry
def handle_sla_breach(self, ctx, wf_input: dict) -> str:
    """
    This function runs every time Drasi detects a new SLA breach.

    The @drasi_trigger decorator:
    - Subscribes to the 'support.sla-breach' topic on 'agent-pubsub'
    - Validates the incoming message as a ChangeEvent
    - Wakes up this workflow when a message arrives

    The @workflow_entry decorator marks this as the Dapr workflow entrypoint.
    """
    logger.info("Received SLA breach event, processing...")

    # Parse the ChangeEvent from the workflow input
    try:
        event = ChangeEvent.model_validate(wf_input)
    except Exception as e:
        logger.error("Failed to parse SLA breach event: %s", e)
        return f"Error parsing event: {e}"

    results = []

    for breach in event.addedResults:
        ticket_id = breach.get("ticket_id", "unknown")
        customer_id = breach.get("customer_id", "unknown")
        sla_hours = breach.get("sla_hours", 0)

        logger.info(
            "Processing breach: ticket=%s, customer=%s, hours_overdue=%s",
            ticket_id, customer_id, sla_hours,
        )

        # Use the agent's LLM to draft a response
        prompt = (
            f"Customer {customer_id} has ticket {ticket_id} that is {sla_hours} hours past SLA. "
            "Draft a brief, empathetic apology message and suggest one concrete next step."
        )

        # Run the agent's LLM synchronously within the workflow activity
        response = yield ctx.call_activity(
            _draft_response,
            input={"prompt": prompt, "ticket_id": ticket_id},
        )

        logger.info("Response drafted for ticket %s: %s", ticket_id, response)
        results.append({"ticket_id": ticket_id, "response": response})

    return str(results)


def _draft_response(ctx, input_data: dict) -> str:
    """
    A Dapr workflow activity that calls the LLM to draft a response.
    Activities run in a thread pool and can do blocking I/O.
    """
    prompt = input_data.get("prompt", "")
    # Simple synchronous LLM call
    messages = [{"role": "user", "content": prompt}]
    response = llm.generate(messages=messages)
    return str(response)


# ── Start the agent ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting Proactive Support Agent...")
    logger.info("Listening for SLA breaches on topic 'support.sla-breach'")
    logger.info("Agent is ready. It will wake up only when Drasi sends an event.")

    runner = AgentRunner()
    runner.serve(agent)
