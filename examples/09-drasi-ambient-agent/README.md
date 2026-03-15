# Example 09: Drasi Ambient Agent (Proactive Support Agent)

This example shows an **Ambient Agent** — an AI agent that sleeps completely
until a real-world data condition is met, then wakes up and acts.

**Scenario**: A customer support team has SLA commitments. When a ticket goes
more than 24 hours without resolution, a Drasi continuous query detects it.
This agent wakes up, drafts a personalized apology, and logs it.

## How it works

1. Drasi watches a database for tickets open >24h
2. When one is detected, it publishes a change event
3. The **Router Reaction** routes it to the agent's topic on Dapr Pub/Sub
4. **This agent** wakes up (from zero), processes the ticket, then sleeps again

## Prerequisites

- Python 3.11+
- Dapr CLI installed: https://docs.dapr.io/getting-started/install-dapr-cli/
- Dapr initialized: `dapr init`
- OpenAI API key (set as `OPENAI_API_KEY` environment variable)
- Drasi Agent Router reaction running (see `../../reactions/dapr-agents-router/`)

## Quick Start (Local Testing — No Real Drasi Needed)

You can test this agent locally by manually publishing a fake Drasi event.
No database or Drasi installation required.

### Step 1: Install dependencies
Open Terminal (or VSCode terminal: View > Terminal):
```bash
cd /path/to/dapr-agents/examples/09-drasi-ambient-agent
pip install -r requirements.txt
```

### Step 2: Start the agent
```bash
dapr run \
  --app-id proactive-support \
  --app-port 8009 \
  --resources-path ./components \
  -- python agent.py
```

You should see: `INFO: Agent 'proactive-support' started and waiting for events`

### Step 3: Send a test event (simulate Drasi detecting an SLA breach)
Open a NEW terminal tab:
```bash
dapr publish \
  --publish-app-id proactive-support \
  --pubsub agent-pubsub \
  --topic support.sla-breach \
  --data '{"kind":"change","queryId":"sla-breaches","sequence":1,"sourceTimeMs":1700000000000,"addedResults":[{"ticket_id":"T001","customer_id":"C123","sla_hours":26}],"updatedResults":[],"deletedResults":[]}'
```

### Step 4: Watch the agent respond
Back in the first terminal, you'll see the agent wake up and process the ticket:
```
INFO: Received SLA breach event for query 'sla-breaches'
INFO: Processing breach: ticket=T001, customer=C123, hours_overdue=26
INFO: Agent response drafted: "Dear Customer C123, we sincerely apologize..."
```

## Files

- `agent.py` — The agent code
- `requirements.txt` — Python dependencies
- `components/agent-pubsub.yaml` — Dapr pub/sub config (uses Redis)
- `components/statestore.yaml` — Dapr state store config (uses Redis)
