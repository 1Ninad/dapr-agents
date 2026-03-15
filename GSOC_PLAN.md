# Plan: Drasi + Dapr Agents Integration Layer

## Context

Current AI agents wait for user input. "Ambient Agents" should wake up autonomously when real-world data conditions change. Drasi detects database changes via continuous queries and publishes them to Dapr Pub/Sub. Dapr Agents provides a durable, workflow-based agent runtime. This plan connects them: Drasi change events flow into Dapr Agents, so agents wake up only when specific data conditions are met (scale-to-zero).

**Data flow:**
```
Database --> Drasi CQ Engine --> [drasi-pubsub: {queryId}-results] --> Router Reaction --> [agent-pubsub: configurable-topic] --> Dapr Agent
```

Three deliverables (build order):
1. **Router Reaction** (standalone Python microservice in **drasi-platform** repo) - bridges Drasi events to agent pub/sub topics + hosts MCP server. **Build first** - produces the CloudEvent contract.
2. **SDK Extension** (`dapr_agents.ext.drasi` in **dapr-agents** repo) - Pydantic models + `@drasi_trigger` decorator. **Build second** - consumes the contract defined by the router.
3. **Demo** - end-to-end "Proactive Support Agent" in **dapr-agents** repo. **Build last** - wires everything together.

---

## Deliverable 1: Router Reaction (BUILD FIRST)

> Lives in **drasi-platform** repo at `reactions/dapr-agents-router/`. Defines the CloudEvent contract that agents will consume.

### Files to create

```
reactions/dapr-agents-router/
    pyproject.toml
    Dockerfile
    README.md
    src/
        __init__.py
        main.py           # Entrypoint: starts reaction + MCP server
        config.py         # RouterQueryConfig model
        router.py         # DrasiAgentRouter (composes DrasiReaction)
        formatter.py      # Packed/Unpacked CloudEvent formatters
        mcp_server.py     # MCP server for query discovery
    tests/
        test_router.py
        test_formatter.py
```

### Step 1: Per-query config (`src/config.py`)

```python
class OutputFormat(str, Enum):
    PACKED = "packed"
    UNPACKED = "unpacked"

class RouterQueryConfig(BaseModel):
    # Use Field aliases: YAML files use camelCase (pubsubName, topicName)
    pubsub_name: str = Field(default="agent-pubsub", alias="pubsubName")
    topic_name: str = Field(alias="topicName")
    format: OutputFormat = OutputFormat.PACKED
    skip_control_signals: bool = Field(default=True, alias="skipControlSignals")
    model_config = {"populate_by_name": True}

def parse_query_config(f) -> dict:
    """Passed to DrasiReaction; parses /etc/queries/{queryId} YAML files."""
    return yaml.safe_load(f)
```

Loaded from `/etc/queries/{queryId}` YAML files (same pattern as all Drasi reactions).

### Step 2: Formatter (`src/formatter.py`)

Two modes matching the C# `ChangeHandler.cs` at `reactions/dapr/post-pubsub/.../Services/ChangeHandler.cs`:

- **Packed:** Serialize entire `ChangeEvent` as one CloudEvent. Type = `drasi.change.packed`.
- **Unpacked:** Extract each individual add/update/delete. One CloudEvent per record. Types = `drasi.change.insert`, `drasi.change.update`, `drasi.change.delete`.

```python
def format_packed(event: ChangeEvent, config: RouterQueryConfig) -> list[dict]:
    return [event.model_dump()]

def format_unpacked(event: ChangeEvent, config: RouterQueryConfig) -> list[dict]:
    messages = []
    for record in event.addedResults:
        messages.append({"op": "I", "queryId": event.queryId, ...})
    for update in event.updatedResults:
        messages.append({"op": "U", "queryId": event.queryId, ...})
    for record in event.deletedResults:
        messages.append({"op": "D", "queryId": event.queryId, ...})
    return messages

def get_cloudevent_type(config: RouterQueryConfig, op: str | None) -> str:
    # Returns drasi.change.packed / drasi.change.insert / drasi.change.update / drasi.change.delete
    ...
```

### Step 3: Router (`src/router.py`)

Composes the existing `DrasiReaction` from the Drasi Python SDK.

> **Note:** `DaprClient` is **synchronous**. Use `with DaprClient()` and `client.publish_event()` — not `async with` or `await`.

```python
class DrasiAgentRouter:
    def __init__(self, port=80):
        self._reaction = DrasiReaction(
            on_change_event=self._handle_change,
            on_control_event=self._handle_control,
            parse_query_configs=parse_query_config,
            port=port,
        )

    async def _handle_change(self, event: ChangeEvent, raw_config):
        config = RouterQueryConfig.model_validate(raw_config)
        messages = format_packed(...) if config.format == PACKED else format_unpacked(...)
        await self._publish_messages(event, config, messages)

    async def _publish_messages(self, event, config, messages):
        with DaprClient() as client:                   # synchronous context manager
            for msg in messages:
                client.publish_event(                  # synchronous call
                    pubsub_name=config.pubsub_name,
                    topic_name=config.topic_name,
                    data=json.dumps(msg),
                    data_content_type="application/json",
                    publish_metadata={
                        "cloudevent.source": f"drasi/query/{event.queryId}",
                        "cloudevent.type": get_cloudevent_type(config, msg.get("op")),
                    },
                )

    def start(self):
        self._reaction.start()
```

### Step 4: MCP Server (`src/mcp_server.py`)

Runs in a background thread on port 3001. Exposes Drasi queries as MCP **resources** (not tools — queries are data, not actions).

- `list_resources()` returns `drasi://query/{queryId}` for each configured query
- `read_resource(uri)` returns the query's config (topic, format, description)

Agents connect via `MCPClient` (already in dapr-agents SDK at `dapr_agents/tool/mcp/client.py`) to discover available queries at runtime.

```python
mcp = FastMCP("drasi-router")

@mcp.list_resources()
async def list_resources() -> list[types.Resource]:
    # return drasi://query/{queryId} URI for each query in _query_configs
    ...

@mcp.read_resource()
async def read_resource(uri: str) -> str:
    # return JSON config for the requested queryId
    ...
```

### Step 5: Entrypoint (`src/main.py`)

```python
def main():
    router = DrasiAgentRouter()
    # Run MCP server in background thread on port 3001
    threading.Thread(
        target=lambda: mcp.run(transport="streamable-http", port=3001),
        daemon=True
    ).start()
    router.start()  # blocks
```

### Step 6: Reaction config (`reaction.yaml`)

```yaml
kind: Reaction
apiVersion: v1
name: my-agent-router
spec:
  kind: DaprAgentsRouter
  properties:
    defaultPubsubName: agent-pubsub
    defaultFormat: packed
    mcpPort: "3001"
  queries:
    sla-breaches: |
      pubsubName: agent-pubsub
      topicName: support.sla-breach
      format: packed
```

---

## Deliverable 2: SDK Extension (BUILD SECOND)

> Lives in the **dapr-agents** repo. No new external dependencies needed. Consumes the CloudEvent contract defined by the Router.

### Files to create

| File | Purpose |
|------|---------|
| `dapr_agents/ext/__init__.py` | Empty. Establishes `ext` namespace for future extensions. |
| `dapr_agents/ext/drasi/__init__.py` | Public API: exports models, decorator, config. |
| `dapr_agents/ext/drasi/models.py` | Pydantic v2 models for Drasi events (vendored, not imported from drasi-reaction-sdk). |
| `dapr_agents/ext/drasi/decorator.py` | `@drasi_trigger` decorator. |
| `dapr_agents/ext/drasi/config.py` | `DrasiSubscriptionConfig` dataclass. |
| `tests/ext/__init__.py` | Empty. |
| `tests/ext/drasi/__init__.py` | Empty. |
| `tests/ext/drasi/test_models.py` | Tests for model validation. |
| `tests/ext/drasi/test_decorator.py` | Tests for decorator behavior. |

### Step 1: Pydantic models (`dapr_agents/ext/drasi/models.py`)

Vendor the Drasi event models (~60 lines). Do NOT add `drasi-reaction-sdk` as a dependency — it's alpha and not on PyPI.

> Add `model_config = ConfigDict(extra="ignore")` to every model so unknown fields from real Drasi payloads don't cause `ValidationError`.

Source reference: `drasi-platform/reactions/sdk/python/drasi/reaction/models/ChangeEvent.py`

**Simplification vs Drasi SDK:** Drasi uses `RootModel[Optional[Dict[str, Any]]]` for records. Use `Dict[str, Any]` directly — simpler for agent consumption.

```python
class ResultEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")
    kind: str
    queryId: str
    sequence: int
    sourceTimeMs: int
    metadata: Optional[Dict[str, Any]] = None

class UpdatePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None

class ChangeEvent(ResultEvent):
    """Packed change event — all adds/updates/deletes in one message."""
    kind: Literal["change"] = "change"
    addedResults: List[Dict[str, Any]] = Field(default_factory=list)
    updatedResults: List[UpdatePayload] = Field(default_factory=list)
    deletedResults: List[Dict[str, Any]] = Field(default_factory=list)

class ControlSignalKind(str, Enum):
    BOOTSTRAP_STARTED = "bootstrapStarted"
    BOOTSTRAP_COMPLETED = "bootstrapCompleted"
    RUNNING = "running"
    STOPPED = "stopped"
    DELETED = "deleted"

class ControlSignal(BaseModel):
    model_config = ConfigDict(extra="ignore")
    kind: str

class ControlEvent(ResultEvent):
    kind: Literal["control"] = "control"
    controlSignal: ControlSignal

# For unpacked delivery (individual change notifications)
class ChangeOp(str, Enum):
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"

class ChangePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None

class DrasiChangeNotification(BaseModel):
    """Single change record — used when router sends unpacked events."""
    model_config = ConfigDict(extra="ignore")
    op: ChangeOp
    queryId: str
    sequence: int
    tsMs: int
    payload: ChangePayload
```

### Step 2: `@drasi_trigger` decorator (`dapr_agents/ext/drasi/decorator.py`)

**Approach:** Compose the existing `@message_router` decorator. This means:
- The existing registration pipeline (`register_message_routes`, `_collect_message_bindings`, `_subscribe_message_bindings`) works unchanged
- CloudEvent extraction, Pydantic validation, workflow scheduling all work as-is
- `@drasi_trigger` is syntactic sugar that calls `@message_router` with the right topic + model
- Set `_is_drasi_trigger = True` on the decorated function — required for Step 3

```python
def drasi_trigger(
    func=None,
    *,
    query_id: str | None = None,
    pubsub: str = "agent-pubsub",
    topic: str | None = None,
    format: Literal["packed", "unpacked"] = "packed",
    dead_letter_topic: str | None = None,
):
    """
    Subscribe an agent workflow to Drasi change events.
    Either `query_id` or `topic` is required.
    If only `query_id` given, topic defaults to f"drasi.{query_id}".
    """
    resolved_topic = topic or (f"drasi.{query_id}" if query_id else None)
    if not resolved_topic:
        raise ValueError("Provide 'topic' or 'query_id'.")

    model = ChangeEvent if format == "packed" else DrasiChangeNotification

    def decorator(f):
        decorated = message_router(
            f, pubsub=pubsub, topic=resolved_topic,
            message_model=model, dead_letter_topic=dead_letter_topic,
        )
        setattr(decorated, "_is_drasi_trigger", True)
        return decorated

    return decorator if func is None else decorator(func)
```

**Usage:**
```python
@drasi_trigger(query_id="sla-breaches", topic="support.sla-breach")
def handle_breach(ctx, wf_input: dict):
    ...
```

### Step 3: Fix topic override in `_build_pubsub_specs`

**Problem:** `_build_pubsub_specs` in `dapr_agents/workflow/runners/agent.py` always replaces the decorator's topic with `config.agent_topic`. This silently breaks `@drasi_trigger` — the agent subscribes to the wrong topic.

**Fix:** Check for `_is_drasi_trigger` flag and preserve the decorator's own topic/pubsub.

```python
# In _build_pubsub_specs (dapr_agents/workflow/runners/agent.py, lines 273-305):
for _, handler in handlers.items():
    meta = getattr(handler, "_message_router_data", {})
    is_drasi = getattr(handler, "_is_drasi_trigger", False)
    is_broadcast = meta.get("is_broadcast", False)

    if is_drasi:
        # Drasi triggers define their own pubsub and topic — preserve them
        topic = meta.get("topic")
        pubsub_name = meta.get("pubsub") or config.pubsub_name
    else:
        topic = config.broadcast_topic if is_broadcast else config.agent_topic
        pubsub_name = config.pubsub_name

    if not topic:
        raise ValueError(...)

    specs.append(PubSubRouteSpec(
        pubsub_name=pubsub_name, topic=topic,
        handler_fn=handler, message_model=message_model,
    ))
```

**File to modify:** `dapr_agents/workflow/runners/agent.py` (lines 273-305)

### Step 4: Config dataclass (`dapr_agents/ext/drasi/config.py`)

```python
@dataclass
class DrasiSubscriptionConfig:
    default_pubsub: str = "agent-pubsub"
    router_mcp_url: str | None = None  # e.g., "http://drasi-router:3001/mcp"
```

---

## Deliverable 3: Demo — Proactive Support Agent (BUILD LAST)

> Lives in **dapr-agents** repo at `examples/09-drasi-ambient-agent/`.

### Files

```
examples/09-drasi-ambient-agent/
    agent.py                    # The agent
    components/
        agent-pubsub.yaml       # Dapr pubsub component (Redis)
        statestore.yaml         # Dapr state store (Redis, actorStateStore=true)
```

### `agent.py`

> **Pattern:** Use plain `wf.WorkflowRuntime()` + `register_message_routes()`.
> Do NOT use `DurableAgent` — it requires `broadcast_topic` + actor state store, which is unnecessary here.

> **Critical:** Register BOTH the decorated entry-point function (`handle_sla_breach`) AND the
> workflow function (`handle_sla_breach_workflow`) with the runtime. Dapr schedules workflows by
> the decorated function's name — if only the workflow function is registered, you get
> `OrchestratorNotRegisteredError` at runtime.

```python
llm = OpenAIChatClient(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))

def handle_sla_breach_workflow(ctx: wf.DaprWorkflowContext, wf_input: dict):
    """Workflow: one activity call per breach record in the event."""
    event = ChangeEvent.model_validate(wf_input)
    for breach in event.addedResults:
        yield ctx.call_activity(draft_response_activity, input=breach)

def draft_response_activity(ctx, breach: dict) -> str:
    """Activity: blocking LLM call — safe here, runs in thread pool."""
    # build prompt from breach fields, call llm.generate(), return text
    ...

@drasi_trigger(query_id="sla-breaches", topic="support.sla-breach")
def handle_sla_breach(ctx: wf.DaprWorkflowContext, wf_input: dict):
    """Entry point wired to pub/sub by @drasi_trigger."""
    return handle_sla_breach_workflow(ctx, wf_input)

async def main():
    runtime = wf.WorkflowRuntime()
    runtime.register_workflow(handle_sla_breach_workflow)
    runtime.register_workflow(handle_sla_breach)   # both names required — see note above
    runtime.register_activity(draft_response_activity)
    runtime.start()

    with DaprClient() as client:
        closers = register_message_routes(targets=[handle_sla_breach], dapr_client=client)
        await _wait_for_shutdown()
        for close in closers:
            close()

    runtime.shutdown()
```

### End-to-end flow

1. Drasi CQ `sla-breaches` monitors database for `tickets WHERE status='open' AND sla_hours > 24`
2. New match → Drasi publishes `ChangeEvent` to `sla-breaches-results` on `drasi-pubsub`
3. Router receives it, wraps as CloudEvent, publishes to `support.sla-breach` on `agent-pubsub`
4. Agent wakes up (scale from zero), `handle_sla_breach` fires, LLM drafts response per ticket
5. Workflow completes, agent goes back to sleep

### To test locally (no real Drasi needed)

```bash
# Terminal 1 — start agent (no --app-port flag; streaming subscriptions don't need it)
export OPENAI_API_KEY=sk-...
dapr run --app-id proactive-support --resources-path ./components -- python agent.py

# Terminal 2 — simulate a Drasi event (same JSON structure as real Drasi)
dapr publish --publish-app-id proactive-support \
    --pubsub agent-pubsub --topic support.sla-breach \
    --data '{
      "kind":"change","queryId":"sla-breaches","sequence":1,
      "sourceTimeMs":1700000000000,
      "addedResults":[{"ticket_id":"T001","customer_id":"C123","sla_hours":26}],
      "updatedResults":[],"deletedResults":[]
    }'
```

Watch Terminal 1 — the agent wakes up, calls the LLM, logs the generated response.

---

## Failure Scenarios

| Scenario | Handling |
|----------|----------|
| Router crashes mid-publish | Drasi redelivers (at-least-once via Dapr pub/sub). Router is stateless per event. |
| Agent workflow fails | Dapr workflow retry policy (exponential backoff). Dead letter topic collects poison messages. |
| Duplicate events | Agent-side: `DedupeBackend` in `registration.py` deduplicates by CloudEvent ID. Events include `sequence` for ordering. |
| Router can't reach agent pubsub | Dapr pub/sub component retries internally. Router returns non-success to Drasi topic, triggering redelivery. |
| Out-of-order events | `sequence` field is monotonically increasing per query. Rely on at-least-once + idempotent workflows. Future: track last sequence in state store. |

## Security Considerations

| Concern | Mitigation |
|---------|------------|
| Pub/sub auth | Configure Dapr pub/sub components with auth (Redis passwords, Kafka SASL). Not application code. |
| MCP server exposure | ClusterIP only (not exposed outside cluster). Add API key header if external access needed. |
| Sensitive data in events | Add optional `field_filter` in `RouterQueryConfig` to mask/drop sensitive fields before forwarding. |
| Topic authorization | Use Dapr pub/sub topic scoping so agents can only subscribe to their designated topics, not `drasi-pubsub`. |
| Secrets | Use Dapr secret store references in reaction config, not plaintext env vars. |

## Verification / Testing

1. **Unit tests:** Validate Pydantic models parse real Drasi JSON payloads. Test `@drasi_trigger` sets correct `_message_router_data` attributes. Test formatter outputs correct packed/unpacked formats.
2. **Integration test:** Start agent locally with `dapr run`. Publish a mock `ChangeEvent` via `dapr publish`. Verify agent workflow triggers and LLM is called with real data.
3. **End-to-end:** Use Drasi CLI to create a source + query against a test Postgres DB. Insert a row. Verify the agent receives the change and produces output.

## Key Files to Modify/Create

**Modify:**
- `dapr_agents/workflow/runners/agent.py` (lines 273-305) — add `_is_drasi_trigger` check in `_build_pubsub_specs`

**Create (SDK extension):**
- `dapr_agents/ext/__init__.py`
- `dapr_agents/ext/drasi/__init__.py`
- `dapr_agents/ext/drasi/models.py`
- `dapr_agents/ext/drasi/decorator.py`
- `dapr_agents/ext/drasi/config.py`

**Create (Router Reaction):**
- `reactions/dapr-agents-router/src/main.py`
- `reactions/dapr-agents-router/src/router.py`
- `reactions/dapr-agents-router/src/formatter.py`
- `reactions/dapr-agents-router/src/config.py`
- `reactions/dapr-agents-router/src/mcp_server.py`

**Create (Demo):**
- `examples/09-drasi-ambient-agent/agent.py`
- `examples/09-drasi-ambient-agent/components/*.yaml`
