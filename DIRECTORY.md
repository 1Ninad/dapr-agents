# Project Directory Structure
Files and folders created or modified as part of the Drasi × Dapr Agents integration.

---

## Repo 1: `dapr-agents`

```
dapr-agents/
│
├── dapr_agents/
│   │
│   ├── ext/                                 – extensions namespace
│   │   ├── __init__.py
│   │   └── drasi/                           – Drasi SDK extension module
│   │       ├── __init__.py                  – public API exports
│   │       ├── models.py                    – Pydantic models for Drasi events
│   │       ├── decorator.py                 – @drasi_trigger decorator
│   │       └── config.py                    – DrasiSubscriptionConfig dataclass
│   │
│   └── workflow/
│       └── runners/
│           └── agent.py                     – Modified: added _is_drasi_trigger check
│
├── examples/
│   └── 09-drasi-ambient-agent/              – end-to-end demo
│       ├── agent.py                         – Proactive Support Agent
│       ├── requirements.txt
│       ├── README.md
│       └── components/
│           ├── agent-pubsub.yaml            – Dapr pub/sub component (Redis)
│           └── statestore.yaml              – Dapr state store (Redis)
│
└── tests/
    └── ext/                                 – tests for the extension
        ├── __init__.py
        └── drasi/
            ├── __init__.py
            ├── test_models.py               – unit tests for Pydantic models
            └── test_decorator.py            – unit tests for @drasi_trigger
```

---

## Repo 2: `drasi-platform`

```
drasi-platform/
│
└── reactions/
    └── dapr-agents-router/                   – Router Reaction microservice
        ├── Dockerfile                        – container image build
        ├── pyproject.toml                    – dependencies and project config
        ├── reaction.yaml                     – Drasi reaction config (queries, routing rules)
        ├── README.md
        │
        ├── src/
        │   ├── __init__.py
        │   ├── main.py                       – entrypoint: starts Router + MCP server
        │   ├── router.py                     – DrasiAgentRouter: receives events, publishes to agent
        │   ├── config.py                     – RouterQueryConfig: parses per-query YAML routing rules
        │   ├── formatter.py
        │   └── mcp_server.py                 – MCP server for query discovery
        │
        └── tests/
<!-- @import "[TOC]" {cmd="toc" depthFrom=1 depthTo=6 orderedList=false} -->

            ├── __init__.py
            ├── test_router.py                – unit tests for router logic
            └── test_formatter.py             – unit tests for packed/unpacked formatting
```

---

## How the Two Repos Connect

```
drasi-platform                         dapr-agents
(Router Reaction)                      (SDK Extension + Demo)
      │                                        │
      │   publishes CloudEvent JSON            │
      └──────────────────────────────────────► │
              via Redis (Dapr Pub/Sub)
              agent-pubsub / support.sla-breach
```

The only connection between the two repos is a **message contract** — the JSON shape of a `ChangeEvent`.
The Router produces it. The Agent validates it against `models.py`. No shared code, no imports between repos.
