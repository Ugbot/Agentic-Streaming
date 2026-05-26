# PyFlink-native Python integration

The recommended path for shipping Python-defined agents as part of a real
PyFlink job (`flink run -py …`). Python builds a declarative
**agent plan** (JSON) from decorated user code; the plan is handed to
Java's `CompileUtils.attachAgent` via PyFlink's existing Py4J gateway,
which inserts an `AgentPlanProcessFunction` into the job graph. At
runtime the function invokes Python tools and actions through PEMJA
(Python embedded in the JVM) on the operator's own thread — no IPC, no
second process.

This is parallel to Apache Flink Agents' upstream pattern, adapted to
this framework's SPIs.

## Architecture

```
Python driver           Java (PyFlink Py4J gateway)         PEMJA
─────────────           ─────────────────────────           ─────
@tool / @action  ──▶    CompileUtils.attachAgent     ──▶   per-slot
@chat_connection        AgentPlanProcessFunction            Python
   ↓                       ↓                                interpreter
build_plan()            PlanReader (FQN → SPI)              + cloudpickle
   ↓                    ToolRegistry (java + python)        registry
JSON                    Dispatch by event-type
```

## Install

```bash
pip install 'agentic-flink[pyflink]'
```

This pulls in `apache-flink` and `cloudpickle`. The Java side needs the
optional PEMJA dependency on the classpath at runtime
(`com.alibaba:pemja:0.4.1`) — included automatically when you build the
framework jar with `mvn package`.

## Defining an agent

```python
from agentic_flink.pyflink import (
    Agent, ResourceRef, action, environment, tool,
)

class TriageAgent(Agent):
    agent_id = "triage"
    system_prompt = "You triage support tickets."
    chat_setup = {"model": "qwen2.5:3b", "temperature": "0.2"}
    chat_connection = ResourceRef(
        "org.agentic.flink.llm.langchain4j.LangChain4jChatConnection",
        {"provider": "OLLAMA", "base_url": "http://localhost:11434"},
    )

    @tool
    def classify_intent(self, text: str) -> str:
        return "billing" if "refund" in text.lower() else "general"

    @action("ticket")
    def draft_reply(self, event, ctx):
        intent = self.classify_intent(event["body"])
        return {"id": event["id"], "intent": intent}
```

## Attaching to a PyFlink job

```python
from pyflink.datastream import StreamExecutionEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
tickets = s_env.from_collection([...])

ae = environment(s_env)
answers = (
    ae.from_datastream(tickets, key_selector=lambda t: t["id"])
      .apply(TriageAgent())
      .to_datastream()
)
answers.print()
s_env.execute("triage")
```

`from_datastream(...).apply(...).to_datastream()` returns a regular
PyFlink `DataStream` you can keep chaining.

## Plan format

The JSON plan is the contract between Python and Java. Schema:

```json
{
  "agent_id": "triage",
  "system_prompt": "...",
  "chat_connection": {
    "fqn": "org.agentic.flink.llm.langchain4j.LangChain4jChatConnection",
    "config": {"provider": "OLLAMA", "base_url": "http://localhost:11434"}
  },
  "chat_setup": {"model": "qwen2.5:3b", "temperature": "0.2"},
  "tools": [
    {"kind": "java", "name": "web-fetch",
     "fqn": "org.agentic.flink.web.WebFetchTool", "config": {}},
    {"kind": "python", "name": "classify_intent",
     "cloudpickle_b64": "...", "param_names": ["text"]}
  ],
  "actions": [
    {"name": "draft_reply", "events": ["ticket"], "cloudpickle_b64": "..."}
  ],
  "resources": {
    "embedder": {"fqn": "...", "config": {}}
  },
  "listeners": [
    {"kind": "java",   "fqn": "..."},
    {"kind": "python", "cloudpickle_b64": "..."}
  ]
}
```

Inspect the plan that will be sent without running anything:

```python
import json
from agentic_flink.pyflink.plan import build_plan
print(json.dumps(build_plan(TriageAgent()), indent=2))
```

## Decorators

| Decorator | Marks | Notes |
|-----------|-------|-------|
| `@tool` | Method exposed to the LLM as a callable tool | Optional `name=` / `description=`; parameter names inferred from the signature. |
| `@action(events)` | Event handler routed by event type | `events` is a string or list; empty/omitted matches anything. |
| `@listener` | Lifecycle listener | Hooks: guardrail-block, tool-call, response (phase-6 doc set). |
| `@chat_model_connection(ref)` | Default chat connection | Convenience for cases where you don't want to set `chat_connection` at class scope. |

Decorators only attach marker attributes; the plan builder walks them in
`build_plan(agent)`. This keeps decorated classes importable without a
JVM or PyFlink installation, which is what lets the offline tests run.

## Runtime semantics

* **One Python interpreter per task slot** — PEMJA boots a single
  interpreter that's reused across invocations. Cloudpickled callables
  are deserialized once and cached behind opaque handles
  (`PythonExecutor.register`).
* **Java SPIs by FQN** — chat connection, embedder, corpus, vector
  memory, etc. are referenced in the plan as fully-qualified class
  names plus a `config` map. The Java side instantiates them via
  reflection, calling either a `Map<String,String>`-arg constructor or
  the no-arg constructor followed by `initialize(config)` (the same
  pattern as `StorageFactory.createLongTermStore`).
* **Event routing** — `AgentPlanProcessFunction` infers an event type
  from a `Map` with a `"type"` key, otherwise from the class simple
  name. Every matching action fires; if no action matches, the event
  passes through unchanged.
* **Checkpointing** — the operator is a `KeyedProcessFunction`. State
  inside Python callbacks is *not* checkpointed; keep durable state on
  the Java side (Flink keyed state, the framework's
  `ShortTermMemory` / `LongTermMemoryStore` / `VectorMemorySpec`).

## When to prefer the JPype standalone path

* No PyFlink job — just a script / notebook / service.
* Synchronous request/response — no streaming context.
* You want JNI-level access to arbitrary Java classes (not just the
  agent operator surface).

The two paths can coexist in one project but **cannot share a process**:
JPype boots a JVM inside Python; PyFlink launches the JVM separately and
talks to it via Py4J. Pick one per process.
