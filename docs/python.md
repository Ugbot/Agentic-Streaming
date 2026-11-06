# Python

Python has two levels of API over the same `agentic/v1` workflow, both in `ports/pyagentic`
(distribution `pyagentic`, import package `agentic`). The high-level API loads or builds a workflow
and runs one turn with `spec.run(...)`. The full-control API constructs a `Runtime` object, deploys
the workflow on it, and drives `submit`, `restart` and `close` by hand. Both levels take the same
workflow document and return the same normalized result (`spec/v1/result.schema.json`).

Python here means plain Python, with no JVM and no streaming engine. The JPype facade and the PyFlink
binding were moved out of this repository; the code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/python).

Every code block on this page is a file under `docs/snippets/python/`, run from the repository root,
and the printed output is quoted as it was printed. `docs/tools/test_docs.py` checks that the blocks
on this page match those files.

## Install

Nothing is published to PyPI. Install from a checkout, on Python 3.9 or later (tested on 3.12 and 3.13):

```bash
python -m venv .venv
.venv/bin/pip install -e ports/pyagentic
```

## The workflow used on this page

`spec/conformance/v1/workflows/support.yaml` is the shared conformance workflow: a keyword router
(`billing`, `account`, `general`), a rule brain per path, a `constant` tool `lookup_charge` that
returns `42.5` and is triggered by the word `balance`, a prefix verifier and one regex input
guardrail. It uses no model and no external service, so it runs anywhere. The turn on this page is
`what is my balance?` in conversation `c1` with `turn_id` `t1`.

## Level 1: the high-level API

### Load a workflow and run a turn

`load(path)` reads a YAML or JSON workflow document into an `AgentSpec`. `spec.run(...)` deploys the
workflow on a fresh runtime (which is where the document is validated and the runtime's capabilities
are checked against what the workflow needs), submits one turn, closes the runtime and returns the
normalized result.

<!-- snippet: snippets/python/high_level.py -->
```python
"""High-level API: load one agentic/v1 workflow, run one turn, print the normalized result.

    python docs/snippets/python/high_level.py
"""
import json

from agentic import load

spec = load("spec/conformance/v1/workflows/support.yaml")
result = spec.run(runtime="local", text="what is my balance?", conversation_id="c1", turn_id="t1")

print(json.dumps({k: result[k] for k in ("status", "path", "reply", "tool_calls")}, sort_keys=True))
```

It prints:

```text
{"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed", "tool_calls": [{"args": {"user": "anonymous"}, "attempt": 1, "index": 0, "result": 42.5, "tool": "lookup_charge"}]}
```

The full result also carries `turn_id`, `conversation_id`, `events` (the log entries this turn
appended) and `state` (the fold over the log). The snippet prints the four fields that are easiest
to compare by eye.

### Build the workflow in Python, with a Python function as a tool

`Agent(id)` is a fluent builder that produces the same document as the YAML above. `.use_tool(name,
fn)` declares a `kind: function` tool and binds the Python callable to it. `.with_memory(...)`
selects the conversation store kind. `build()` returns an `AgentSpec` with the same `run` method.

<!-- snippet: snippets/python/builder_tools.py -->
```python
"""High-level builder: the same support workflow written in Python, with a Python function as a tool.

    python docs/snippets/python/builder_tools.py
"""
import json

from agentic import Agent


def lookup_charge(user: str) -> float:
    """Look up the most recent charge for a user."""
    return 42.5


spec = (
    Agent("support")
    .route(kind="keyword", rules={"billing": ["refund", "charge", "balance"]}, default="general")
    .path("billing", brain="rule", tools=["lookup_charge"], tool_triggers={"balance": "lookup_charge"})
    .path("general", brain="rule")
    .use_tool("lookup_charge", lookup_charge)
    .with_memory(conversation="memory")
    .policies(ordering="per-conversation", idempotency="turn-id")
    .build()
)

result = spec.run(runtime="local", text="what is my balance?", conversation_id="c1", turn_id="t1")
print(json.dumps({k: result[k] for k in ("status", "path", "reply")}, sort_keys=True))
```

It prints:

```text
{"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed"}
```

`run` handles exactly one turn on a runtime it creates and closes. A conversation with several
turns, or a restart between turns, needs the full-control API below.

## Level 2: the full-control API

A runtime implements the four methods defined by `agentic.runtime.Runtime`:

```python
class Runtime(ABC):
    def capabilities(self) -> Dict[str, str]: ...   # every v1 capability id -> supported | partial | unsupported | not_tested
    def deploy(self, spec) -> None: ...             # validate, check capabilities, make the workflow live
    def submit(self, event) -> Dict[str, Any]: ...  # process one turn to a terminal status, return the normalized result
    def close(self) -> None: ...
```

`deploy` raises `CapabilityError` listing every requirement the runtime does not declare
`supported` or `partial`. `capabilities()` is what the runtime declares about itself, not a
conformance result; see [conformance results](runtimes/README.md#conformance-results).

<!-- snippet: snippets/python/full_control.py -->
```python
"""Full-control API: construct the runtime, deploy, submit, restart, submit the same turn, close.

    python docs/snippets/python/full_control.py
"""
import json

from agentic import load
from agentic.runtime import LocalRuntime, Turn

event = Turn(conversation_id="c1", turn_id="t1", text="what is my balance?")

rt = LocalRuntime()
rt.deploy(load("spec/conformance/v1/workflows/support.yaml"))
first = rt.submit(event)
# Drop every materialized view; only the event log survives. restart() returns a fresh instance
# over the same log, so rebind it.
rt = rt.restart()
again = rt.submit(event)  # same turn_id after the restart: answered as a duplicate, nothing re-runs
rt.close()


def show(result):
    return {k: result[k] for k in ("status", "path", "reply")}


print(json.dumps({"first": show(first), "after_restart": show(again)}, sort_keys=True))
```

It prints:

```text
{"after_restart": {"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "duplicate"}, "first": {"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed"}}
```

The second `submit` carries the same `turn_id` as the first. After `restart()` the runtime rebuilt
its state from the event log alone and answered the redelivery with `status: duplicate` and the
original reply, without running the tool again. Note what this covers: a turn that reached a
terminal status. It says nothing about a tool whose turn did not finish; see the status block in
the [README](../README.md).

`agentic.runtime.get_runtime(name, **options)` constructs a runtime by name through the
`agentic.runtimes` entry-point group, which is what `spec.run(runtime=name)` uses. Only `local` is
registered by this repository.

## Tests

```bash
.venv/bin/pip install -e "ports/pyagentic[test]" -e "ports/agentic-pipeline[test]"
.venv/bin/python -m pytest ports/pyagentic/tests          # the pure Python engine and fixtures
.venv/bin/python -m pytest ports/agentic-pipeline         # the pipeline.yaml CLI
.venv/bin/python -m pytest docs/tools/test_docs.py        # relative links and snippet sync for docs/
```

## Troubleshooting

- `RuntimeNotAvailableError`: the runtime name is not registered. `local` is the only one here.
- `status: duplicate` with an empty `events` list: a redelivered `turn_id`. This is the specified behavior
  (`spec/conformance/v1/fixtures/08-duplicate-turn.yaml`), not a bug.
