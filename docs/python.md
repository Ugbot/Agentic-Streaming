# Python

Python has two levels of API over the same `agentic/v1` workflow. The high-level API loads or
builds a workflow and runs one turn with `spec.run(runtime=...)`. The full-control API constructs
a `Runtime` object, deploys the workflow on it, and drives `submit`, `restart` and `close` by
hand. Both levels take the same workflow document and return the same normalized result
(`spec/v1/result.schema.json`). Which runtime executes the turn is a parameter.

Every code block on this page is a file under `docs/snippets/python/`. Each was run from the
repository root on the runtimes named next to it, and the printed output is quoted as it was
printed. `docs/tools/test_docs.py` checks that the blocks on this page match those files.

## Packages

| Package | Directory | Runtimes it provides | Runtime page |
|---|---|---|---|
| `pyagentic` (import `agentic`) | `ports/pyagentic/` | `local`: the pure Python engine | [runtimes/python.md](runtimes/python.md) |
| `agentic-flink` (import `agentic_flink`) | `python/` | `local-jvm`: `org.jagentic.core.LocalRuntime` in an in-process JVM through JPype; `flink-jvm`: a Flink MiniCluster job in the same JVM | [runtimes/python-facade.md](runtimes/python-facade.md) |
| `agentic-pyflink` (import `agentic_pyflink`) | `pyflink/` | `pyflink`: PyFlink authors the job graph, the Java `WorkflowTurnFunction` executes it | [runtimes/pyflink.md](runtimes/pyflink.md) |

The `agentic-flink` package also carries the legacy Flink DSL (`Agent.builder()` and `@tool`
over the Java framework). That surface is pure Flink and is outside the `agentic/v1` conformance
matrix. It is documented in its own section at the end of this page.

Versions: Java 21 (`./mvnw`; the enforcer rejects older JDKs and Maven below 3.9), Apache Flink
2.2.1, `apache-flink` 2.2.1, CPython 3.10 to 3.12. All paths are relative to the repository root.

## Build and install

The JVM-backed runtimes need three jars. The pure Python runtime needs none.

```bash
# 1. canonical core -> ~/.m2
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
# 2. Flink framework incl. the adapter (WorkflowTurnFunction) -> target/agentic-flink-*-uber.jar
./mvnw package -DskipTests
# 3. thin PyFlink bridge (JSON turns <-> core Event, result -> JSON) -> pyflink/java/target/agentic-pyflink-*.jar
./mvnw -f pyflink/java/pom.xml package
```

Step 3 resolves `org.agentic.flink:agentic-flink:1.0.0-SNAPSHOT` from the local Maven repository,
so run `./mvnw install -DskipTests` instead of `package` in step 2 when step 3 fails with
`Could not find artifact`.

One virtual environment holds all three packages:

```bash
python -m venv ~/.venv-agentic
~/.venv-agentic/bin/pip install -e ports/pyagentic -e "python[flink,test]" -e "pyflink[test]"
~/.venv-agentic/bin/pip install "apache-flink==2.2.1"     # pin to the Java side's Flink version
```

Jar discovery: `AGENTIC_FLINK_JAR` or `AGENTIC_FLINK_UBER_JAR` and `AGENTIC_PYFLINK_JAR` first,
then the `target/` directories of the checkout. A missing jar raises an error that names the
`./mvnw` command to run. The `flink-jvm` runtime also needs the Flink distribution jars, which
the shaded framework jar keeps out; `pip install "python[flink]"` supplies them from the
`apache-flink` wheel, or set `FLINK_HOME` or `AGENTIC_FLINK_CLASSPATH`.

## The workflow used on this page

`spec/conformance/v1/workflows/support.yaml` is the shared conformance workflow: a keyword router
(`billing`, `account`, `general`), a rule brain per path, a `constant` tool `lookup_charge` that
returns `42.5` and is triggered by the word `balance`, a prefix verifier and one regex input
guardrail. It uses no model and no external service, so it runs anywhere. The turn on this page is
`what is my balance?` in conversation `c1` with `turn_id` `t1`. The expected normalized result
is `status: completed`, `path: billing`, `reply: [billing] lookup_charge returned 42.5`, and one
tool call.

## Level 1: the high-level API

### Load a workflow and run a turn

`load(path)` reads a YAML or JSON workflow document into an `AgentSpec`. `spec.run(runtime=name,
...)` deploys the workflow on a fresh runtime (which is where the document is validated and the
runtime's capabilities are checked against what the workflow needs), submits one turn, closes the
runtime and returns the normalized result. The pure Python package and the facade expose the same
call; only the import differs.

<!-- snippet: snippets/python/high_level.py -->
```python
"""High-level API: load one agentic/v1 workflow, run one turn, print the normalized result.

    python docs/snippets/python/high_level.py local        # pure Python, ports/pyagentic
    python docs/snippets/python/high_level.py local-jvm    # JVM core through the facade, python/
    python docs/snippets/python/high_level.py flink-jvm    # Flink MiniCluster through the facade
    python docs/snippets/python/high_level.py pyflink      # PyFlink job, agentic_pyflink registered as a runtime
"""
import json
import sys

runtime = sys.argv[1] if len(sys.argv) > 1 else "local"

if runtime == "local":
    from agentic import load  # ports/pyagentic: pure Python
else:
    from agentic_flink import load  # python/: the facade; its spec is accepted by every JVM-backed runtime

spec = load("spec/conformance/v1/workflows/support.yaml")
result = spec.run(runtime=runtime, text="what is my balance?", conversation_id="c1", turn_id="t1")

print(json.dumps({k: result[k] for k in ("status", "path", "reply", "tool_calls")}, sort_keys=True))
```

Run on `local` (pure Python), `local-jvm` (JVM core), `flink-jvm` (Flink MiniCluster) and
`pyflink` (PyFlink job). All four printed the same line:

```text
{"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed", "tool_calls": [{"args": {"user": "anonymous"}, "attempt": 1, "index": 0, "result": 42.5, "tool": "lookup_charge"}]}
```

The full result also carries `turn_id`, `conversation_id`, `events` (the log entries this turn
appended) and `state` (the fold over the log). The snippet prints the four fields that are easiest
to compare by eye.

Runtime names resolve through the `agentic.runtimes` entry-point group, so any installed package
can add one: `pyagentic` registers `local`, `agentic-flink` registers `local-jvm`, `flink-jvm` and
`pekko`, and `agentic-pyflink` registers `pyflink`. The `AgentSpec` you pass has to come from a
package the runtime accepts. The facade's `agentic_flink.load` produces a spec that every JVM-backed
runtime takes. The pure Python `agentic.load` produces a spec that only `local` takes: handing it to
`local-jvm` fails with `TypeError: deploy() needs an AgentSpec or a workflow mapping, got AgentSpec`,
and to `pyflink` with `WorkflowLoadError: workflow document must be a mapping`. That is why the
snippet switches the import on the runtime name.

PyFlink also has a command line for the same turn, which prints the whole normalized result:

```bash
python -m agentic_pyflink run spec/conformance/v1/workflows/support.yaml --text "what is my balance?"
```

### Build the workflow in Python, with a Python function as a tool

`Agent(id)` is a fluent builder that produces the same document as the YAML above. `.use_tool(name,
fn)` declares a `kind: function` tool and binds the Python callable to it. `.with_memory(...)`
selects the conversation store kind. `build()` returns an `AgentSpec` with the same `run` method.

<!-- snippet: snippets/python/builder_tools.py -->
```python
"""High-level builder: the same support workflow written in Python, with a Python function as a tool.

    python docs/snippets/python/builder_tools.py local        # agentic.Agent, pure Python
    python docs/snippets/python/builder_tools.py local-jvm    # agentic_flink.WorkflowAgent, JVM core
    python docs/snippets/python/builder_tools.py flink-jvm    # raises CapabilityError (see docs/python.md)
"""
import json
import sys

runtime = sys.argv[1] if len(sys.argv) > 1 else "local"

if runtime == "local":
    from agentic import Agent
else:
    from agentic_flink import WorkflowAgent as Agent


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

result = spec.run(runtime=runtime, text="what is my balance?", conversation_id="c1", turn_id="t1")
print(json.dumps({k: result[k] for k in ("status", "path", "reply")}, sort_keys=True))
```

Run on `local` and `local-jvm`. Both printed:

```text
{"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed"}
```

Run on `flink-jvm` it does not execute. `deploy()` raises before any job starts:

```text
agentic.errors.CapabilityError: runtime 'flink-jvm' cannot run this workflow; unsupported requirements:
  - tools[lookup_charge] (kind=function (Python) tools cannot run inside a Flink job graph)
```

This is the intended behavior. A Flink job graph runs Java operators; a Python callable cannot be
serialized into it. Workflows for `flink-jvm` and `pyflink` declare tools of kind `constant`,
`http` or another kind the Java adapter implements, as `support.yaml` does. The facade builder is
exported as `agentic_flink.WorkflowAgent` because the name `agentic_flink.Agent` belongs to the
legacy DSL handle (see below).

`run` handles exactly one turn on a runtime it creates and closes. A conversation with several
turns, or a restart between turns, needs the full-control API below.

## Level 2: the full-control API

Every runtime implements the same four methods, defined by `agentic.runtime.Runtime` in
`ports/pyagentic` and mirrored by `agentic_flink._contract.Runtime` when `pyagentic` is not
installed:

```python
class Runtime(ABC):
    def capabilities(self) -> Dict[str, str]: ...   # every v1 capability id -> supported | partial | unsupported | not_tested
    def deploy(self, spec) -> None: ...             # validate, check capabilities, make the workflow live
    def submit(self, event) -> Dict[str, Any]: ...  # process one turn to a terminal status, return the normalized result
    def close(self) -> None: ...
```

`deploy` raises `CapabilityError` listing every requirement the runtime does not declare
`supported` or `partial`. `capabilities()` reports what the runtime declares about itself; the
generated matrix in [capabilities.md](capabilities.md) reports what the fixtures proved, and it is
the matrix that counts.

<!-- snippet: snippets/python/full_control.py -->
```python
"""Full-control API: construct the runtime, deploy, submit, restart, submit the same turn, close.

    python docs/snippets/python/full_control.py local        # agentic.runtime.LocalRuntime
    python docs/snippets/python/full_control.py local-jvm    # agentic_flink.JvmLocalRuntime
    python docs/snippets/python/full_control.py flink-jvm    # agentic_flink.FlinkRuntime
    python docs/snippets/python/full_control.py pyflink      # agentic_pyflink.FlinkRuntime
"""
import json
import sys

runtime = sys.argv[1] if len(sys.argv) > 1 else "local"
workflow = "spec/conformance/v1/workflows/support.yaml"
turn = {"conversation_id": "c1", "turn_id": "t1", "text": "what is my balance?"}

if runtime == "local":
    from agentic import load
    from agentic.runtime import LocalRuntime, Turn

    rt = LocalRuntime()
    spec = load(workflow)
    event = Turn(**turn)
elif runtime == "local-jvm":
    from agentic_flink import Event, JvmLocalRuntime, load

    rt = JvmLocalRuntime()
    spec = load(workflow)
    event = Event(**turn)
elif runtime == "flink-jvm":
    from agentic_flink import Event, FlinkRuntime, load

    rt = FlinkRuntime(parallelism=1, checkpoint_interval="1s")
    spec = load(workflow)
    event = Event(**turn)
else:
    from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

    rt = FlinkRuntime(FlinkConfig(mode="local", parallelism=1, checkpoint_interval="1s"))
    spec = load_workflow(workflow)
    event = turn

rt.deploy(spec)
first = rt.submit(event)
# Drop every materialized view; only the event log survives. The pure Python LocalRuntime
# returns a fresh instance over the same log; the JVM-backed runtimes restart in place.
rt = rt.restart() or rt
again = rt.submit(event)  # same turn_id after the restart: answered as a duplicate, nothing re-runs
rt.close()


def show(result):
    return {k: result[k] for k in ("status", "path", "reply")}


print(json.dumps({"first": show(first), "after_restart": show(again)}, sort_keys=True))
```

Run on `local`, `local-jvm`, `flink-jvm` and `pyflink`. All four printed:

```text
{"after_restart": {"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "duplicate"}, "first": {"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed"}}
```

The second `submit` carries the same `turn_id` as the first. After `restart()` every runtime
rebuilt its state from the event log alone and answered the redelivery with `status: duplicate`
and the original reply, without running the tool again. That is the `idempotency` and `replay`
behavior of `spec/v1/primitives.md`, checked by the `duplicate-turn` and `replay-after-restart`
fixtures on every binding in [capabilities.md](capabilities.md#fixtures).

What differs between the four constructors:

| Runtime | Class | `restart()` | Turn type |
|---|---|---|---|
| `local` | `agentic.runtime.LocalRuntime(log=None, store_dir=None, ...)` | closes this instance and returns a fresh `LocalRuntime` over the same log | `agentic.runtime.Turn` or a mapping |
| `local-jvm` | `agentic_flink.JvmLocalRuntime()` | rebuilds `org.jagentic.core.LocalRuntime` over the same in-memory `ConversationLog`, in place | `agentic_flink.Event` or a mapping |
| `flink-jvm` | `agentic_flink.FlinkRuntime(parallelism=1, checkpoint_interval=None, durable=True, ...)` | stop with savepoint, then a fresh job restored from it, in place; refused with `durable=False` | `agentic_flink.Event` or a mapping |
| `pyflink` | `agentic_pyflink.FlinkRuntime(FlinkConfig(...), source=None, sink=None)` | stop with savepoint, then a fresh job restored from it, in place | a mapping |

`agentic.runtime.get_runtime(name, **options)` constructs any of them by name through the
`agentic.runtimes` entry-point group, which is what `spec.run(runtime=name)` uses.

### PyFlink: bounded runs

`agentic_pyflink.FlinkRuntime.run(spec, turns)` executes a list of turns as one bounded job and
returns the normalized results in per-conversation order. The streaming form (`deploy`, `submit`,
`restart`) is the one shown above.

<!-- snippet: snippets/python/pyflink_bounded.py -->
```python
"""PyFlink bounded run: a list of turns in, normalized results out, the job ends when drained.

    python docs/snippets/python/pyflink_bounded.py
"""
import json

from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

spec = load_workflow("spec/conformance/v1/workflows/support.yaml")
with FlinkRuntime(FlinkConfig(mode="local", parallelism=1)) as rt:
    results = rt.run(spec, [{"conversation_id": "c1", "turn_id": "t1", "text": "what is my balance?"}])
print(json.dumps({k: results[0][k] for k in ("status", "path", "reply")}, sort_keys=True))
```

Run on `pyflink`. It printed:

```text
{"path": "billing", "reply": "[billing] lookup_charge returned 42.5", "status": "completed"}
```

`FlinkConfig`, the connectors and cluster submission are documented on the
[PyFlink runtime page](runtimes/pyflink.md).

## Runtime names and what the matrix says

| Name | Matrix column | Notes section |
|---|---|---|
| `local` | [python](capabilities.md#capabilities) | [notes](capabilities.md#python) |
| `local-jvm` | [python-jvm](capabilities.md#capabilities) | [notes](capabilities.md#python-jvm) |
| `flink-jvm` | [python-flink](capabilities.md#capabilities) | [notes](capabilities.md#python-flink) |
| `pyflink` | [pyflink](capabilities.md#capabilities) | [notes](capabilities.md#pyflink) |

Every capability claim for these runtimes lives in that generated file, and the per-runtime pages
under [runtimes/](runtimes/) repeat only the entries that are not `supported`, with a test in
`docs/tools/test_docs.py` that fails when a page and the matrix disagree. In the current matrix
`timers` and `checkpoint_recovery` are `unsupported` on all four Python bindings, so the three
timer fixtures are skipped there. Do not read a `supported` entry in `rt.capabilities()` as a
conformance result; only the matrix is.

## Pekko through the facade: unsupported

`agentic_flink.PekkoRuntime` (name `pekko`) exists in the facade, and the package registers it as
an entry point. It is not a supported path. The `agentic-pekko` module needs a newer Jackson than
the one shaded into the Flink framework jar, so the facade can only start it by placing the Pekko
jars ahead of the framework jar in a JVM that has not been started yet. The two runtimes cannot share
a JVM, and the facade's conformance binding does not run on it: every capability of the `pekko`
runtime in the facade is declared `not_tested` (`replay` is declared `unsupported`, because the
binding has no `restart()` for the actor system). The facade `pekko` runtime therefore has no column
in [capabilities.md](capabilities.md). Use the JVM entry points on the
[Pekko runtime page](runtimes/pekko.md) instead. The Jackson conflict is not worked around in this
repository.

## The legacy Flink DSL: pure Flink flavor

`agentic_flink.Agent` is a JPype handle on the Java `org.agentic.flink.dsl.Agent`, built with
`Agent.builder()`. `@tool` wraps a Python function as a Java `org.agentic.flink.tools.ToolExecutor`
(Java `Map<String, Object>` parameters become keyword arguments; the return value is wrapped in a
`CompletableFuture`; exceptions come back as a failed future). This is the Flink framework's own
API. It is supported and kept, it does not take an `agentic/v1` workflow document, and it is
outside the conformance matrix: nothing in [capabilities.md](capabilities.md) speaks about it.

<!-- snippet: snippets/python/legacy_dsl.py -->
```python
"""Legacy Flink DSL through the facade: Agent.builder() and @tool. Pure Flink flavor, outside agentic/v1.

    python docs/snippets/python/legacy_dsl.py
"""
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, flink_jars, langchain4j_ollama, tool

af.start_jvm(extra_jars=flink_jars())  # the shaded jar keeps Flink itself out (provided scope)


@tool(name="add", description="Add two integers")
def add(a: int, b: int) -> int:
    return a + b


agent = (
    Agent.builder()
    .with_id("calc-bot")
    .with_system_prompt("You are a calculator.")
    .with_chat_connection(langchain4j_ollama())
    .with_chat_setup(ChatSetup(model="qwen2.5:3b", temperature=0.3))
    .with_tools(add)
    .with_max_iterations(5)
    .build()
)

args = af.jclass("java.util.HashMap")()
args.put("a", 2)
args.put("b", 40)
print(agent.id, sorted(agent.allowed_tools), add._to_java().execute(args).get())
```

Run in the facade's in-process JVM with the Flink distribution jars on the classpath. It printed
`calc-bot ['add'] 42` after the framework's state machine warnings. No model is called: the
snippet builds the agent and invokes the tool through its Java proxy, which is the path the agent
operator takes when a model calls the tool. Without `extra_jars=flink_jars()` the build fails with
`NoClassDefFoundError: org/apache/flink/api/common/functions/RuntimeContext`, because the shaded
jar excludes the provided Flink runtime.

### What the legacy surface wraps

| Module | Surface |
|--------|---------|
| `agentic_flink.agent` | `Agent`, `AgentBuilder`, default minimal state machine |
| `agentic_flink.llm` | `ChatSetup`, `ChatMessage`, `langchain4j_ollama`, `langchain4j_openai`, `chat()` |
| `agentic_flink.tools` | `@tool` decorator, `PythonTool` |
| `agentic_flink.memory` | `flink_state_short_term`, `flink_state_brute_force`, `flink_state_hnsw` |
| `agentic_flink.embedding` | `EmbeddingSetup`, `ollama_embedding`, `djl_embedding` |
| `agentic_flink.corpus` | `single_operator`, `broadcast`, `external` |
| `agentic_flink.channel` | `static_seed`, `kafka`, `kafka_context`, `webhook`, `tool_invocation_side_output`, `tool_invocation_in_jvm` |
| `agentic_flink.inference` | `InferenceSetup`, `djl_classification`, `djl_embedding`, `classifier_guardrail`, `inference_tool` |
| `agentic_flink.web` | `options`, `fetch_tool`, `extract_links_tool`, `url_request`, `crawler_core` |
| `agentic_flink.ingest` | `recursive_chunker`, `chunk()`, `pipeline_from` |
| `agentic_flink.retrieve` | `pipeline_from` |
| `agentic_flink.listener` | `PyAgentEventListener` |
| `agentic_flink.skill` | `Skill`, `mcp_stdio`, `mcp_http` |
| `agentic_flink.pyflink` | PyFlink-native agent plan (`AgentPlan` JSON to `CompileUtils.attachAgent`); see [pyflink-integration.md](pyflink-integration.md) |

Every wrapper exposes `_to_java()` to reach the live Java object. `af.bootstrap()` picks a
deployment mode from `AGENTIC_FLINK_MODE` (`inproc`, `session`, `embedded`); the `.env.*.example`
files at the repository root hold one configuration each, and `docs/compose.md` covers the
container side. The legacy examples are `python -m agentic_flink.examples.quickstart`,
`agentic_flink.examples.rag` and `agentic_flink.examples.live_research`; `quickstart` calls
`af.start_jvm()` without the Flink jars and fails on a checkout that has only the shaded jar, as
described above.

## Tests

```bash
~/.venv-agentic/bin/python -m pytest ports/pyagentic                   # pure Python engine and fixtures
~/.venv-agentic/bin/python -m pytest python/tests                      # facade: JPype, local-jvm, flink-jvm
~/.venv-agentic/bin/python -m pytest pyflink                           # PyFlink: unit, e2e, fixtures on the MiniCluster
~/.venv-agentic/bin/python -m agentic_pyflink.conformance              # fixture report: PASS/FAIL/SKIP per id
~/.venv-agentic/bin/python -m agentic_flink.conformance --runtime local-jvm
~/.venv-agentic/bin/python -m pytest docs/tools/test_docs.py           # relative links and snippet sync for docs/
```

Lint: `ruff check ports/pyagentic/agentic pyflink python/agentic_flink docs/snippets docs/tools`.

## Troubleshooting

- `JarNotFoundError` or `MissingJarError`: run the `./mvnw` commands under "Build and install", or set
  `AGENTIC_FLINK_UBER_JAR`, `AGENTIC_PYFLINK_JAR` or `AGENTIC_FLINK_JAR`.
- `NoClassDefFoundError: org/apache/flink/...` from the facade: the shaded jar excludes provided Flink
  classes. Pass `extra_jars=flink_jars()` to `start_jvm`, or select the `flink-jvm` runtime, which does this.
- `ValidationException` from `executeAndCollect`: the document failed the shared `WorkflowValidator`;
  the message names the field, identical to what the JVM runtime prints.
- `ResultTimeoutError` from PyFlink `submit()`: the result did not reach the spool within `result_timeout`
  seconds (default 60). A `FAILED` job surfaces its exception as `RuntimeStateError` instead.
- `CapabilityError` naming `kind=function`: a Python callable tool on a Flink runtime. Use a tool kind the
  Java adapter implements, or run on `local` or `local-jvm`.
- `status: duplicate` with an empty `events` list: a redelivered `turn_id`. This is the specified behavior
  (`spec/conformance/v1/fixtures/08-duplicate-turn.yaml`), not a bug.
- JPype tests may segfault at interpreter exit when the Pekko jars are on the classpath. The test results
  before the exit are still valid; run the Pekko facade tests in their own process.
