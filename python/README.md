# agentic-flink (Python)

JPype-backed Python facade over the [Agentic Flink](../README.md) Java framework. Two surfaces
live here: the `agentic/v1` runtimes `local-jvm`, `flink-jvm` and `pekko` (the `python-jvm` and
`python-flink` columns of the generated [capability matrix](../docs/capabilities.md), described
on the [Python facade runtime page](../docs/runtimes/python-facade.md)), and the legacy Flink DSL
(`Agent.builder()` and `@tool`), a pure-Flink API outside the conformance matrix.

## Install

```bash
pip install agentic-flink
```

(Optional) PyFlink support:

```bash
pip install agentic-flink[pyflink]
```

You'll also need the framework jar, see [`docs/python.md`](../docs/python.md)
for the discovery rules.

## Quick start (legacy Flink DSL)

The shaded jar excludes the Flink distribution classes the DSL needs, so pass them to
`start_jvm` (`flink_jars()` finds them in `FLINK_HOME` or the `apache-flink` wheel).

```python
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, flink_jars, langchain4j_ollama, tool

af.start_jvm(extra_jars=flink_jars())

@tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

agent = (
    Agent.builder()
        .with_id("calc-bot")
        .with_system_prompt("You are a calculator.")
        .with_chat_connection(langchain4j_ollama())
        .with_chat_setup(ChatSetup(model="qwen2.5:3b"))
        .with_tools(add)
        .build()
)
```

## Portable workflows (`agentic/v1`)

The package is also the JVM-backed Tier-2 implementation of the shared cross-runtime
contract: define a workflow once (valid against `spec/v1/workflow.schema.json`), pick a
runtime, get a normalized result (valid against `spec/v1/result.schema.json`).

```python
from agentic_flink.workflow import Agent, load          # the shared high-level API
from agentic_flink._contract import get_runtime         # re-exports `agentic.runtime` when pyagentic is installed
import agentic_flink  # importing registers local-jvm / flink / pekko (also entry points)

def issue_refund(user: str, amount: float = 10.0) -> dict:
    return {"ok": True, "user": user, "amount": amount}

spec = (Agent("support")
        .route(kind="keyword", rules={"billing": ["refund", "charge"]}, default="general")
        .path("billing", brain="rule", prompt="Billing.", tools=["issue_refund"],
              tool_triggers={"refund": "issue_refund"})
        .path("general", brain="rule", prompt="General.")
        .use_tool("issue_refund", issue_refund)
        .verify("prefix")                          # verifiers are workflow-level (`agent.verifier`), not per path
        .policies(ordering="per-conversation", idempotency="turn-id", retry="exponential")
        .build())
spec = load("spec/conformance/v1/workflows/support.yaml")      # same AgentSpec from YAML/JSON

result = spec.run(runtime="local-jvm", text="refund me", conversation_id="c1", turn_id="t1")

rt = get_runtime("flink-jvm", parallelism=8)   # full control
rt.capabilities()                          # {capability: supported|partial|unsupported|not_tested}
rt.deploy(spec)                            # raises CapabilityError listing what is missing
rt.submit_all([...])                       # turns into the long-lived streaming job
rt.restart()                               # stop with savepoint, restore, recorded turns not re-run
rt.close()
```

Runtimes, what they wrap and what they need on the classpath:

| name | over | jars |
|---|---|---|
| `local-jvm` (alias `local`) | `org.jagentic.core.LocalRuntime` | shaded jar |
| `flink-jvm` | Flink adapter, one streaming job on a local cluster per `deploy()` (`LocalWorkflowSession`), `restart()` = stop with savepoint + restore; `durable=False` runs one bounded job per submit (`pyflink` is the separate agentic-pyflink package) | + Flink distribution (`FLINK_HOME`, `pip install "agentic-flink[flink]"`, or `AGENTIC_FLINK_CLASSPATH`) |
| `pekko` | `agentic-pekko` `PekkoBackendProvider` | + `./mvnw -f agentic-pekko/pom.xml package` and `AGENTIC_PEKKO_CLASSPATH` |

What each of them has proven is the generated [capability matrix](../docs/capabilities.md)
(`python-jvm` and `python-flink` columns; `python -m agentic_flink.conformance --runtime <name>`
produces the rows). `pekko` through this facade has no column, declares every capability
`not_tested`, and is unsupported because of a Jackson version conflict between the shaded jar
and Pekko's Jackson Scala module; the [Python facade runtime page](../docs/runtimes/python-facade.md)
has the details and the [Pekko page](../docs/runtimes/pekko.md) the supported native path.

Legacy `agentic_flink.Agent` (the LangChain4J `AgentBuilder` proxy) is unchanged; the
shared-contract builder is `agentic_flink.workflow.Agent` (also exported as
`agentic_flink.WorkflowAgent`).

### Jars

`start_jvm()` looks for the shaded jar in `AGENTIC_FLINK_JAR`, then a source checkout's
`target/agentic-flink-*-uber.jar`, then package data under `agentic_flink/jars/`. Wheels that
bundle the jar copy it there before `python -m build` (see `agentic_flink/jars/README.md`); a
missing jar raises `MissingJarError` naming those three options.

Full guide: [`docs/python.md`](../docs/python.md).
Runnable examples: `agentic_flink.examples.quickstart`,
`agentic_flink.examples.rag`, `agentic_flink.examples.live_research`.
