# Python API

Agentic Flink offers two Python paths and, within the top-level facade, three
**runtime modes** for notebooks. Pick the path first, then (for top-level) the
mode.

## Paths

| Path | Module | Use when | Mechanism |
|------|--------|----------|-----------|
| **PyFlink-native** *(recommended for streaming)* | `agentic_flink.pyflink` | You're shipping a real PyFlink job (`flink run -py …`) | Python builds an `AgentPlan` (JSON) → Java `CompileUtils.attachAgent` wires the agent operator into the job graph → PEMJA invokes Python callbacks on the operator thread |
| **JPype standalone** | `agentic_flink` (top-level) | Notebooks, scripts, services with no PyFlink dep | One of three runtime modes — see below |

The two paths don't compose; pick one. The sections below cover the JPype path;
for PyFlink-native, see [`pyflink-integration.md`](pyflink-integration.md).

## Runtime modes (JPype path)

`af.bootstrap()` picks a mode from the `AGENTIC_FLINK_MODE` env var. Same
notebook code, three execution targets:

| Mode       | What runs the work                                    | Cluster needed?                      |
|------------|-------------------------------------------------------|--------------------------------------|
| `inproc`   | JPype-managed JVM inside this Python process. No Flink job; operators are called directly via `rt.jclass(...)`. | No |
| `session`  | A long-running Flink session cluster, reached over REST at `FLINK_REST_URL`. Job submission via `rt.submit_level(...)`. | Yes (local podman or remote host) |
| `embedded` | JPype JVM **plus** a transient in-process Flink MiniCluster per `submit_level()`. Same Java code path as `session` but no cluster to bring up. | No |

Notebook bootstrap collapses to three lines regardless of mode:

```python
import agentic_flink as af
rt = af.bootstrap()                # picks mode from AGENTIC_FLINK_MODE in .env
print(rt.info)
```

Ready-to-use `.env` files at the repo root:

```bash
cp .env.inproc.example          .env   # default; in-process JVM, no cluster
cp .env.cluster.local.example   .env   # session mode; bring cluster up via run-session-cluster.sh
cp .env.cluster.remote.example  .env   # session mode; remote host (set FLINK_REST_URL)
cp .env.embedded.example        .env   # MiniCluster in-process
```

See [`compose.md`](compose.md) for the Docker side and a dockerised-notebook
option (`docker-compose-notebook.yml`).

---

## JPype standalone path

`agentic-flink` (top-level) is a thin JPype-backed facade over the Java
framework. The JVM runs **in-process** (thread mode); Python and Java
share threads, and calls cross JNI without serialization. The Java
framework is the single source of truth — the Python package adds
Pythonic ergonomics on top.

## Install

```bash
pip install agentic-flink
```

The package depends only on `JPype1`. PyFlink users can opt into the
PyFlink dep group:

```bash
pip install agentic-flink[pyflink]
```

You also need the framework jar somewhere the bootstrap can find it.
Discovery order:

1. `AGENTIC_FLINK_JAR` environment variable.
2. `jar_path=` kwarg to `start_jvm`.
3. Sibling Maven build (`../target/agentic-flink-*.jar`) — covers
   editable installs from a checkout.
4. Bundled package data (when shipping a wheel that includes the jar).

Build the jar yourself with `mvn -DskipTests package` from the repo root.

## Quick start

```python
from datetime import timedelta
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, langchain4j_ollama, tool

af.start_jvm()

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
        .with_short_term_ttl(timedelta(minutes=30))
        .build()
)

print(agent)
# Invoke the tool through its Java ToolExecutor proxy — same path the
# agent operator uses at runtime.
HashMap = af.jclass("java.util.HashMap")
args = HashMap(); args.put("a", 2); args.put("b", 3)
print("2 + 3 =", add._to_java().execute(args).get())
```

## The `@tool` decorator

Decorating a Python function makes it callable as a Java
`org.agentic.flink.tools.ToolExecutor`. The decorator generates a
JPype proxy that converts Java `Map<String, Object>` parameters to Python
kwargs (matched by parameter name), invokes the function, and wraps the
return value in a `CompletableFuture`.

```python
@tool
def greet(name: str) -> str:
    """Friendly greeting."""
    return f"hi {name}"

@tool(name="weather", description="Look up today's weather for a city")
def weather(city: str) -> dict:
    return {"city": city, "temp_c": 18.0, "summary": "cloudy"}
```

The Python function runs on whatever JVM thread invoked it (typically the
agent operator's `processElement` thread). No serialization, no process
boundary, no extra deps.

If the function raises, the exception flows back as
`CompletableFuture.failedFuture(...)`; the agent operator's normal
error-handling kicks in.

## Listeners (and other interfaces) in Python

Subclass `PyAgentEventListener` and override the hooks you care about:

```python
from agentic_flink.listener import PyAgentEventListener

class StdoutListener(PyAgentEventListener):
    name = "stdout-listener"
    def on_chat_request(self, agent_id, model, message_count):
        print(f"{agent_id}: requesting {model} with {message_count} messages")
    def on_tool_call_end(self, agent_id, tool, call_id, success, duration_ms):
        print(f"{agent_id}: {tool} {'ok' if success else 'failed'} in {duration_ms}ms")

agent = Agent.builder().with_listener(StdoutListener()).build()
```

Same pattern works for `Guardrail`, `Classifier`, `Scorer`, `Chunker`
implementations — JPype's `@JImplements` decorator generates the proxy.

## PyFlink integration

JPype's JVM is the same JVM PyFlink runs against. Use the package's
wrappers to build the spec objects, then hand them to ordinary PyFlink
operators:

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
# ... pull the underlying StreamExecutionEnvironment Java object, pass to
# CrawlerCore.builder().frontier(...).open(env_java), etc.
```

See `agentic_flink.examples.live_research` for a full PyFlink-driven job
graph that wires the framework's `CrawlerCore` + `IngestionPipeline` +
`RetrievalPipeline` together.

## What's wrapped

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

Every wrapper exposes `_to_java()` to drop down to the live Java object.

## Examples

Three runnable scripts under `agentic_flink.examples`:

- `quickstart` — calculator tool + agent build, no LLM call.
- `rag` — sequential Python RAG with sentence-transformer embeddings.
- `live_research` — full PyFlink job with two-input crawler + retrieve.

```bash
python -m agentic_flink.examples.quickstart
python -m agentic_flink.examples.rag
python -m agentic_flink.examples.live_research
```

## Testing

The package's pytest suite assumes a built framework jar. From the repo
root:

```bash
mvn -DskipTests package
pip install -e python[test]
pytest python/tests/
```

The shared `conftest.py` boots the JVM once per session and discovers the
framework + runtime jars via Maven.

## Troubleshooting

- **`FileNotFoundError: agentic-flink jar not found`** — set
  `AGENTIC_FLINK_JAR` or run `mvn -DskipTests package`.
- **`NoClassDefFoundError: org/slf4j/LoggerFactory`** — the shaded jar
  doesn't include `<scope>provided</scope>` Flink deps. Pass the full
  runtime classpath via `extra_jars=` to `start_jvm`. Generate it with
  `mvn dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt`.
- **`Initial state has no outgoing transitions`** — the Python builder
  supplies a permissive default state machine; if you call
  `.with_state_machine(...)` make sure it covers every non-terminal
  `AgentState`.
- **`TypeError: No matching overloads found`** — JPype is strict about Java
  boxed types (`Long`, `Integer`, etc.). The wrappers handle the common
  cases; for new bindings explicit box with
  `af.jclass("java.lang.Long")(int(x))`.
