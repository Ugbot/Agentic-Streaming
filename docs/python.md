# Python

Two Python packages ship in this repository, for two different jobs:

| Package | Directory | What it is |
|---------|-----------|------------|
| **`agentic-pyflink`** | `pyflink/` | Runs the **portable v1 workflow document** (`spec/v1/workflow.schema.json`) on Apache Flink from Python. PyFlink authors the job graph; the operator inside it is the Java Flink adapter of the canonical core (`ports/jagentic-core`), the same code the JVM runtime executes. One document, same normalized result (`spec/v1/result.schema.json`) as the JVM, Pekko and Clojure runtimes. **This is the cross-runtime Python path.** |
| `agentic-flink` | `python/` | JPype-backed facade over the Java framework's own builder API (`Agent.builder()`, `@tool`, memory/retrieval/crawler wrappers). Notebook and scripting ergonomics for the Flink-specific framework surface; not tied to the portable document. |

Versions, for both: **Java 17**, **Apache Flink 2.2.1**, `apache-flink` (PyFlink) 2.x, CPython 3.10-3.12
(PyFlink's supported interpreters). All paths below are relative to the repository root.

---

## `agentic-pyflink`: portable workflows on Flink

### Build (once per checkout)

The Python side never re-implements the workflow semantics; it drives three jars:

```bash
# 1. canonical core -> ~/.m2
mvn -f ports/jagentic-core/pom.xml install -DskipTests
# 2. Flink framework incl. the adapter (WorkflowTurnFunction) -> target/agentic-flink-*-uber.jar
mvn package -DskipTests
# 3. thin PyFlink bridge (JSON turns <-> core Event, result -> JSON) -> pyflink/java/target/agentic-pyflink-*.jar
mvn -f pyflink/java/pom.xml package
# 4. the Python package
python -m venv ~/.venv-pyflink && ~/.venv-pyflink/bin/pip install -e "pyflink[test]"
```

Jar discovery: `AGENTIC_FLINK_UBER_JAR` / `AGENTIC_PYFLINK_JAR` environment variables first, then the
Maven `target/` directories of the checkout. A missing jar raises `JarNotFoundError` with the exact
`mvn` command to run; nothing falls back silently.

### Run a document

```bash
~/.venv-pyflink/bin/python -m agentic_pyflink run examples/pipelines/banking.yaml --text "what is my balance?"
~/.venv-pyflink/bin/python pyflink/examples/banking_local.py        # bounded run, duplicate delivery, parallelism 2
~/.venv-pyflink/bin/python pyflink/examples/support_streaming.py    # deploy/submit/restart on a savepoint
```

Same document, same answer as `mvn -f agentic-pekko/pom.xml exec:java ... banking.yaml` or the
Clojure demo (`docs/examples/banking-everywhere.md`): `path=payments`, tool `get_balance`, reply
`[payments] get_balance returned 1234.56`.

```python
from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

spec = load_workflow("examples/pipelines/banking.yaml")          # YAML or JSON; the v1 document as-is
rt = FlinkRuntime(FlinkConfig(mode="local", parallelism=2, checkpoint_interval="1s", state_backend="rocksdb"))

# bounded: a list of turns in, normalized results out, job ends when drained
results = rt.run(spec, [{"conversation_id": "c1", "turn_id": "t1", "text": "what is my balance?"}])

# streaming: the job stays up; each submit() blocks until that turn's normalized result is visible
rt.deploy(spec)                        # capability check + validation + execute_async
r = rt.submit({"conversation_id": "c1", "turn_id": "t2", "text": "I lost my card"})
rt.restart()                           # stop-with-savepoint, fresh job restored from it
rt.close()

rt.capabilities()                      # {"routing": "supported", "llm_brain": "not_tested", ...}
```

The document is parsed in Python (PyYAML/json) and handed to the JVM as JSON; **validation is the
shared `WorkflowValidator`** of `ports/jagentic-core`. There is no Python-side schema, builder or
second format. Runtime-specific knobs inside the document (`runtime.flink`: state TTL, timer
domain, resume delay) are read by the adapter exactly as in the JVM runner; deployment knobs live in
`FlinkConfig`.

The package registers `flink = agentic_pyflink.runtime:FlinkRuntime` in the `agentic.runtimes`
entry-point group, which is how the shared `agentic.runtime.get_runtime("flink", parallelism=8,
checkpoint_interval="30s")` surface discovers it (keyword arguments become `FlinkConfig` fields).

### Turn and result wire form

A turn is the fixture turn shape of `spec/conformance/v1`: `conversation_id`, `turn_id` (both
required), `text`, optional `user_id` (default `anonymous`), `metadata` (string map) and, to resume
a suspended turn, `signal` (mapping) instead of `text`. A result is one `spec/v1/result.schema.json`
document per turn, encoded by the adapter's `TurnResultCodec`. Both are one JSON document per record
on every connector.

### Flink configuration from Python (`FlinkConfig`)

| Field | Flink setting | Notes |
|-------|---------------|-------|
| `mode` | `execution.target` | `"local"` (default) or `"cluster"`, see below |
| `parallelism` | `parallelism.default` + `env.set_parallelism` | proven at 2 by `test_parallel_job_keeps_per_conversation_order` |
| `checkpoint_interval` | `execution.checkpointing.interval` + `enable_checkpointing` | `500`, `"500ms"`, `"30s"`, `"2m"`, `"1h"` |
| `state_backend` | `state.backend.type` | `hashmap` (default), `rocksdb`, `forst`; all three ship in `flink-dist` 2.2 |
| `incremental_checkpoints` | `execution.checkpointing.incremental` | rejected for `hashmap` |
| `checkpoint_dir`, `savepoint_dir` | `execution.checkpointing.dir`, `...savepoint-dir` | paths become `file://` URIs; `s3://`, `hdfs://` pass through |
| `rest_address`, `rest_port` | `rest.address`, `rest.port` | required in `cluster` mode |
| `extra_jars` | `pipeline.jars` (via `env.add_jars`) | e.g. an S3 filesystem plugin or a connector |
| `flink_options` | any key | escape hatch, applied last |

Invalid values raise `ValueError` at construction, before a JVM is touched.

### Connectors

| Class | Direction | Mechanism | Tested |
|-------|-----------|-----------|--------|
| `CollectionSource(turns)` | in | bounded `from_collection`; used by `run()` | yes, all e2e tests |
| `FileSource(directory)` | in | FLIP-27 filesystem source, one JSON turn per line, `monitor_continuously` | yes, every `deploy()` test and all 15 fixtures |
| `FileSink(directory)` | out | FLIP-143 `FileSink`, row format, rolls on checkpoint | yes, same |
| `KafkaSource(bootstrap_servers, topic, ...)` | in | `flink-connector-kafka` `KafkaSource`, simple string schema | **job-graph construction only** (`test_kafka_connectors_build_a_job_graph_without_a_broker`); delivery through a broker is not exercised in CI |
| `KafkaSink(bootstrap_servers, topic)` | out | `KafkaSink`, at-least-once | same |

The Kafka connector classes come bundled in the framework uber jar; a connector whose classes are
missing raises `ConnectorUnavailableError` naming the jar to add. `deploy()` with no connectors
given creates a local spool (`FileSource` + `FileSink` under a temp dir), which is what `submit()`
writes to and reads from. With Kafka connectors, produce/consume the topics yourself; `submit()`
refuses (it only knows the spool) rather than pretending.

### Local MiniCluster vs. cluster submission

**`mode="local"`** (what CI runs): PyFlink starts a Py4J gateway JVM and the job executes on a Flink
MiniCluster inside it. No `flink run`, no cluster, no Podman. Savepoints and checkpoints go to a
temp directory unless `checkpoint_dir`/`savepoint_dir` are set. This is the mode every test in
`pyflink/tests` and the conformance binding use.

**`mode="cluster"`**: the job graph is submitted over REST (`execution.target=remote`) to a running
Flink cluster. This package configures the submission; it does not provision or fake the cluster.
Prerequisites, all yours to provide:

1. A Flink **2.2.1** session cluster reachable at `rest_address:rest_port` (`examples-bin/run-session-cluster.sh`
   starts one with Podman; see `docs/compose.md`).
2. The **same three jars** (`agentic-flink-*-uber.jar`, `agentic-pyflink-*.jar`, and `jagentic-core`
   is inside the uber jar) on the cluster's classpath, either shipped with the job (`add_jars` does
   this for the two local jars) or placed in every TaskManager's `lib/`.
3. `checkpoint_dir`/`savepoint_dir` on a filesystem **every TaskManager can reach** (`s3://`, `hdfs://`,
   NFS); a local path only works when the whole cluster shares that disk. `restart()` needs this.
4. Connectors the cluster can reach: `submit()`'s file spool is local to the submitting process, so
   cluster deployments use `KafkaSource`/`KafkaSink` (or a `FileSource`/`FileSink` on a shared
   filesystem) and produce/consume the topics directly.
5. A `python` on the TaskManagers is **not** required: no Python UDFs run inside the job; all
   operators are Java.

```python
rt = FlinkRuntime(
    FlinkConfig(mode="cluster", rest_address="jobmanager.internal", rest_port=8081,
                parallelism=8, checkpoint_interval="30s", state_backend="rocksdb",
                incremental_checkpoints=True, checkpoint_dir="s3://agentic/ckpt",
                savepoint_dir="s3://agentic/savepoints",
                extra_jars=["/opt/flink/plugins/s3/flink-s3-fs-hadoop-2.2.1.jar"]),
    source=KafkaSource("kafka:9092", "turns", group_id="support"),
    sink=KafkaSink("kafka:9092", "results"),
)
rt.deploy(spec)   # returns once the JobManager accepted the job
```

Cluster mode is **not exercised by the test suite** in this repository (there is no cluster in CI);
the configuration mapping itself is unit-tested (`test_cluster_mode_needs_rest_address_and_targets_remote`).

### Capabilities

`FlinkRuntime.capabilities()` returns every id of `spec/v1/primitives.md` §6. An id is `supported`
only when a test in `pyflink/tests` proves it (`agentic_pyflink.capabilities.PROOF` names the test);
everything else the Java adapter implements but this package has not proven is `not_tested`:

- **supported**: routing, rule_brain, tools, structured_tool_args, guardrails, verifier, ordering,
  idempotency, retry, memory, retrieval, replay, suspend_resume, saga, a2a, durable_store
  (all 15 conformance fixtures), parallelism (`parallelism=2` ordering test).
- **not_tested**: llm_brain, context_window, timers, cep, event_time, checkpoint_recovery
  (`restart()` proves savepoint restore, not recovery from a failure-triggered checkpoint).

`deploy()`/`run()` derive what a document needs (`required_capabilities`) and raise `CapabilityError`
for anything `unsupported`; `not_tested` needs produce a `warnings.warn`.

### Tests and conformance

```bash
~/.venv-pyflink/bin/python -m pytest pyflink                       # unit + e2e + 15 fixtures, ~40 s
~/.venv-pyflink/bin/python -m agentic_pyflink.conformance          # fixture report: PASS/FAIL/SKIP per id
```

`pyflink/tests/test_conformance.py` runs every fixture of `spec/conformance/v1/fixtures` through the
local MiniCluster with the comparator imported from `spec/tools/run_conformance.py`. Fixture verbs
map as in the JVM harness: `concurrent_with` turns are delivered back to back in one file so Flink's
per-key ordering is what gets tested; `restart_runtime` is stop-with-savepoint plus a fresh job
restored from it, so only checkpointed state survives. A fixture whose `requires` is outside the
runtime's supported set is a pytest **skip with that reason**, never a pass. Tests that need the JVM
side **fail** (with the build commands) when the jars are missing; they do not skip.

Lint: `ruff check pyflink` (config in `pyflink/pyproject.toml`).

### Troubleshooting

- `JarNotFoundError`: run the three `mvn` commands under "Build", or set `AGENTIC_FLINK_UBER_JAR` /
  `AGENTIC_PYFLINK_JAR`.
- `ValidationException` from `executeAndCollect`: the document failed the shared `WorkflowValidator`;
  the message names the field, identical to what the JVM runtime prints.
- `ResultTimeoutError` from `submit()`: the result did not reach the spool within `result_timeout`
  seconds (default 60). The message includes the job status; a `FAILED` job surfaces its exception
  as `RuntimeStateError` instead.
- Duplicate `turn_id` returns `status: duplicate` with the original reply/tool calls and an empty
  `events` list. That is the specified behaviour (`spec/conformance/v1/fixtures/08-duplicate-turn.yaml`),
  not a bug.

---

## `agentic-flink`: JPype facade over the Java framework

`python/` is a thin JPype-backed facade over the Java framework's builder API. The JVM runs
**in-process**; Python and Java share threads and calls cross JNI without serialization. It targets
the framework's own surface (`Agent.builder()`, tools, memory, retrieval, crawler), not the portable
document; use it for notebooks and scripts against the Flink-specific features.

### Install

```bash
pip install -e python            # JPype1 only
pip install -e "python[pyflink]" # adds apache-flink 2.x (2.2.1 matches the Java build)
```

The framework jar is discovered from `AGENTIC_FLINK_JAR`, the `jar_path=` kwarg to `start_jvm`, the
checkout's `target/agentic-flink-*.jar` (build with `mvn -DskipTests package`), or bundled package data.

### Runtime modes

`af.bootstrap()` picks a mode from the `AGENTIC_FLINK_MODE` env var:

| Mode       | What runs the work | Cluster needed? |
|------------|--------------------|-----------------|
| `inproc`   | JPype-managed JVM inside this Python process; operators are called directly via `rt.jclass(...)` | No |
| `session`  | A running Flink session cluster reached over REST at `FLINK_REST_URL`; `rt.submit_level(...)` submits | Yes (`examples-bin/run-session-cluster.sh` with Podman, or a remote host) |
| `embedded` | JPype JVM plus a transient in-process MiniCluster per `submit_level()` | No |

```python
import agentic_flink as af
rt = af.bootstrap()                # picks mode from AGENTIC_FLINK_MODE in .env
print(rt.info)
```

Ready-to-use `.env` files at the repo root: `.env.inproc.example`, `.env.cluster.local.example`,
`.env.cluster.remote.example`, `.env.embedded.example`. See `docs/compose.md` for the container side.

### Quick start

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
```

`@tool` makes a Python function callable as a Java `org.agentic.flink.tools.ToolExecutor` (Java
`Map<String, Object>` parameters become kwargs; the return value is wrapped in a
`CompletableFuture`; exceptions come back as `failedFuture`). `PyAgentEventListener` and the
`Guardrail`/`Classifier`/`Scorer`/`Chunker` interfaces are implemented the same way through JPype's
`@JImplements`.

### What's wrapped

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
| `agentic_flink.pyflink` | PyFlink-native plan (`AgentPlan` JSON -> `CompileUtils.attachAgent`); see `docs/pyflink-integration.md` |

Every wrapper exposes `_to_java()` to drop down to the live Java object.

### Examples and tests

```bash
python -m agentic_flink.examples.quickstart      # calculator tool + agent build, no LLM call
python -m agentic_flink.examples.rag             # sequential Python RAG
python -m agentic_flink.examples.live_research   # full PyFlink job: crawler + retrieve

mvn -DskipTests package && pip install -e "python[test]" && pytest python/tests/
```

### Troubleshooting

- **`FileNotFoundError: agentic-flink jar not found`**: set `AGENTIC_FLINK_JAR` or run `mvn -DskipTests package`.
- **`NoClassDefFoundError: org/slf4j/LoggerFactory`**: the shaded jar excludes `provided` Flink deps;
  pass the runtime classpath via `extra_jars=` to `start_jvm` (`mvn dependency:build-classpath -Dmdep.outputFile=cp.txt`).
- **`Initial state has no outgoing transitions`**: a custom `.with_state_machine(...)` must cover every non-terminal `AgentState`.
- **`TypeError: No matching overloads found`**: JPype is strict about boxed types; box explicitly with `af.jclass("java.lang.Long")(int(x))`.
