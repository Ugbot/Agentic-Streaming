# Getting started

This guide takes a fresh clone to a running agent. The project is experimental and pre-release;
read the status block in the [README](../README.md) first. Commands that do not work are listed as
such rather than left out. Part 4 needs Ollama or Podman on the machine.

The first path needs no model, no model download, no vector store and no container. The
workflow documents under `examples/pipelines/` describe a banking agent whose paths use either
a `rule` brain (deterministic keyword routing and tool triggers) or an `llm` brain driven by the
`stub` provider, which replays a scripted sequence of tool calls and text instead of calling a
model. The same document runs unchanged on the pure Python runtime and on the JVM local
runtime. Ollama, Qdrant and the compose stacks come later on this page and are optional.

## Part 1: A deterministic agent in two minutes

### The workflow document

`examples/pipelines/banking.yaml` uses `brain: rule` on every path:

```yaml
backend: local
agent:
  router:
    kind: keyword
    default: general
    rules:
      cards:    [card, crypto, cash-back, cashback]
      payments: [balance, transfer, payment, dispute, charge, limit]
  paths:
    payments:
      brain: rule
      prompt: You answer payment questions.
      tool_triggers: {balance: get_balance}
  verifier:
    kind: prefix
tools:
  - {id: get_balance, kind: constant, value: 1234.56}
```

`examples/pipelines/banking-llm.yaml` gives the `payments` path an `llm` brain and makes it
deterministic with the scripted stub provider. The `script` key is only read by the `stub`
provider; each step is either a tool call or the final text:

```yaml
llm:
  provider: stub
  model: qwen2.5:3b
  script:
    - {tool: get_balance, args: {user: demo}}
    - {text: "Your balance is 1234.56."}
agent:
  paths:
    payments: {brain: llm, prompt: You are a payments specialist., tools: [get_balance], verifier: {kind: none}}
```

Switching the same file to a real model is a two line change (`provider: ollama` or
`provider: openai`, see Part 4), which is why the examples and the tests use the stub.

### On the pure Python runtime

Requires Python 3.11 or newer (the packages declare 3.9 and 3.10 as their minimum; see
`docs/versioning.md`). No JVM is involved.

```bash
python -m venv .venv && . .venv/bin/activate
python -m pip install -e ports/pyagentic -e ports/agentic-pipeline
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking-llm.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "which card types do you offer?"
```

Output of the three runs:

```
backend=local path=payments ok=True
reply: [payments] get_balance returned 1234.56
tools: ['get_balance']

backend=local path=payments ok=True
reply: [payments] Your balance is 1234.56.
tools: ['get_balance']

backend=local path=cards ok=True
reply: [cards] We offer three card types: classic, gold, and platinum, each with different fees.
```

The first reply comes from the `rule` brain firing the `balance` tool trigger; the second from
the `llm` brain replaying the script (tool call, then text); the third from the retrieval block
of the same file, answered from the in-memory hashing embedder.

### On the JVM local runtime

Requires JDK 21. Nothing else: the wrapper downloads Maven.

```bash
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q -f ports/jagentic-core/pom.xml exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
./mvnw -q -f ports/jagentic-core/pom.xml exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="examples/pipelines/banking-llm.yaml --text 'what is my balance?'"
```

```
backend=local path=payments ok=true
reply: [payments] get_balance returned 1234.56
tools: [get_balance]

backend=local path=payments ok=true
reply: Your balance is 1234.56.
tools: [get_balance]
```

`docs/examples/banking-everywhere.md` shows the same agent on Pekko and Flink.

### The specification behind the file

The workflow documents are instances of the `agentic/v1` specification. Its reference runtime
is pure Python and its 24 conformance fixtures run in about a second, also without a model:

```bash
python spec/tools/validate_spec.py        # checked 33 document(s), 0 failure(s)
python spec/tools/run_conformance.py      # 31 passed, 0 failed, 0 skipped
```

Which runtime passes which fixture is in the [conformance results](runtimes/README.md#conformance-results).

## Part 2: The JVM build

### Java 21 and the Maven wrapper

The whole repository targets Java 21; older JDKs fail at compile time. A JDK 21 tarball is at
https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse; unpack
it and set `JAVA_HOME`.

Do not install Maven; use the committed wrapper. The build runs the Enforcer plugin and rejects
Maven older than 3.9, so a distribution-packaged `mvn` (3.6.3 on Ubuntu 22.04) fails with
`Use the committed wrapper (./mvnw) or Maven 3.9+.`

`./mvnw` downloads the pinned Maven on first use. If `repo.maven.apache.org` is unreachable or
rate limited (HTTP 429), point the wrapper and the build at the Google mirror:

```bash
export MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
./mvnw -s .mvn/settings-mirror.xml ...   # a settings file with <mirrorOf>central</mirrorOf>
```

where the settings file declares one mirror with `<mirrorOf>central</mirrorOf>` and the same
URL. The repository does not ship that file; write it locally when you need it.

### The reactor

`reactor/pom.xml` is the parent and aggregator of every first-class JVM module:
`ports/jagentic-core`, the Flink framework (the root `pom.xml`, artifact `agentic-flink`),
and `agentic-pekko`. One command builds and tests all of them in dependency order, with one groupId (`org.jagentic`) and one version
(`1.0.0-SNAPSHOT`):

```bash
./mvnw -f reactor/pom.xml install -DskipTests       # compile and install, no tests
./mvnw -f reactor/pom.xml verify                    # everything CI gates on
```

`package` and later phases also attach a `-sources.jar` and a `-javadoc.jar` next to every
module jar.

A single module builds with `-f <module>/pom.xml` once the modules it depends on are in the
local repository. The Flink module and Pekko both depend on `ports/jagentic-core`, so install
it first (this also installs the reactor parent the module refers to):

```bash
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q clean package -DskipTests                 # the Flink module, from the repository root
```

The second command produces two jars under `target/`:

- `agentic-flink-1.0.0-SNAPSHOT.jar`: the thin jar of framework classes that other Maven
  modules depend on.
- `agentic-flink-1.0.0-SNAPSHOT-uber.jar`: the shaded jar for `flink run`. It does not contain
  Flink itself: `flink-streaming-java` and `flink-clients` are `provided` scope and are
  excluded from the shade, because a Flink cluster supplies them. That is why running it
  with a bare `java -cp` fails with `NoClassDefFoundError`.

### The Flink tests

```bash
./mvnw test
```

Runs the framework's unit tests inside the Flink MiniCluster; the suite uses stub brains and
needs no model or service. Formatting is checked in the `verify` phase, not here; run
`./mvnw spotless:apply` before committing Java changes (see `CONTRIBUTING.md`).

## Part 3: Run an agent on Flink

The two paths that work on a fresh clone are both tests, because they run inside the Flink
MiniCluster with the test classpath.

### The shared fixtures on the Flink binding

```bash
./mvnw -q test -Dtest=FlinkConformanceTest
```

Result on 2026-11-06 (the class runs the 31 fixtures plus two checks of its own):

```
Tests run: 33, Failures: 0, Errors: 0, Skipped: 0
```

A skip, when there is one, is never a pass.

### A `pipeline.yaml` through the Flink runner

`FlinkPipelineRunner` assembles a real Flink job from a pipeline file: source, optional
native CEP, `keyBy` on the conversation, the portable graph in a keyed operator, sink.
One turn through it on a local MiniCluster:

```bash
./mvnw -q -P examples compile exec:exec -Dexec.mainClass=org.agentic.flink.pipeline.FlinkPipelineRunner \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

```
c1 | path=payments | ok=true | [payments] get_balance returned 1234.56
```

`FlinkPipelineRunnerTest` drives the same file:

```bash
./mvnw -q test -Dtest=FlinkPipelineRunnerTest -Dsurefire.failIfNoSpecifiedTests=false
```

```
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

Read `src/test/java/org/agentic/flink/pipeline/FlinkPipelineRunnerTest.java` to see how the
runner is called; it is the reference for embedding the runner in your own job.

### Durability

`WorkflowTurnFunctionMiniClusterTest` covers savepoint restart, checkpoint recovery after a failure
that follows a checkpoint, and a registered timer surviving a savepoint restart. It does not cover a
turn that ran after the last completed checkpoint; that turn runs again, tools included:

```bash
./mvnw -q test -Dtest=WorkflowTurnFunctionMiniClusterTest
```

```
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
```

## Part 4: Optional: a real model, a vector store, containers

Nothing above needs any of this. Skip the section until you want a live LLM or an external store.

### Ollama

Ollama is only needed for `provider: ollama` in a workflow document.

```bash
ollama serve
ollama pull qwen2.5:3b
ollama pull nomic-embed-text
```

To point `examples/pipelines/banking-llm.yaml` at a model instead of the script, change the `llm`
block to `provider: ollama` and `model: qwen2.5:3b`; `provider: openai` reads `OPENAI_API_KEY`.
[using a real model](portability/using-a-real-model.md) has the keys.

### Qdrant, Postgres, Valkey

Only a document that declares them and the service-backed tests need them. Use Podman:

```bash
podman run -d -p 6333:6333 qdrant/qdrant
podman run -d -p 5432:5432 -e POSTGRES_PASSWORD=agentic pgvector/pgvector:pg16
podman run -d -p 6379:6379 valkey/valkey
```

Tests that need these skip cleanly when they are not running. In CI they are not allowed to:
the workflow provisions the services and `tools/ci/skip_audit.py` fails the job for any skip
whose reason is not listed in `tools/ci/skip-allowlist.txt`; every entry there states why
(declared conformance capability gaps, Ollama, and the two `AGENTIC_PEKKO_INTEGRATION`
tests the workflow explains). To run the service-backed tests locally, start the services and
export the same variables the `core` job in `.github/workflows/ci.yml` sets
(`AGENTIC_TEST_PG_URL`, `AGENTIC_TEST_REDIS_URL`, `AGENTIC_KAFKA_BOOTSTRAP`, ...).

### Testcontainers through Podman

The Pekko Testcontainers suite (`RedisJournalIT`) starts its own container through the Docker API.
With Podman, start the compatibility socket and point Testcontainers at it:

```bash
systemctl --user enable --now podman.socket        # or: podman system service --time=0 &
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true           # Ryuk needs a privileged container and Docker Hub short names; the tests stop their containers themselves
./mvnw -f agentic-pekko/pom.xml verify -P integration-tests
```

GitHub-hosted runners provide Docker, which Testcontainers finds without configuration; both
runtimes run the same tests.

### Compose stacks

`docker-compose.yml` holds Postgres, Redis and Ollama, and `docker-compose-kafka.yml` and
`docker-compose-session.yml` add Kafka and a Flink session cluster. `docker-compose-all.yml` includes
the lot. Each file's header says how to start it. `tools/smoke-examples.sh` runs `examples/pipelines/` in no-key mode and needs none of
them.

## Part 5: The other runtimes

Pekko, after the core install from Part 2:

```bash
./mvnw -q -f agentic-pekko/pom.xml compile exec:java \
  -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

Without `compile` in the same invocation the class is not on the exec classpath and the
command fails with `ClassNotFoundException: org.jagentic.pekko.PipelineMain`.

Python beyond the pipeline CLI: the two levels of the Python API (high-level `run(...)` and
full-control `Runtime.deploy(spec)`) are in [python.md](python.md).

What each of these runtimes is made of, and what they all guarantee for an `agentic/v1`
workflow, is one page each under [runtimes/](runtimes/README.md).

## Next steps

1. [the banking agent on every runtime](examples/banking-everywhere.md) for one document on each
   runtime.
2. [`pipeline.yaml`](portability/pipelines.md) for the document's schema.
3. [runtimes/](runtimes/README.md) for what each runtime is made of, and its
   [conformance results](runtimes/README.md#conformance-results).

## Common questions

### The build failed

1. `java -version` must show 21 or later.
2. Use `./mvnw`, not `mvn`.
3. Did you run the `ports/jagentic-core` install first? Without it a `./mvnw` run at the
   repository root fails to resolve `org.jagentic:jagentic-core`. `./mvnw -f reactor/pom.xml verify`
   needs no such step.
4. Maven downloads dependencies on first use; see the mirror note in Part 2 if Central is
   unreachable.

### Can I use OpenAI instead of Ollama?

Yes: `provider: openai` in the document's `llm:` block. See
[using a real model](portability/using-a-real-model.md).

### How do I see more logging?

Add to `src/main/resources/log4j2.properties`:

```
logger.agent.name = org.agentic.flink
logger.agent.level = DEBUG
```
