# Getting started

This guide takes a fresh clone to a running agent. Every command on this page was run on a
clean checkout with Java 21 and the committed Maven wrapper; the ones that do not work are
listed as such rather than left out.

> **Pick your runtime.** The fastest path (no JVM, no infrastructure) is the Python one:
> ```bash
> python -m pip install -e ports/pyagentic
> PYTHONPATH=ports/agentic-pipeline \
> python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
> ```
> For the same banking agent on Flink, Pekko, Clojure, Python and Go, and on the experimental
> adapters under `ports/`, see [the banking agent on every runtime](examples/banking-everywhere.md).
> The rest of this page is the JVM and Flink path.

## What you will do

- Install Java 21 and check the Maven wrapper.
- Build `ports/jagentic-core` first, then the Flink module.
- Run the shared conformance fixtures against the Flink binding.
- Run a `pipeline.yaml` through the Flink runner from a test.
- Know which example entry points work today and which do not.

## Part 1: Setup

### Step 1: Java 21

The whole repository targets Java 21. Older JDKs fail at compile time; a JDK 17 box needs a
21 install alongside it.

```bash
java -version
```

You should see `openjdk version "21.` or later. If not, install a JDK 21 from
https://adoptium.net/ (or unpack the tarball from
https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse and set
`JAVA_HOME` to it).

### Step 2: Maven

Do not install Maven; use the committed wrapper. The root `pom.xml` runs the Enforcer plugin
and rejects Maven older than 3.9, so a distribution-packaged `mvn` (3.6.3 on Ubuntu 22.04)
fails with:

```
Use the committed wrapper (./mvnw) or Maven 3.9+.
```

`./mvnw` downloads the pinned Maven on first use. If `repo.maven.apache.org` is unreachable
or rate limited, point the wrapper and the build at the Google mirror:

```bash
export MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
./mvnw -s .mvn/settings-mirror.xml ...   # a settings file with <mirrorOf>central</mirrorOf>
```

where the settings file declares one mirror with `<mirrorOf>central</mirrorOf>` and the same
URL. The repository does not ship that file; write it locally when you need it.

### Step 3: Ollama (optional)

The unit tests, the conformance fixtures, and the Python one-liner use stub brains and need
no model. Ollama is only needed for the examples that call a live LLM.

```bash
ollama serve
ollama pull llama3.1:latest
ollama pull nomic-embed-text
```

`SimpleAgentExample` and `QuickStartExample` are configured for `llama3.1:latest` at
`http://localhost:11434`, but neither reaches Ollama on a fresh clone; see Part 4.

### Step 4: Qdrant, Postgres, Valkey (optional)

Only the RAG and storage examples and the integration tests need them. Use Podman:

```bash
podman run -d -p 6333:6333 qdrant/qdrant
podman run -d -p 5432:5432 -e POSTGRES_PASSWORD=agentic postgres:16
podman run -d -p 6379:6379 valkey/valkey
```

Tests that need these skip cleanly when they are not running. In CI they are not allowed to:
the workflow provisions the services and `tools/ci/skip_audit.py` fails the job for any skip
whose reason is not listed in `tools/ci/skip-allowlist.txt`; every entry there states why
(declared conformance capability gaps, Ollama, Fluss, and the two `AGENTIC_PEKKO_INTEGRATION`
tests the workflow explains). To run the service-backed tests locally, start the services and
export the same variables the `core` job in `.github/workflows/ci.yml` sets
(`AGENTIC_TEST_PG_URL`, `AGENTIC_TEST_REDIS_URL`, `AGENTIC_KAFKA_BOOTSTRAP`, ...).

The Testcontainers suites (`./mvnw test -P integration-tests`: `Postgres*Test`, `Redis*IT`)
start their own containers through the Docker API. With Podman, start the compatibility
socket and point Testcontainers at it; `src/test/resources/testcontainers.properties`
already disables the startup checks Podman does not implement:

```bash
systemctl --user enable --now podman.socket        # or: podman system service --time=0 &
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true           # Ryuk needs a privileged container and Docker Hub short names; the tests stop their containers themselves
./mvnw test -P integration-tests
```

GitHub-hosted runners provide Docker, which Testcontainers finds without configuration; both
runtimes run the same tests.

## Part 2: Build

### Step 1: The reactor

`reactor/pom.xml` is the parent and aggregator of every first-class JVM module: `ports/jagentic-core`,
the Flink framework (the root `pom.xml`), `agentic-pekko`, `pyflink/java`, `tool-services/*`
and `banking-job`. One command builds and tests all of them in dependency order, with one
groupId (`org.jagentic`) and one version (`1.0.0-SNAPSHOT`):

```bash
./mvnw -f reactor/pom.xml verify                    # everything CI gates on
./mvnw -f reactor/pom.xml verify -P a2a-gateway     # plus the Quarkus A2A gateway (slow, opt-in)
./mvnw -f reactor/pom.xml install -DskipTests       # compile and install, no tests
```

The root `pom.xml` stays the Flink module, so every `./mvnw ...` command run from the
repository root below still addresses the framework. The adapters under `ports/experimental/`
are not part of the reactor; each has its own `pom.xml` and is built on its own.

### Step 1b: Building one module at a time

A single module builds with `-f <module>/pom.xml` once the modules it depends on are in the
local repository. The Flink module and Pekko both depend on `ports/jagentic-core`, so install
it first (this also installs the reactor parent pom the module refers to):

```bash
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
```

### Step 2: Build the Flink module

```bash
./mvnw -q clean package -DskipTests
```

This produces two jars under `target/`:

- `agentic-flink-1.0.0-SNAPSHOT.jar`: the thin jar of framework classes that other Maven
  modules depend on.
- `agentic-flink-1.0.0-SNAPSHOT-uber.jar`: the shaded jar for `flink run`. It does not contain
  Flink itself: `flink-streaming-java` and `flink-clients` are `provided` scope and are
  excluded from the shade, because a Flink cluster supplies them. That is why running it
  with a bare `java -cp` fails (Part 4).

### Step 3: Run the tests

```bash
./mvnw test
```

Result on a fresh clone (2026-09-14):

```
Tests run: 818, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Part 3: Run an agent on Flink

The two paths that work on a fresh clone are both tests, because they run inside the Flink
MiniCluster with the test classpath.

### The shared fixtures on the Flink binding

```bash
./mvnw -q test -Dtest=FlinkConformanceTest
```

```
Tests run: 24, Failures: 0, Errors: 0, Skipped: 7
```

The 7 skips are the fixtures whose capabilities the Flink binding declares `unsupported`
(`docs/capabilities.md` lists them). A skip is never a pass.

### A `pipeline.yaml` through the Flink runner

`FlinkPipelineRunner` assembles a real Flink job from a pipeline file: source, optional
native CEP, `keyBy` on the conversation, the portable graph in a keyed operator, sink.
`FlinkPipelineRunnerTest` drives `examples/pipelines/banking.yaml` through it:

```bash
./mvnw -q test -Dtest=FlinkPipelineRunnerTest -Dsurefire.failIfNoSpecifiedTests=false
```

```
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

Read `src/test/java/org/agentic/flink/pipeline/FlinkPipelineRunnerTest.java` to see how the
runner is called; it is the reference for embedding the runner in your own job.

### Durability

Crash and restart durability on Flink is proven by
`WorkflowTurnFunctionMiniClusterTest` (savepoint restart, checkpoint recovery after failure,
and a registered timer surviving a savepoint restart):

```bash
./mvnw -q test -Dtest=WorkflowTurnFunctionMiniClusterTest
```

## Part 4: Example entry points that do not work from a fresh clone

These are the commands earlier versions of this page told you to run. Each was tried on a
fresh clone on 2026-09-14 and each fails. They are listed so nobody wastes time on them; the
failures are tracked in [`docs/audit-backlog.md`](audit-backlog.md) under AGS-40.

| Command | Failure |
|---|---|
| `java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar org.agentic.flink.example.SimpleAgentExample` | `NoClassDefFoundError: org/apache/flink/configuration/ReadableConfig`. The uber jar excludes the provided Flink runtime. |
| `./mvnw -q compile exec:java -Dexec.mainClass=org.agentic.flink.example.SimpleAgentExample -Dexec.classpathScope=provided` | `NoClassDefFoundError: org/apache/flink/connector/datagen/source/GeneratorFunction`. `flink-connector-datagen` is test scope. |
| the same with `test-compile` and `-Dexec.classpathScope=test` | `Object org.agentic.flink.stream.AgentExecutionStream$$Lambda ... is not serializable` during job graph construction. |
| `./mvnw -q test-compile exec:java -Dexec.mainClass=org.agentic.flink.example.QuickStartExample -Dexec.classpathScope=test` | `Initial state has no outgoing transitions` from `AgentBuilder.build()`, before any Ollama call. |
| `./mvnw -q test-compile exec:java -Dexec.mainClass=org.agentic.flink.pipeline.FlinkPipelineRunner -Dexec.classpathScope=test -Dexec.args=examples/pipelines/banking.yaml` | `Could not deserialize stream node 4: ... SimpleUdfStreamOperatorFactory` at job submission. The same runner passes inside `FlinkPipelineRunnerTest`. |

There is no `MyFirstAgentExample` in the repository; an earlier version of this page invented
it. `RagAgentExample` and `ContextManagementExample` exist but have the same uber-jar problem
as `SimpleAgentExample`.

## Part 5: The other first-class runtimes

Pekko, after the core install from Part 2:

```bash
./mvnw -q -f agentic-pekko/pom.xml compile exec:java \
  -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

Without `compile` in the same invocation the class is not on the exec classpath and the
command fails with `ClassNotFoundException: org.jagentic.pekko.PipelineMain`.

Clojure:

```bash
cd agentic-clj && clojure -M:run
```

This runs the banking demo (`agentic.main`) against the in-process Datomic store; `clojure -X:test`
runs the suite, including the conformance fixtures.

Python is the block at the top of this page. `ports/agentic-pipeline` has no package metadata,
so it is not pip-installable and must be on `PYTHONPATH`; `ports/pyagentic` is installable.
The two levels of the Python API (high-level `run(runtime=...)` and full-control
`Runtime.deploy(spec)`) on pure Python, the JVM facade and PyFlink are in [python.md](python.md).

What each of these runtimes is made of, and what they all guarantee for an `agentic/v1`
workflow, is one page each under [runtimes/](runtimes/README.md).

## Next steps

1. [concepts.md](concepts.md) for how agents, tools, and events fit together.
2. [reference/examples.md](reference/examples.md) for the example catalogue.
3. [reference/agent-framework.md](reference/agent-framework.md) for the framework API.
4. [reference/troubleshooting.md](reference/troubleshooting.md) for common failures.
5. [capabilities.md](capabilities.md) for what each runtime has proven, and
   [runtimes/](runtimes/README.md) for what each runtime is made of.

## Common questions

### The build failed

1. `java -version` must show 21 or later.
2. Use `./mvnw`, not `mvn`.
3. Did you run the `ports/jagentic-core` install first? Without it a `./mvnw` run at the
   repository root fails to resolve `org.jagentic:jagentic-core`. `./mvnw -f reactor/pom.xml verify`
   needs no such step.
4. Maven downloads dependencies on first use; see the mirror note in Part 1 if Central is
   unreachable.

### Can I use OpenAI instead of Ollama?

Yes. The LLM provider is configured per agent (`AgentConfig.setLlmModel` and
`addLlmProperty` in the code-first DSL, or `llm:` in a pipeline file). See
[configuration.md](configuration.md).

### How do I see more logging?

Add to `src/main/resources/log4j2.properties`:

```
logger.agent.name = org.agentic.flink
logger.agent.level = DEBUG
```
