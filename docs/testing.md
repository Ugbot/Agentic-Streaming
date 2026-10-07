# Testing

How to run the unit, integration and conformance suites of this repository, and what each
of them requires. GitHub Actions is disabled on the repository, so the local runs described
here are the authoritative verification; `.github/workflows/ci.yml` and
`.github/workflows/conformance.yml` document the same commands for a hosted runner.

## Unit suites

The Maven reactor (`reactor/pom.xml`) builds every first-class JVM module in dependency
order. The default surefire run executes the `*Test.java` classes of each module and
excludes the `integration` and `djl` JUnit tags; it never runs `*IT.java` classes.

```
./mvnw -f reactor/pom.xml -DskipTests install   # build everything once
./mvnw -f reactor/pom.xml test                  # unit suites of every module
./mvnw test                                     # the Flink framework (root module) alone
./mvnw -f ports/jagentic-core/pom.xml test      # shared JVM core
./mvnw -f agentic-pekko/pom.xml test            # Pekko runtime
```

Two root-module classes are known to need a specific host setup and are excluded by name
on hosts that lack it: `ZeroMqChannelTest` has no per-test timeout and hangs on some hosts,
and `PythonExecutorTest` embeds CPython through PEMJA, which has no wheel for every Python
version. Exclude them explicitly when needed:

```
./mvnw -f reactor/pom.xml test -Dtest='!ZeroMqChannelTest,!PythonExecutorTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`-Dtest` replaces surefire's include patterns rather than narrowing them, so a pattern made
only of exclusions selects every test class, including `*IT.java`. When combining `-Dtest`
with the `integration-tests` profile add `!*IT` to the pattern so the integration classes run
once, through failsafe.

Tests that read example pipelines (`examples/pipelines/*.yaml`) locate the repository root
from the surefire `basedir` property, the working directory or the test class location
(`org.agentic.flink.testkit.RepoRoot`, `org.jagentic.core.RepoFixtures`,
`org.jagentic.pekko.testing.RepoFixtures`, `agentic.fixtures` in Clojure). A missing fixture
fails the test; it is never skipped. This holds both for a reactor build and for a module
built on its own.

The Clojure runtime is tested from `agentic-clj/`:

```
cd agentic-clj && clojure -X:test
```

If Maven Central answers with HTTP 429, point the wrapper at the Google mirror:

```
export MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
```

## Integration suites

Integration tests live in `*IT.java` classes and run through `maven-failsafe-plugin`, which
the `integration-tests` profile binds to the `integration-test` and `verify` phases in
`reactor/pom.xml`. The profile also lifts the `integration` tag exclusion from surefire for
the few `*Test.java` classes that carry it. The `djl` tag stays excluded (it downloads a
native PyTorch runtime; see the `djl-native` profile).

```
./mvnw -f reactor/pom.xml verify -P integration-tests   # whole reactor
./mvnw verify -P integration-tests                      # root module alone
./mvnw -f agentic-pekko/pom.xml verify -P integration-tests
./mvnw verify -P integration-tests -Dit.test=PgVectorStoreIT \
  -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false   # one class
```

Each class starts the service it needs through Testcontainers:

| Class | Service | Image |
| --- | --- | --- |
| `PostgresConversationStoreIT`, `PostgresA2ATaskStoreIT`, `StorageFactoryPostgresIT` | PostgreSQL | `postgres:16-alpine` |
| `PgVectorStoreIT` | PostgreSQL with pgvector | `pgvector/pgvector:pg16` |
| `QdrantVectorStoreIT` | Qdrant (gRPC) | `qdrant/qdrant:v1.13.2` |
| `RedisConversationStoreIT`, `RedisHotVectorIndexIT`, `A2ARedisTaskStoreIT`, `RedisA2ABridgeIT`, `RedisJournalIT` (Pekko) | Redis | `redis:7-alpine` |
| `FlussChannelIT`, `FlussConversationStoreIT` | Fluss (ZooKeeper, coordinator, tablet server) | `fluss/fluss:0.7.0`, `zookeeper:3.9.2` |

`StorageFactoryPostgresIT` also proves that both the `postgres` and the `postgresql` factory
aliases produce a usable `PostgresConversationStore`. The H2 database is no longer a test
dependency: every SQL-backed test runs against PostgreSQL.

If no container runtime is reachable, these classes fail with a message that names the
socket they looked for. They do not skip. The Fluss helper
(`org.agentic.flink.testkit.FlussTestCluster`) honours `FLUSS_BOOTSTRAP_SERVERS` for an
externally managed cluster and fails if that address does not answer.

### Podman

The project runs containers with rootless Podman. Testcontainers talks to Podman through its
Docker-compatible API socket:

```
podman system service --time=0 unix:///run/user/$(id -u)/podman/podman.sock &
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
```

`systemctl --user start podman.socket` is equivalent where user services are available.
Ryuk, the Testcontainers reaper side-car, needs a privileged container and Docker Hub
short-name resolution that rootless Podman does not provide by default; the integration
classes stop their containers in `@AfterAll`, so Ryuk is not needed. The Fluss cluster uses
host networking with random free ports because rootless Podman's CNI backend does not
reliably keep the user-defined networks Testcontainers would otherwise create.

Pull the images once if the box has limited bandwidth:

```
podman pull docker.io/library/postgres:16-alpine docker.io/pgvector/pgvector:pg16 \
  docker.io/library/redis:7-alpine docker.io/qdrant/qdrant:v1.13.2 \
  docker.io/library/zookeeper:3.9.2 docker.io/fluss/fluss:0.7.0
```

### Not covered by an integration test

- `MilvusVectorStore`: Milvus standalone is a multi-process deployment (Milvus plus etcd
  and an object store) with an image of more than a gigabyte, and the driver has no
  single-container mode this harness can start deterministically. It is compiled and
  discovered through `ServiceLoader` by `VectorStoreDiscoveryTest` but not exercised against
  a server.
- `StandaloneLLMIT` and `StandaloneToolExecutionIT`: need a local Ollama with the
  `qwen2.5:latest` model and are `@Disabled` with that reason. They are the only `*IT`
  classes that report as skipped.
- `DjlRecallIT`: downloads a native PyTorch runtime on first use; run it with
  `./mvnw test -P djl-native -Dtest=DjlRecallIT`.

## Conformance suite

The `agentic/v1` contract is checked by the fixtures under `spec/conformance/v1`. The
reference runtime and the schema validation need only Python with `jsonschema` and `pyyaml`:

```
python spec/tools/validate_spec.py      # schemas, primitives and fixtures are consistent
python spec/tools/run_conformance.py    # all fixtures against the reference runtime
python -m pytest spec/tools -q          # the spec tooling itself
```

The per-runtime conformance suites run as part of the unit suites above
(`FlinkConformanceTest`, `ConformanceTest` in jagentic-core, `PekkoConformanceTest`, the
Clojure and Python conformance modules). A fixture whose `requires` names a capability the
runtime does not declare is reported as skipped with a reason that names that capability;
this is a declared gap that appears in the generated `docs/capabilities.md`, not a missing
service. The full matrix across every binding present in the checkout is produced by:

```
python spec/tools/conformance_matrix.py --require reference --require-present \
  --logs build/conformance-logs --output build/conformance-matrix.json \
  --write-docs build/capabilities.md
```

`docs/capabilities.md` and the excerpts under `docs/runtimes/` are generated from that
output and must not be edited by hand.

## Skips

A skipped test is acceptable only when the skip reason describes a capability or service
that is deliberately not provided, and the reason is listed in `tools/ci/skip-allowlist.txt`:
declared conformance capability gaps, Ollama, and the two `agentic-pekko` tests behind
`AGENTIC_PEKKO_INTEGRATION`. `tools/ci/skip_audit.py` reads the surefire and failsafe XML
reports and fails for any other skip. A test must not skip because a fixture file is
missing or because a service that Testcontainers can start is unreachable.

Outside the Maven root module, `ports/jagentic-core` still has tests that probe optional
services (`StoreTest.qdrantColdTierIfAvailable`, `postgresLongTermIfAvailable`,
`redisConversationStoreIfAvailable`, `EmbeddingTest.ollamaEmbedderIfAvailable`,
`McpTest.mcpClientRegistersAndCallsTools`) and abort when the service is absent; CI
provisions those services on fixed ports. `tool-services-app` has the same pattern for
Redis and Kafka (`RedisToolBridgeTest`, `KafkaToolBridgeTest`). These skips are explained
by the service they probe and are the remaining candidates for the Testcontainers approach
described above.
