# Testing

How to run the unit, integration and conformance suites of this repository, and what each
of them requires. `.github/workflows/ci.yml` and `.github/workflows/conformance.yml` run the same
commands on a hosted runner.

## Unit suites

The Maven reactor (`reactor/pom.xml`) builds every first-class JVM module in dependency
order. The default surefire run executes the `*Test.java` classes of each module; it never runs
`*IT.java` classes.

```
./mvnw -f reactor/pom.xml -DskipTests install   # build everything once
./mvnw -f reactor/pom.xml test                  # unit suites of every module
./mvnw test                                     # the Flink framework (root module) alone
./mvnw -f ports/jagentic-core/pom.xml test      # shared JVM core
./mvnw -f agentic-pekko/pom.xml test            # Pekko runtime
```

`-Dtest` replaces surefire's include patterns rather than narrowing them, so a pattern made
only of exclusions selects every test class, including `*IT.java`. When combining `-Dtest`
with the `integration-tests` profile add `!*IT` to the pattern so the integration classes run
once, through failsafe.

Tests that read example pipelines (`examples/pipelines/*.yaml`) locate the repository root
from the surefire `basedir` property, the working directory or the test class location
(`org.agentic.flink.testkit.RepoRoot`, `org.jagentic.core.RepoFixtures`,
`org.jagentic.pekko.testing.RepoFixtures`). A missing fixture
fails the test; it is never skipped. This holds both for a reactor build and for a module
built on its own.

If Maven Central answers with HTTP 429, point the wrapper at the Google mirror:

```
export MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
```

## Integration suites

Integration tests live in `*IT.java` classes and run through `maven-failsafe-plugin`, which
the `integration-tests` profile binds to the `integration-test` and `verify` phases in
`reactor/pom.xml`. Only `agentic-pekko` has any.

```
./mvnw -f reactor/pom.xml verify -P integration-tests   # whole reactor
./mvnw -f agentic-pekko/pom.xml verify -P integration-tests
```

`RedisJournalIT` starts Redis (`redis:7-alpine`) through Testcontainers. If no container runtime is
reachable it fails with a message that names the socket it looked for. It does not skip.

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
classes stop their containers in `@AfterAll`, so Ryuk is not needed.

## Conformance suite

The `agentic/v1` contract is checked by the fixtures under `spec/conformance/v1`. The
reference runtime and the schema validation need only Python with `jsonschema` and `pyyaml`:

```
python spec/tools/validate_spec.py      # schemas, primitives and fixtures are consistent
python spec/tools/run_conformance.py    # all fixtures against the reference runtime
python -m pytest spec/tools -q          # the spec tooling itself
```

The per-runtime conformance suites run as part of the unit suites above
(`FlinkConformanceTest`, `ConformanceTest` in jagentic-core, `PekkoConformanceTest`, and
`agentic.conformance` in Python). A fixture whose `requires` names a capability the
runtime does not declare is reported as skipped with a reason that names that capability;
this is a declared gap that appears in the matrix, not a missing service. The full matrix across every binding present in the checkout is produced by:

```
python spec/tools/conformance_matrix.py --require reference --require-present \
  --logs build/conformance-logs --output build/conformance-matrix.json \
  --write-docs build/capabilities.md
```

No copy of that output is committed; see
[conformance results](runtimes/README.md#conformance-results).

## Skips

A skipped test is acceptable only when the skip reason describes a capability or service
that is deliberately not provided, and the reason is listed in `tools/ci/skip-allowlist.txt`:
declared conformance capability gaps, Ollama, and the two `agentic-pekko` tests behind
`AGENTIC_PEKKO_INTEGRATION`. `tools/ci/skip_audit.py` reads the surefire and failsafe XML
reports and fails for any other skip. A test must not skip because a fixture file is
missing or because a service that Testcontainers can start is unreachable.

`ports/jagentic-core` has tests that probe optional services
(`StoreTest.qdrantColdTierIfAvailable`, `pgvectorColdTierIfAvailable`, `postgresLongTermIfAvailable`,
`redisConversationStoreIfAvailable`, `EmbeddingTest.ollamaEmbedderIfAvailable`,
`McpTest.mcpClientRegistersAndCallsTools`) and abort when the service is absent; CI
provisions those services on fixed ports. These skips are explained
by the service they probe and are the remaining candidates for the Testcontainers approach
described above.
