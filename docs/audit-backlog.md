# Audit backlog

Findings from the full-project audit of 2026-07-25, kept here because the
SQLite tracker that originally held them was destroyed on 2026-07-26 (an
unrelated incident wiped most of the home directory). The eleven epics survived
as GitHub issues; the thirty-three sub-tasks below existed only in that
database. **This file is now the source of truth.** If the tracker is rebuilt,
rebuild it from here.

Every claim below was verified against the code at the time of the audit,
file:line citations are literal, and measured numbers come from actual runs, not
estimates. Where an audit track was wrong, the correction is noted inline.

## Ground rules that shaped this list

Decisions from the owner, which change what counts as a gap:

- **All 17 `ports/` are products**, not reference material. Thinness in a port
  is a gap to close, not a deliberate illustration.
- **Native runtime state + checkpoints satisfies "state as a materialized
  view."** A separate durable append-only event log is *not* required on any
  runtime. So the work is proving recovery actually functions and is tested,
  not building a log. Do not file tickets demanding one.
- **Publishing target is Maven Central + PyPI**, public. Packaging and release
  gaps are P1 blockers, not backlog.
- **Java 21 is the baseline** for every JVM module (all 13 poms currently
  declare 17). Java 25 was considered and rejected: Flink 2.x supports 17/21,
  and the pinned Lombok 1.18.38 predates JDK 25 support.

## Measured baseline (JDK 21, after AGS-9)

| Surface | Result |
|---|---|
| `agentic-flink` (main module) | **770 tests**, 0 failures, 0 skipped |
| `ports/jagentic-core` | 95 tests, 0 failures, 5 skipped (external infra) |
| `ports/{kafka-streams,temporal,pekko,pulsar}` | 2 tests each |
| `ports/{spring,quarkus}` | compile only. **0 tests** |
| `agentic-pekko` | 14 tests |
| `tool-services` (packs + app) | 15 + 11 tests |
| `a2a-gateway` | 9 tests |
| `banking-job` | no, sources (uber-jar assembly only) |
| **JVM total in CI** | **922 tests, 0 failures, 7 skipped** |
| `ports/go` | `go vet` clean, 6/6 packages pass |
| `agentic-clj` | 64 tests / 249 assertions |
| `ports/pyagentic` | 88 passed, 5 skipped |
| `ports/tests` (adapters) | 7 passed, 1 skipped |

`TEST_REPORT.md` (dated 2026-05-26, claiming 487 tests on Java 17) is stale.

## Epic → GitHub issue map

The `uid` is the tracker's stable identity, recovered from the marker in each
issue body. Reuse these uids when rebuilding so the existing issue markers keep
resolving instead of renumbering.

| Key | Issue | Title | uid |
|---|---|---|---|
| AGS-1 | [#1](https://github.com/Ugbot/Agentic-Streaming/issues/1) | Build & release plumbing | `be98960938e041ff9594d769abe19d6a` |
| AGS-2 | [#2](https://github.com/Ugbot/Agentic-Streaming/issues/2) | Java 21 baseline | `5db51bb487ff4e9cb0c20c29cabac351` |
| AGS-3 | [#3](https://github.com/Ugbot/Agentic-Streaming/issues/3) | CI that builds and tests everything | `9aea59de6d84480597fcedb4067cc7f4` |
| AGS-4 | [#4](https://github.com/Ugbot/Agentic-Streaming/issues/4) | Overclaimed capabilities | `95c30c3adb9c4532821a98514e71a4ab` |
| AGS-5 | [#5](https://github.com/Ugbot/Agentic-Streaming/issues/5) | Correctness bugs | `4c7cf1df7137475b9e2f62a97aea77e1` |
| AGS-6 | [#6](https://github.com/Ugbot/Agentic-Streaming/issues/6) | Paradigm completion (Flink fault tolerance) | `56c40bdd7d5b42769659b1997526f9e6` |
| AGS-7 | [#7](https://github.com/Ugbot/Agentic-Streaming/issues/7) | API surface / dead code | `6d3508d3b80e418ab32080591c44de60` |
| AGS-8 | [#8](https://github.com/Ugbot/Agentic-Streaming/issues/8) | Test integrity | `bac8fa7ebb70442483189e5427ebd6c5` |
| AGS-17 | [#9](https://github.com/Ugbot/Agentic-Streaming/issues/9) | Ports productization | `da3d5d9d461c4acda53645392c5b79fc` |
| AGS-34 | [#10](https://github.com/Ugbot/Agentic-Streaming/issues/10) | Table stakes for a public library set | `a3f6ee43ddfb48f3bd02df70b2058f0c` |
| AGS-39 | [#11](https://github.com/Ugbot/Agentic-Streaming/issues/11) | Docs truth | `20bfcb40a2d141e38df18df11bf7a856` |

---

# AGS-1: Build & release plumbing (P1)

## AGS-9. Fix jagentic-core compile error DONE (`f39e6a1`, PR #12)

`Retrieval.java:77` declared `removeEldestEntry(Map.Entry<String, Entry>)` inside
an anonymous `LinkedHashMap` subclass, where a bare `Entry` resolves to the
inherited `java.util.Map.Entry` rather than the enclosing record, an erasure
clash javac rejects. Fixed by qualifying as `InMemoryHotVectorIndex.Entry`.
Since jagentic-core is a hard compile-scope dep of the main module
(`pom.xml:90-94`), nothing in the JVM tree built from a clean checkout.
Introduced in `0424ea3`. Added `hotIndexEvictsBeyondItsCapacityBound` to pin the
capacity contract (javac catches the signature; the test catches the policy).

## AGS-10: Create a real Maven reactor with a shared parent POM (P1)

13 poms, **zero `<modules>` blocks anywhere**. Every module builds via an
explicit `-f`. Measured consequence: `banking-job`, `tool-services-app` and
`a2a-gateway` all fail until their upstreams are manually `install`ed in the
right order; run the chain by hand and all three pass, so this is ordering
fragility, not broken code.

Coordinate sprawl to fix at the same time: 4 unrelated groupIds
(`org.agentic.flink`, `org.jagentic`, `org.jagentic.ports`, `org.jagentic.pekko`),
2 unsynchronised version lines (`1.0.0-SNAPSHOT` vs `0.1.0`), and a **duplicate
artifactId**,`agentic-pekko` exists at both `org.jagentic.pekko:agentic-pekko`
and `org.jagentic.ports:agentic-pekko`. Modules on non-SNAPSHOT `0.1.0` are
silently overwritten by every local `install`, claiming a release that was never
cut (0 git tags).

## AGS-11: Move to a verifiable groupId/namespace (P1): **the bottleneck**

Central requires namespace ownership proof. `org.agentic.flink` implies
flink.agentic.org; `org.jagentic` implies jagentic.org. The project is at
`github.com/Ugbot/Agentic-Streaming`, so neither is verifiable. Putting `flink`
in a groupId you don't own is also Apache trademark exposure. Recommend
`io.github.ugbot.agentic.*`. Also fix `<name>Agentic Flink</name>` in the root
pom (pre-rename).

Same class of problem elsewhere:
- `ports/go/go.mod:1` is `module github.com/jagentic/goagentic`, that repo
  **404s**, so `go get` can never work. Only importable path is
  `github.com/Ugbot/Agentic-Streaming/ports/go`.
- `ports/pyagentic/pyproject.toml:6` claims PyPI name `pyagentic`, which is
  **already taken** (PyAgentic 0.0.2, uploaded 2023-11-16). Upload rejected.

Nearly the whole release path is blocked on this decision.

## AGS-12: Add Central release plumbing and POM metadata (P1)

Zero of 13 poms contain `distributionManagement`, `maven-source-plugin`,
`maven-javadoc-plugin`, `maven-gpg-plugin`, `central-publishing-maven-plugin`, or
`maven-release-plugin`. All are missing the metadata Central rejects on:
`<url>`, `<licenses>`, `<developers>`, `<scm>`. `mvn deploy` cannot succeed for
any module. 0 git tags.

Do it in the parent from AGS-10 so it is inherited once.

**Leave the shading alone, it is already correct.** The root pom sets
`shadedArtifactAttached` + `shadedClassifierName=uber`, so the main artifact
stays thin (~1.3 MB) with a proper dependency-reduced POM and the fat jar is a
classified `-uber` secondary (~260 MB). The "127 MB shaded jar" in
`TEST_REPORT.md` is a stale pre-split figure.

## AGS-29: Python packaging (P1)

Repo-wide, `find` locates only **two** `pyproject.toml` (`python/`,
`ports/pyagentic/`) and **zero** `py.typed`, despite dense type hints in both.

`pyagentic` is the strongest Python surface (30 modules, zero runtime deps,
imports clean on 3.9-3.14, 93 tests passing with no JVM/broker/container) and has
the weakest packaging: an 18-line pyproject with no readme/license/authors/
classifiers/urls, no README or LICENSE in the directory (its PyPI page would
render blank), no extras for optional backends (redis/qdrant/litellm/psycopg/
duckdb/mcp/yaml), no publish workflow. **and the name is taken** (see AGS-11).

**9 port directories have no packaging at all** (~2,200 LOC):
`agentic-pipeline`, `gateway-fastapi`, `airflow`, `celery`, `dask`, `faust`,
`nats`, `ray`, `tests`. All twelve consumers reach pyagentic via
`sys.path.insert`; 5 of those are in *library* code
(`gateway-fastapi/backends.py:27,110,159`, `agentic-pipeline/loader.py:19`).
Note `agentic-pipeline` hosts the README's headline quick-start command, so the
single fastest advertised on-ramp is unpackaged.

`agentic-clj` likewise has only `deps.edn`, no pom/build.clj, no Clojars
target, no artifact coords. Consumable only via `:local/root` or `:git/url`.
(Its suite is healthy: 64 tests / 249 assertions.)

*Partially addressed:* PyYAML was added to pyagentic's `test` extra in the CI
commit, fixing the one clean-machine failure.

## AGS-30: `agentic-flink` wheel is non-functional (P1)

`pip install agentic-flink` is documented (`docs/python.md:61`,
`python/README.md:8`) but **PyPI 404s**, it has never published.

- **Trusted publisher points at the pre-rename repo.** Fixed the URLs and the
  workflow comments in the CI commit, but the publisher itself **must be
  re-registered on pypi.org** against `Agentic-Streaming`. PyPI matches the
  OIDC `repository` claim exactly. Cannot be fixed from the repo.
- **Jar discovery picks the thin jar, not `-uber`.** `_jvm.py:130-136`,
  `runtime.py:552-557` and `:208-215` glob `agentic-flink-*.jar`, filter on
  `"original-" not in name` (a no-op, since `shadedArtifactAttached` never emits
  `original-*`) and take the last sorted entry, the thin jar. Masked in
  inproc/embedded; **fatal in session mode**, which uploads it to the cluster →
  `ClassNotFoundException`. The comment at `_jvm.py:135` claims the opposite of
  what the code does.
- **Undeclared deps:** only `JPype1` is declared. `requests` is a top-level
  import in `session.py:20-26` (hard `ImportError`, kills all of session mode);
  `pyzmq` for `session.py:230`; `python-dotenv` for `runtime.py:523`; notebooks
  also need `websockets`, `nest_asyncio`, `kafka-python`.
- **No jar can ever ship in the wheel**: `docs/python.md:78` advertises bundled
  package-data discovery and `_jvm.py:139-143` implements it, but there is no
  `package-data`/`MANIFEST.in`. So the wheel only works for someone who already
  built Maven artifacts, i.e. someone who doesn't need it.
- Delete the committed stale classpath cache `python/tests/.cp` (234 absolute
  paths pinning Flink 1.20.1, none of which exist, proof the suite has not run
  since the 2.2 bump).

## AGS-33: Dependency hygiene (P2)

Optional marking is genuinely correct for 21 of 52 root deps, and
`flink-streaming-java`/`flink-clients` are correctly `provided`. But 13
mandatory compile-scope deps are imposed on every consumer:

- **`flink-cep` is the one real scope bug**,`pom.xml:80-84` omits
  `<scope>provided</scope>` while both siblings have it, leaking flink-cep and
  its flink-runtime transitives onto consumer classpaths.
- Postgres JDBC driver + HikariCP forced on everyone, for a framework whose
  pitch is pluggable storage.
- **All five langchain4j artifacts** forced, an Ollama user still drags in the
  OpenAI, Anthropic and Gemini clients.
- **`log4j-core` + `log4j-slf4j-impl` at runtime scope on the library**
  (`pom.xml:187-199`). Libraries must depend on api only. Cost is already
  visible: `a2a-gateway/pom.xml:47-54` excludes all of
  `org.apache.logging.log4j:*` to avoid a duplicate-binding clash. Every
  consumer will need that exclusion.
- **Jackson skew between two coupled modules:** jagentic-core compiles against
  2.17.2; the root pom declares 2.15.2 with no `jackson-bom`. Nearest-definition
  wins, so jagentic-core executes against 2.15.2 inside agentic-flink.

Version decisions: log4j 2.17.1 (Dec 2021), langchain4j 1.16.3 (latest 1.18.0),
Flink 2.2.1 (latest 2.3.0, hardcoded in 3 unrelated poms). `a2a-java-sdk
1.0.0.Alpha3` **is** the current upstream latest, not a blind bump, but means
the A2A wire format rests on a 1.0 line with three alphas and no final.

---

# AGS-2: Java 21 baseline (P1)

## AGS-13: Bump all 13 poms 17 to 21, using `release` not `source`/`target` DONE

All 13 declare 17: root (`pom.xml:15`), `agentic-pekko`, `a2a-gateway`,
`banking-job`, `ports/{jagentic-core,kafka-streams,pekko,pulsar,quarkus,spring,temporal}`,
`tool-services/{packs,app}`. Do it as one property in the AGS-10 parent.

Fix the correctness issue while there: the root pom uses `<source>`/`<target>`
(`pom.xml:451-452`) rather than `<release>`. With source/target javac links
against the *host* JDK's API, so code can reference post-baseline APIs and still
compile, producing a jar that `NoSuchMethodError`s on a real baseline JVM.
`ports/quarkus:80` and `ports/kafka-streams:62` already do this correctly.

Was mechanical, as expected. All 13 poms now declare `21` and every one uses
`release` (the two that already did, `ports/quarkus` and `ports/kafka-streams`,
were left as-is). No literal `17` remains in any pom.

## AGS-14: Enforce the baseline DONE

Nothing enforced the Java version: no `maven-toolchains-plugin`, no
`~/.m2/toolchains.xml`, no `.mvn/jvm.config`, no enforcer `requireJavaVersion`,
**and no Maven wrapper**. The maintainer's own machine had no `mvn` on PATH at
all, only a cached wrapper dist under `~/.m2`, which the 2026-07-26
home-directory loss then destroyed, taking the build tool with the cache.

Delivered:
- **`mvnw` / `mvnw.cmd` / `.mvn/wrapper/`** committed, pinned to Maven 3.9.16.
  The build no longer depends on a preinstalled mvn, which is exactly the
  failure mode that cost a reinstall on 2026-07-26.
- **`maven-enforcer-plugin`** with `requireJavaVersion [21,)` and
  `requireMavenVersion [3.9,)`, each with an actionable message, so a
  contributor on JDK 17 is told the baseline rather than handed an obscure
  compiler error.
- **Lombok 1.18.38 → 1.18.46** (1.18.40 was the first release supporting JDK 25),
  so a future baseline bump isn't blocked by annotation processing.
- **Runtime `--add-opens`**, which surefire had but no launcher did:
  `.mvn/jvm.config` covers every `mvn exec:java` path (`_common.sh`,
  `run-demo.sh`) because `exec:java` runs in the Maven JVM, and a new
  `examples-bin/jvm-opts.sh` holds the single flag list that the two bare
  `java -jar` launchers source. Also made `run-banking-local.sh` executable
  (it was 100644 while all 14 siblings were 100755).

A `maven-toolchains-plugin` was considered and skipped: with the enforcer plus a
pinned wrapper, it would add a `toolchains.xml` prerequisite for no additional
guarantee on this project.

---

# AGS-3: CI (P1)

## AGS-22. CI that builds and tests every language surface DONE (`9731fdc`, PR #13)

Four parallel jobs (jvm/go/clojure/python) on every PR and push to main. Green
on first run: 922 JVM tests, `go vet` + 6/6 packages, 64 Clojure tests, 88+7
Python. Surfaced and fixed two real bugs in the process (undeclared PyYAML in
pyagentic; `ports/tests` hard-failing without Celery instead of skipping).

**Not included, deliberately:** Spotless as a gate. `spotless:check` fails on
code already in `main`, and `apply` is bound to the default lifecycle so every
build rewrites sources. Flipping to `check` needs a repo-wide format commit
first. Container-backed ITs are also out of scope (see AGS-23).

---

# AGS-4: Overclaimed capabilities: implement or retract (P2)

## AGS-24: a2a-gateway advertises gRPC and REST that do not exist (P2)

Claimed in `CLAUDE.md:49`, `a2a-gateway/README.md:6`, `docs/a2a.md:8,17`,
`README.md:59`. Reality: `grep -rin grpc a2a-gateway/` returns **zero hits** in
the pom and zero in any `.java`. `A2AResource.java` exposes exactly three
endpoints: Agent Card, JSON-RPC POST, SSE POST. There is no HTTP+JSON/REST
binding either. "REST" is the JSON-RPC POST relabelled.

Actively harmful: `GatewayConfig.java:47` `grpcUrl()` and `:52` `restUrl()`
exist only to **advertise endpoints on the Agent Card that the gateway cannot
serve**. A client that trusts the card and dials gRPC gets nothing.

Status: the documentation half is retracted (`CLAUDE.md`, `a2a-gateway/README.md`,
`docs/a2a.md`, `README.md` now state JSON-RPC and SSE only). The code half, dropping
`grpcUrl()`/`restUrl()` from the Agent Card or implementing the servers, is still open.

(Note `tool-services/` *does* ship real gRPC, quarkus-grpc, a `.proto`,
`ToolGrpcService` and 3 passing tests. The capability exists, just not here.)

Also in this module: `README.md:17-22` documents four classes that exist
nowhere (`AgentCardProducer`, `AgenticFlinkAgentExecutor`, SDK `AgentEmitter`);
`BridgeProducer.java:16` has a dangling javadoc link to the deleted class;
`a2a.version` (`pom.xml:24`) is declared and unused; and `pom.xml:74-80` claims
the image hosts an embedded MiniCluster job launched as a second process,
nothing launches any process, yet that false comment justifies ~200 MB of
compile-scope Flink deps.

## AGS-25: `ports/README` capability matrix credits capabilities absent from the code (P2)

- **Pekko row (`:58`)**: "Cluster Sharding (C1+C2) **+ Persistence (C3)**, all
  native." In `ports/pekko`, `grep EventSourcedBehavior|PersistenceId` matches
  **only javadoc**: `ConversationActor.java:66-67` literally says "replace this
  with an EventSourcedBehavior". Actual behaviour is `Behaviors.setup` with
  `new ConversationStore.InMemory()`. `pekko-persistence-typed` and
  `pekko-serialization-jackson` are declared and unused. (The top-level
  `agentic-pekko/` module *does* implement it, the error is crediting this row
  with that module's capability; `ports/pekko/README.md:1` even calls itself
  superseded.)
- **Pulsar row**: "native durable state (C1+C3), runs + tested ". Both tests
  run against `InMemoryContext.java`, a reflective `Proxy` fake that ships in
  **`src/main`** (~21% of module main LOC) and silently no-ops `publish`,
  `recordMetric`, `getSecret`. No test touches a real broker.
- **Kafka Streams row**: claims C1 via changelog. `BankingTopology.java:121`
  keeps the transcript in `new ConversationStore.InMemory()`, a partition-local
  heap map that does not survive rebalance. The RocksDB store that *is*
  registered feeds only `KeyValueBackedState`, which the graph never touches.
  Also `main()` only prints `describe()`; there is no `new KafkaStreams`
  anywhere, so it cannot run against a broker.
- **Ray row (`:61,81,232`)**: durability is the comment
  `# write-through point: persist self.store snapshot to Redis/Fluss here. `
  (`ray/agentic_ray.py:62`).
- **"Run the same spec on any backend in any language" (`:28-31`)**: only
  `agentic-pekko` implements `BackendProvider`; none of the six `ports/` JVM
  adapters do. Python offers local/celery/nats, Go local/nats, Java local. **9 of
  12 engines cannot be targeted by a `pipeline.yaml` at all. **
- **Stale counts**: `:71` says "pyagentic 52 pass" (actual 93);
  "agentic-pipeline 11 pass" is a *collected* count (actual 8 pass / 1 hard fail
  / 2 skip); "gateway 9 pass" (0 runnable without FastAPI). `:184` tells the
  reader to run `/tmp/af-venv/bin/python`, a path that does not exist.

Regenerate every number from CI rather than by hand.

## AGS-31: Notebooks ship fabricated outputs and cells that cannot run (P2)

`02_live_scrape_rag.ipynb` and `03_scraper_researcher_fluss.ipynb` ship stored
outputs reading `localhost:8077/flink.html -> 1 chunks`, while their *source*
scrapes flink.apache.org and Wikipedia. Repo-wide grep for `8077` outside
notebooks: **zero hits**; `flink.html`/`agents.html` do not exist. A notebook
titled "Live Scrape → RAG" is shipping results from a private stub server that
is not in the repo. Either re-execute against the public URLs in its own source,
or delete the outputs.

Three fatal cells:
- `01_quickstart.ipynb` cell 17 calls `chat(conn, setup, [ChatMessage.user(...)])`
, signature is `chat(connection, messages, setup)` (`llm.py:105`), args
  reversed, and `ChatMessage` (`llm.py:50`) is a frozen dataclass with **no
  `.user()`** → `AttributeError`. The only cell in the quickstart that talks to
  an LLM has never run. Cell 5 also ignores `WebFetchTool`'s `ok`/`error`
  fields, prints `text length: 4 chars` (that's `len("None")`), then dies on
  `len(None)`, while cell 18 asserts "A real web page was fetched and parsed".
- `07_market_depth_agents.ipynb` cell 7 uses `Counter()` without importing it.
- `09_session_cluster_levels.ipynb` cells 18/22 call `operator_ids(...)` and
  `DebugFlipper(...)`, which live only in `session.py`, are not in
  `__init__.__all__`, and are never imported → `NameError`. 9 of 13 code cells
  have never run.

Prerequisites are also wrong: `docs/compose.md:99-112` says notebooks
01/04/05/06/07/08 need "nothing", but all six need a built jar **and Maven on
PATH** (`runtime.py:560-577` shells out to `mvn dependency:build-classpath` with
`check=True`). `docker-compose-notebook.yml` installs six packages but not
`kafka-python`, which notebook 02 needs. Stored paths still say
`/Users/bengamble/Agentic-Flink/...`.

## AGS-35: agentic-pekko and agentic-clj claim more than they wire (P2)

`agentic-pekko` is the **strongest module in the repo**, event sourcing is
genuinely real (`ConversationEntity` extends `EventSourcedBehavior`,
`PersistenceId.ofUniqueId`, `Effect().persist`, event handler rebuilds state via
`TurnMutations`), the `Recording*Store` overlay correctly makes recovery avoid
re-invoking the LLM, and its 14 tests are substantive (one proves the transcript
survives entity restart while brain invocation count stays flat). Three claims
outrun it:

1. **Kafka `turnId` dedupe does not work.** `README.md:27` claims "at-least-once
   + turnId dedupe"; `kafka/KafkaStreamApp.java:32` claims "effectively-once".
   But `KafkaStreamApp.java:61` and `kafka/AgentStream.java:31` both pass
   `UUID.randomUUID().toString()` as the turnId, so every redelivery gets a
   fresh id and is reprocessed, the dedupe path
   (`ConversationEntity.java:118-122`) is dead on the only ingress that needs
   it. Same at `runtime/PekkoRuntime.java:45`. Derive turnId from the Kafka
   key/partition/offset, or retract the claim.
2. **Cluster sharding is test-only.** `pom.xml:12` and `README.md:3` sell
   "cluster-sharded entity per conversation". `cluster/ConversationSharding.java`
   is real and correct, but its only caller is its test. Every runnable
   entrypoint goes through `PekkoSystem` → `ConversationManager`, a `HashMap` of
   children in a single actor (`runtime/ConversationManager.java:39-42`).
   Single-node only.
3. **Durability profiles unreachable.** `DurabilityProfile`
   (POSTGRES/CASSANDRA/REDIS) is selectable only by calling
   `new PekkoSystem(deps, profile, redisUrl)` directly; no main reads it from
   config or env, nothing wires `-Dconfig.resource`, and pekko-persistence-jdbc
   needs DDL the repo doesn't ship.

`agentic-clj` is also genuinely real (Datomic local, working `as-of` time
travel, 64 tests / 249 assertions), but:
- **No single-writer.** `core.clj:19` says it's "the store's concern"; nothing
  serializes per conversation. CLAUDE.md lists single-writer-per-conversation as
  a defining property.
- **Read-modify-write race.** `store/datomic.clj:70-75` reads `msg-count` then
  transacts that value as `:message/position`; two concurrent appends both write
  position N, so ordering becomes nondeterministic. Same in
  `datomic-long-term-store/save-turn` (`:126-130`). Use a tx-fn or a
  `:db/unique` composite.
- `README.md:7` claims `d/history`, which appears nowhere in `src/` or `test/`.
- `backend:` in a spec is silently ignored, `pipeline.clj:217-225` always
  builds `core/local-system`.

## AGS-38: Fabricated model IDs shipped as defaults (P2)

Non-existent model ids as defaults, so the out-of-box experience 404s:
`ports/pyagentic/pyagentic/llm.py:239` (`gpt-5.4-mini`),
`ports/agentic-pipeline/agentic_pipeline/loader.py:36` (same),
`ports/go/pipeline/loader.go:152` (same),
`python/agentic_flink/llm.py:91` (`gemini-3.5-flash` in a docstring),
`examples-bin/run-banking-local.sh:26` (`gpt-5.4-nano`),
`docker-compose-a2a-banking.yml:41` (`gemini-3.5-flash`).

One audit track argued `gpt-5.4-mini` is a deliberate repo-wide convention
because a Java test discusses real 400 responses from it, worth confirming
intent. Either way, a non-existent default breaks first use.

---

# AGS-5: Correctness bugs (P1)

All verified by reading the source.

## AGS-15: ValidationFunction treats every rejection as approval (P1)

`function/ValidationFunction.java:130`:

```java
boolean isValid = validationResponse.toUpperCase().contains("VALID");
```

`"INVALID".contains("VALID")` is **true**. An LLM answering "INVALID" scores
valid=true, score=1.0, and the failure reason is discarded (the ternary at `:138`
passes null when isValid). **The validation gate can never reject anything.**
The class has zero tests. Fix by matching a normalized token or, better, using
the structured `OutputSchema` path instead of substring sniffing.

## AGS-16: Postgres stores emit H2-only MERGE syntax (P1)

`storage/postgres/PostgresConversationStore.java:209,318,383` and
`a2a/storage/PostgresA2ATaskStore.java:93,166` build upserts as:

```sql
MERGE INTO agent_contexts (...) KEY (flow_id) VALUES (?, ?, ?, ?, ?, ?)
```

`MERGE INTO ... KEY (col) VALUES (...)` is **H2-proprietary**. PostgreSQL 15+
MERGE requires `USING <source> ON <cond> WHEN MATCHED THEN ...`; there is no
`KEY` clause and no bare VALUES form. The comment at `:207` ("works in both H2
and PostgreSQL 15+") is wrong. CLAUDE.md calls this the "production default for
long-term". **it cannot write to a real Postgres.**

Why it shipped green: every test uses H2,
`PostgresConversationStoreTest.java:45` (`jdbc:h2:mem:...;MODE=PostgreSQL`),
`StorageFactoryTest.java:110,128`, and `A2ATaskStoreTest.java:32` runs its
"postgres" `@ValueSource` case against H2. H2's PostgreSQL *mode* does not
restrict MERGE to Postgres grammar.

Fix: `INSERT ... ON CONFLICT (col) DO UPDATE SET ...`, and replace the H2 tests
with Testcontainers `postgres:16` (ties to AGS-23).

## AGS-18: Three transient-field NPEs that only fire once distributed (P1)

Java deserialization does not run field initializers, so `transient` fields with
inline initializers are null on the task side. Local tests pass because nothing
serializes; failure appears only on a real cluster.

1. `listener/MetricsAgentEventListener.java:17-27`, eleven
   `private final transient LongAdder x = new LongAdder()` plus 2 `AtomicLong`.
   All null after deserialization, and `final` blocks repair without
   `readObject`/reflection. Reachable: `Agent` holds listeners and
   `ReActProcessFunction` fans out to them inside the operator → NPE on first
   `onChatRequest`.
2. `web/RobotsCache.java:32`. `private final transient ConcurrentMap cache =
   new ConcurrentHashMap<>()`. After deserialization `cache` is null →
   `isAllowed()` NPEs → the catch at `:53` returns **true** (fail-open). Net
   effect: **robots.txt is silently never enforced** in any serialized
   `Fetcher` (`Fetcher:22` holds it non-transient). A crawler ignoring
   robots.txt is a real liability. There is also no crawl-delay/politeness
   anywhere in `web/`.
3. `execution/LLMClient.java:61`. `private transient AgentEventListener
   listener = new AgentEventListener() {}`. `LLMClient` is a non-transient field
   of `stream/AgentExecutionFunction:46` and `AgentFlatMapFunction:45`, so it
   ships in the job graph → NPE at the first guardrail call.

Fix: initialize in `open()`/`readObject`, drop `final`, or make fields
non-transient with serializable types. Add serialize→deserialize→invoke tests;
current serialization tests only assert `bytes.length > 0`, so they pass with
the bugs live.

**Additional serializability violations** found in the same sweep:
`langchain/LangChainToolAdapter:43` (non-transient non-Serializable
`ToolAnnotationRegistry`); `tools/rag/KnowledgeQueryTool:20` and
`ScrapeUrlTool:18` (non-Serializable `KnowledgeBase`);
`storage/vector/InMemoryVectorStore:35` (non-transient map whose `Entry:261`
isn't Serializable, `new`'d into 7 shipped paths);
`context/relevancy/RelevancyScorer:22` (`Scorer` isn't Serializable);
`example/banking/BankingAgentSetup:267-268` (a lambda capturing a live
`ResilientA2AClient` stored non-transient in `BankingPathFunction:32`, the real
outbound A2A path in the flagship demo has never been submitted to Flink).

## AGS-19: StorageFactory iterates the VectorStore SPI unguarded (P1)

`storage/StorageFactory.java:150` and `:190`:

```java
for (VectorStore candidate : ServiceLoader.load(VectorStore.class))
```

`META-INF/services/org.agentic.flink.storage.VectorStore` registers 5 providers;
3 need optional deps (Qdrant, Milvus, Fluss). Bare iteration instantiates each
eagerly, so the first `createVectorStore()` throws
`ServiceConfigurationError`/`NoClassDefFoundError` for any consumer on a default
build, i.e. everyone.

The fix already exists in the same file: `:94-105` uses
`ServiceLoader.Provider.get()` inside `try/catch(Throwable)` with the comment
"so a missing optional dep (e.g. Jedis) doesn't poison the iteration". ~6 lines.
(`ConversationStores.java:29-40` and `DiscoveringA2AClientFactory.java:48-58`
are also correct; only the two VectorStore loops were missed.)

## AGS-20: Guardrail redaction never reaches the transcript (P1, security)

In jagentic-core, `Agent.java:20` persists the **raw** assistant reply:

```java
ctx.store.append(cid, ChatMessage.assistant(reply));
```

Only afterwards does `RoutedGraph.java:92-107` apply the verifier
(`result.reply = v.reply()`) and output guardrails
(`result.reply = "[blocked] " + reason`). Those mutate the returned `TurnResult`
only, `TurnResult.reply` is a public mutable field. **The store keeps the
unredacted text**, and `LlmBrain` replays `store.history()` into the next
prompt, so content blocked on turn N is fed back to the model on turn N+1. The
redaction is cosmetic.

Shipped green because `PolicyTest.outputGuardrailRedacts` asserts only
`res.reply` and never reads the store.

Fix: apply verifier + guardrails **before** persisting, or persist the
post-guardrail reply. Assert store contents, not the return value.

## AGS-37: docker-compose-session.yml pins Flink 1.20.1 vs project 2.2.1 (P2)

`docker-compose-session.yml:14,40` use `flink:1.20.1-scala_2.12-java17` while the
poms declare `2.2.1`. Because Flink is `provided`, the uber jar carries no Flink
and relies on the cluster's, a job compiled against 2.2.1 will not run on a
1.20.1 TaskManager. Blast radius: `include`d by `-cluster.yml`, `-markets.yml`
and `-all.yml`, used by `examples-bin/run-session-cluster.sh` and notebook 09.

All 11 root compose files were checked; the rest resolve. Minor:
`run-bond-market.sh:20` and `run-crypto-market.sh:18` point at
`docker-compose-kafka.yml` when `-markets.yml` exists for that purpose;
`run-banking-local.sh` is mode 100644 while all 14 siblings are 100755.

---

# AGS-6: Paradigm completion: Flink fault tolerance (P1)

## AGS-21: Wire checkpointing and state backends on the Flink runtime (P1)

Given the bar (native state + checkpoints suffices), two runtimes clear it:
`agentic-pekko` (real Pekko Persistence, recovery tested without re-invoking the
LLM) and `agentic-clj` (real Datomic + `as-of`). `ports/temporal` also clears it
via the engine. **The flagship Flink runtime does not**: `enableCheckpointing`,
`CheckpointingMode`, `setStateBackend` and `CheckpointedFunction` appear **zero
times** in the framework, only in the build-excluded `plugins/flintagents`
demo, and there with the non-durable `HashMapStateBackend`.

Supporting gaps:
- `PollingSource` splits are explicitly stateless, `serialize()` returns
  `new byte[0]` (`channel/source/PollingSource.java:119`), so every non-Kafka
  channel is at-most-once across a restart.
- No sink implements `SupportsCommitter`/`TwoPhaseCommittingSink`.
- `a2a/bridge/A2AJsonTypeInfo:181` restores a snapshot with
  `new A2AJsonSerializer<>(null)` → savepoint restore of a bridge stream NPEs.
- `FlinkStateHnswVectorMemory:140` calls `getMapState()` while
  `IngestionPipeline.IndexFn:164` and `RetrievalPipeline.SearchFn:224` are
  non-keyed `ProcessFunction`s → throws in `open()`. **Both Flink-state vector
  memories are unreachable through the shipped corpus/retrieval API. **
- `FlinkGraphFunction:25-26` is honest that its memory is "not
  Flink-checkpointed, a documented limitation".

Deliverable: checkpointing configured (or explicitly documented as a caller
responsibility), a durable state backend option, sources that snapshot offsets,
and a test that restarts from a checkpoint and asserts the conversation view is
intact.

---

# AGS-7: API surface and dead code (P2)

## AGS-32: Decide what AgentBuilder means (P2)

16 of 48 `AgentBuilder.withX()` methods have zero call sites repo-wide, and 21
of `Agent`'s ~50 getters have zero read sites outside `dsl/`, including every
memory, storage, vector, guardrail, MCP, embedding, context and task-list
option.

Sharpest case: CLAUDE.md's own headline example uses `withShortTermTtl`,
`withLongTermStore`, `withMemoryChannel` and `withVectorMemory`, four methods
nothing in the repo reads or tests. `Agent.getShortTermMemorySpec()` has zero
readers, so `withShortTermTtl` is inert. Meanwhile `ReActProcessFunction` keeps
its own parallel `ListState` transcript, ignoring both `ShortTermMemory` and
`ConversationStore`.

Status: the four methods are no longer shown as working in `README.md` or `CLAUDE.md`
(re-verified: `getShortTermMemorySpec`, `getVectorMemorySpec`, `getLongTermStore`,
`getMemoryChannel` have no readers outside `dsl/`). Wiring, deleting, or throwing from
`withShortTermTtl`, `withVectorMemory`, `withLongTermStore`, `withMemoryChannel` is still
open for the code owner.

Silent no-ops in a public API are the worst of three options: wire, delete, or
throw. `dsl/` also has **no test directory at all**, covering `AgentBuilder`
(874 LOC, largest file in the repo), `Agent` (459) and `SupervisorChain*` (637).
Fix the false javadoc at `AgentBuilder:729-730` claiming guardrails "run before
and after every LLM call".

Also pick **one** of the seven overlapping agent-execution paths and delete the
rest. Two public classes are both named `AgentExecutionFunction` (`job/` and
`stream/`), disqualifying for a 1.0 on its own. The best-designed path
(`ReActProcessFunction`) has zero production call sites; the de-facto real one
(`execution/AgentExecutor`) is a plain loop on `ForkJoinPool.commonPool()` with
no Flink state, and its tool protocol is a `TOOL_CALL: name {json}` regex with a
hand-rolled parser (`LLMClient:274-311`, `replaceAll("[{}]","")` + `split(",")`)
that breaks on nested objects, arrays or commas in strings, while Jackson is
already on the classpath.

## AGS-36: Delete dead code and dead config (P3)

Unreachable in `src/main` (~2,600 LOC; verify each before deleting):
`pattern/AgentCEPPattern` (its `createInactivityDetectionPattern` is also
semantically **inverted**, it matches activity); `cep/CepPatternBuilder` (485)
+ `cep/PatternConditions` (491); `context/memory/*` (a duplicate lineage of
`memory/`); `tools/ToolExecutorRegistry`; `stream/AgentExecutionStream`;
`function/ToolCallAsyncFunctionV2`; `core/AgentConfig`; `core/ToolDefinition`;
`stream/CompensationFunction`; `serde/ControlCommand*`,
`serde/ToolAllowlistUpdate/Action`; `storage/SteeringStateStore` (260-LOC
interface, zero impls, zero callers); `tools/rag/KnowledgeQueryTool`,
`ScrapeUrlTool`.

**Compensation is emitted and dropped**: `AgentJobGenerator` emits to
`COMPENSATION_TAG:106` and nothing consumes it, so saga rollback never runs.

Dead files at the repo root:
- **`sql/schema.sql`**. 141 lines defining `conversations`, `context_items`,
  `messages`, `tool_executions`, `validation_results`. Grep for those table
  names across all Java: zero hits. The real Postgres store uses
  `agent_contexts`/`agent_facts` and creates them itself. Worse,
  `docker-compose.yml:14` mounts this dead schema as an initdb script, so every
  demo Postgres is initialised with a schema nothing reads.
- **`config/storage-{memory,redis}-example.yaml`**, describe a tiered
  `storage: {hot:, warm:}` format; grep for the filenames or `storage.hot`
  across all Java/md/sh/yml/py: zero hits. There is no YAML config loader
  (`StorageConfiguration`'s YAML methods log "not implemented" and return
  empty). Artifacts of a removed HOT/WARM design.
- **`run-demo.sh`**, works, but a stale relic with pre-rename branding, hard
  codes one example, duplicates `examples-bin/_common.sh` with none of its rigor
  (no `set -euo pipefail`, no Ollama check despite declaring it a
  prerequisite), and is referenced by neither CLAUDE.md nor README.

**`examples-bin/` is NOT build output**, all 21 files are intentionally tracked
shell/Python wrappers; it is just misnamed (rename to `scripts/` or `bin/`).
But `examples-bin/run-banking.sh:28-29` defaults `KB_DIR` to
`$ROOT/hackathons/...`, which `.gitignore` excludes as an external repo, while
`banking-kb/` holds exactly that data, repoint it. Its `-Da2a.mode=banking` /
`A2A_MODE` are read by nothing.

**Five modules are undocumented in CLAUDE.md**: `tool-services/`,
`banking-job/`, `banking-kb/`, `examples/`, `examples-bin/`.

---

# AGS-8: Test integrity (P2)

## AGS-23: Tests that fail open (P1)

Three mechanisms let the suite report green on untested code.

1. **Fixture-missing skips hide the flagship parity proof.** jagentic-core's
   `PipelineTest.java:20,45`, `PipelineRagTest.java:28` and
   `cep/CepSpecTest.java:117` resolve fixtures via the **CWD-relative** path
   `../../examples/pipelines/banking.yaml` guarded by `assumeTrue`. Under the
   documented invocation (`mvn -f ports/jagentic-core/pom.xml ...`) CWD is the
   repo root, the path misses, and they silently skip, measured: PipelineTest
   3/3 skipped, PipelineRagTest 5/6 skipped. **Re-run with CWD =
   `ports/jagentic-core` and all 21 parity/replay/CEP tests pass**, so the
   cross-runtime parity claim is real but unverified by default. Resolve from
   `${project.basedir}` or a classpath resource and make a missing fixture a
   hard failure. Same pattern in the Pekko (`Assumptions.abort`) and Clojure
   (`println "skipping"`) parity tests, and `FlinkPipelineRunnerTest.java:42,53`.
2. **H2 stands in for Postgres**, which is why AGS-16 shipped. Use
   Testcontainers.
3. **`*IT.java` files are unrunnable in every profile**, there is no failsafe
   plugin and no surefire `<includes>`, so surefire's filename filter excludes
   them before the `@Tag` filter applies. Net: `RedisA2ABridge`,
   `RedisA2ATaskStore`, `RedisHotVectorIndex`, `FlussChannel`/`FlussSink` have
   **zero executed coverage**, and no IT exists at all for Postgres, pgvector,
   Qdrant or Milvus (~2,300 LOC of untested vector-store drivers).

Also worth a sweep, tests that assert nothing meaningful:
`execution/ToolCallParsingTest` re-declares the regexes locally and never
imports `LLMClient` (it tests `java.util.regex`; the only parser in the real
execution path has zero coverage); `function/ReActProcessFunctionTest:92-140`
constructs the function at `:107`, never invokes it, and ends in
`assertNotNull`, while its `@DisplayName` claims "ReAct loop terminates" (its
*second* test at `:203-231` is a genuine MiniCluster test, keep that);
`config/AgenticFlinkConfigTest` has 14 tests that never set an env var or system
property, though the class exists to resolve them; `job/AgentExecutionFunctionTest`
never calls `processMatch`; `a2a/A2ABridgeTest:41-46`'s `bridge(String transport)`
ignores its parameter and always returns `InProcA2ABridge`;
`inference/InferenceSetupSerializationTest` asserts `count >= 0`;
`SdkA2AClientTest` (45 LOC) has no `send`, no `getTask`, no HTTP. ~200 lines of
SDK↔model mapping are 100% unexercised against an `Alpha3` pin.

**Reference-standard tests worth preserving:**
`job/ResearchPipelineJobTest:587-682` (real ingest→search→RAG round-trips with a
deterministic embedder, asserting exact ranking), `typeinfo/KryoDisabledPipelineTest`,
`typeinfo/TypeExtractorKryoFreeTest`, `memory/conversation/ConversationStoreTest`,
`a2a/ResilientA2AClientTest`, `a2a/A2AAsyncFunctionTest:132-166` (contextId
continuity across two job executions), `operator/AgenticPipelineTest`,
`completion/GoalPredicateTest` (812 LOC, randomized).

---

# AGS-17: Ports productization (P2)

Current state: **3 real cores**,`jagentic-core` (7.5k LOC, 94 tests),
`ports/go` (8.9k, 94 tests, vet clean), `ports/pyagentic` (6k, 93 tests, zero
runtime deps, imports clean 3.9-3.14), plus 14 thin adapters, most written in
1-3 commits on 2026-06-14/15 and never revisited.

## AGS-26: `ports/{celery,dask,ray,faust,airflow,nats}` shadow the PyPI packages they port to (P1)

Verified empirically, from `ports/`, these become implicit namespace packages
that shadow the real libraries:

```
$ cd ports && python3 -c "import celery, dask, ray, faust, airflow"
celery  -> file=None path=['.../ports/celery']
dask    -> file=None path=['.../ports/dask']     (same for ray/faust/airflow)
```

So `try: import celery except ImportError` reports "engine not installed" even
when it is. This is the root cause of every hand-rolled guard in the tree
(`if not hasattr(faust, "App"): faust = None` at `faust/agentic_faust.py:34`,
`ray/agentic_ray.py:31`, `celery/agentic_celery.py:51`, `nats/agentic_nats.py:51`).
`airflow/agentic_banking_dag.py:87-92` has no such guard, so from `ports/` it
silently registers zero DAGs. And `ports/README.md:156` tells the reader to do
exactly this: `cd ports && PYTHONPATH=pyagentic python -m pytest tests/`.

Fix: rename the directories. This single change removes the reason every engine
guard exists.

## AGS-27: Proven runtime bugs in the thin ports (P1)

**airflow. BROKEN.** `airflow/agentic_banking_dag.py:144` calls `_EMBED(text)`;
`_EMBED` is never defined in the file (line 144 is its only occurrence, copied
from `dask/agentic_dask.py`, which does define it). `NameError` on every
ingestion run. `:89` imports `airflow.utils.dates.days_ago`, removed in Airflow
3.x, so on a modern Airflow the import fails and both DAGs silently never
register. And `run_path(path, text, cid)` at `:71-77` **ignores its `path`
argument** and re-runs the whole graph including re-routing, so whichever branch
fires gives the same answer, the branching is decorative. It also builds a
fresh `InMemoryConversationStore` per task, so no state survives.

**celery, single-writer provably fails.** `celery/agentic_celery.py:69` routes
by `hash(conversation_id) % N`. Python's `str` hash is PYTHONHASHSEED-randomized
per interpreter, so the same conversation routes to different queues from
different producer processes → concurrent writers, the exact invariant the port
exists to demonstrate. The test that "covers" it
(`ports/tests/test_adapters.py:93`) asserts
`conversation_queue("c1") == conversation_queue("c1")` **within one process**: a
tautology. Fix with `zlib.crc32`/`hashlib`.

**spring, every transcript is doubled.** `AgentController.java:56-57` calls
`associateUser` + `append(user)`, then `graph.handle(...)`, then
`append(assistant)` at `:64`. But jagentic-core `Agent.java:17-20` already does
all three. So every turn writes 4 messages as user,user,assistant,assistant. Any
brain reading `store.history()` gets a duplicated, mis-ordered transcript. The
module has **zero tests** and no `spring-boot-starter-test` dependency, which is
how it earns a "compile ".

**go, the same spec routes differently than Java/Python.**
`pipeline/builder.go:386` does `for path, kws := range rules`; Go randomizes map
iteration, so a spec with overlapping keywords routes nondeterministically call
to call (same at `:380` for the default path, `:34-39` for `toolTriggers`). Java
iterates a Jackson `LinkedHashMap` (deterministic); Python uses insertion-ordered
dicts. This falsifies "run the same spec on any backend in any language" and
breaks deterministic replay on the Go path.

## AGS-28: `Event` field order diverges across the three cores (P2)

- Python `pyagentic/core.py:21-27`. `Event(conversation_id, text,
  user_id="anonymous", metadata)`
- Java `jagentic-core/.../Event.java:6`. `record Event(conversationId, userId,
  text, metadata)`
- Go `go/core/model.go:11-16`, `{ConversationID; UserID; Text; Metadata}`

Python's 2nd positional arg is **text**; Java's and Go's is **userId**. Anyone
porting a call site between languages, which the parity docs encourage,
silently swaps the user id and the message body, with no type error in any of
the three. Existing parity tests can't catch it: they pin FNV constants, one
embedding vector, one ranking and five keyword routes, each constructed
in-language.

Pick one canonical order (Java/Go already agree, so Python is the odd one out)
or require keyword construction. Add a parity test that builds an `Event` from
an ordered tuple in each language and asserts the same routing outcome.

### Other port notes captured during the audit

- **`ports/tests` is not a conformance suite**. 8 test functions covering 6
  Python adapters only. Nothing for kafka-streams, pekko, pulsar, quarkus,
  spring, temporal or Go. There is no cross-port conformance suite anywhere.
- What *does* exist is a **golden-constant contract**: `pyagentic/tests/test_parity.py`,
  `jagentic-core/.../ParityTest.java` and `go/core/parity_test.go` each hardcode
  the same literals (`1712156752, 2560266987, 2284280159, 3025431163` appear
  identically in all three), 4 tests each. Real but thin, maintained as three
  hand-copied duplicates with no shared fixture.
- **`gateway-fastapi` blocks the event loop**: `app.py:95` is `async def` and
  `:96` calls the *synchronous* `backend.submit(...)`, which takes a
  `threading.Lock` and may do blocking LLM HTTP. On the single event-loop thread
  this stalls all concurrent requests. `TestClient` serialises requests, so no
  test can catch it.
- **`ports/quarkus` is not packageable**, only `maven-compiler-plugin` is
  declared; no `quarkus-maven-plugin`, so `mvn package` yields a plain jar with
  no runner. `application.properties:9` sets `ObjectMapperDeserializer`, which
  has no public no-arg constructor → `KafkaException` at startup.
- **`ports/pulsar`** ships `InMemoryContext.java` (a reflective fake) in
  `src/main`; move it to `src/test`. `pulsar-functions-api` should be `provided`.
- **`ports/temporal`** is the best of the six adapters, real `@UpdateMethod` +
  `@QueryMethod` CQRS on a real `TestWorkflowEnvironment`, but nothing forces a
  replay (no `WorkflowReplayer`, no `setWorkflowCacheSize(0)`), and
  `temporal-testing` is at compile scope because `src/main`'s `LocalDemo` uses
  it. Latent determinism hazard: `Banking.seedKb` iterates `Map.of`, whose order
  depends on per-JVM `ImmutableCollections.SALT`.
- **`ports/kafka-streams`** `KeyValueBackedState.clear()` calls `kv.delete(key)`
  on the bare key while `slot()` builds `key + ' ' + name`, deletes nothing,
  and is unreachable anyway.
- **`jagentic-core` bugs**: `EventLog.InMemory.eventsFor` is not synchronized
  while its siblings are (CME risk in the class documented as "the source of
  truth"); `DurableTimerService` drops `Event.metadata` on restore (`encode`
  never writes it, `decode` rebuilds with `Map.of()`) and writes `t.id()`
  un-base64'd while every other field is encoded; `McpStdioClient` is
  `AutoCloseable`, constructed per server in `GraphBuilder.registerMcp:306-311`,
  never closed; `LocalRuntime.locks` grows unbounded; `LlmBrain.finalize(String,
  AgentContext):134` shadows `Object.finalize` by name.
- **The default embedder is a hash trick.** `Retrieval.embed` is FNV-1a
  bag-of-words with zero semantics, and it is the default everywhere, so every
  retrieval-quality assertion is literal token overlap. The default brain is six
  `contains()` calls, the system ~85 tests exercise has no model.
- **`go` notes**: `core/a2a.go:45-58` has `defer resp.Body.Close()` inside a
  retry loop and sets `last = err` in the `err == nil` branch;
  `engines/natsjs/natsjs.go:137-142` uses `kv.Put` not `kv.Create` on `rev==0`
  (first-turn lost update); `pipeline/loader.go:100-108` silently downgrades a
  Postgres connect failure to in-memory with no log; `gateway/gateway.go:137`
  has an unbounded body decoder, no auth, no rate limit.

---

# AGS-34: Table stakes for a public library set (P2)

Verified absent: `CONTRIBUTING.md`, `CHANGELOG.md`, `SECURITY.md`,
`CODE_OF_CONDUCT.md`, `NOTICE`, any versioning/semver policy, any deprecation
policy, a compatibility matrix (framework ↔ Flink ↔ langchain4j), javadoc
generation or publishing, a Maven wrapper, Dependabot config, and install
instructions with actual coordinates (README has zero hits for `pip install`,
`<dependency>`, or a "maven central" install section). Git tags: **0**.

Present and correct: `LICENSE` (Apache-2.0), `python/LICENSE`, a comprehensive
`.gitignore`, `python/PUBLISHING.md`, a large `docs/` tree.

Also needed before 1.0: an **API-stability surface**. 396 of 403 public types in
`src/main` are `public` with no `@Public`/`@Experimental`/`@Internal` markers
(no such annotation is even defined), no `module-info`, and **zero `@Deprecated`
in `src/main`**. CLAUDE.md advertises "legacy InMemoryShortTermStore
(deprecated path)" and the class exists but carries no annotation, so the
deprecation is invisible to compilers and IDEs.

Getting-started friction: `docs/getting-started.md` requires installing Ollama,
pulling models and running a Qdrant container before `mvn clean package`. There
is no container-free path for a first-time user.

## AGS-44: Document AI-grind as the local dev toolchain (P2)

Local development is managed with [AI-grind](https://github.com/Ugbot/ai-grind).
CONTRIBUTING should name it as the supported way to build, test, track and
profile, rather than leaving contributors to reconstruct the toolchain. Worth
documenting because this audit had to rediscover all of it: no `mvn` on PATH
(only a cached wrapper dist), no JDK 17 installed (21 is what works), and
`timeout`, `go`, `clojure` and a modern Python all had to be installed before
the Go, Clojure and Python surfaces could be verified. A documented bootstrap
(JDK 21, Maven, Go, Clojure CLI, Python ≥3.10, coreutils, Podman) would make a
contributor productive on day one. Also document the mandatory
`mvn -f ports/jagentic-core/pom.xml install` prerequisite, currently
discoverable only from an XML comment.

---

# AGS-39: Docs truth (P1)

Two independent doc audits found systematic doc↔code drift. The pattern is not
typos, whole chapters document APIs that were refactored away, and the primary
run commands fail.

**Clean, do not rewrite:** `docs/examples/incident.md` (zero findings) and
`docs/guides/creating-tools.md` (~95% accurate, every class/method/annotation
verified real). No stale `com.ververica` or `MemoryFeed` references survive.

## AGS-40: Most documented `mvn exec:java` commands cannot run (P1)

`exec-maven-plugin` is **not declared in the root pom** (only `agentic-pekko`
declares it). So every `mvn exec:java` line in README.md, CLAUDE.md,
`docs/guides/{storage-quickstart,openai-setup,flink-agents-integration}.md`,
`docs/examples/{markets,banking-everywhere}.md` and
`docs/portability/{pipelines,stream-stateful-core}.md` resolves an unpinned
plugin from central.

Worse, it then fails: Flink is `provided` scope (`pom.xml:67-77`) and
`flink-cep` is provided too, while `exec:java` defaults to
`classpathScope=runtime` → `NoClassDefFoundError:
org/apache/flink/streaming/api/environment/StreamExecutionEnvironment` for every
Flink-job example. The repo already knows: `TEST_REPORT.md:95` records using
`-Dexec.classpathScope=test`, `:99` marks `StorageIntegratedFlinkJob` as "needs
flink run", and `SuspiciousActivityCascadeExample.java:39` documents the
workaround in its javadoc. Affects `examples-bin/run-{live-research,incident,
moderation,rag}.sh` via `_common.sh:41`.

Reproduced again from a fresh clone (Java 21, `./mvnw`): with the classpath scope fixed,
`QuickStartExample` still fails in `AgentBuilder.build()` with `Initial state has no
outgoing transitions` before any model call, and `FlinkPipelineRunner` fails on the compile
classpath (`flink-connector-datagen` is test scoped) and on the test classpath at job
submission with `Could not deserialize stream node 4:
SimpleUdfStreamOperatorFactory`. Both are now listed as known broken in
`docs/getting-started.md` instead of being advertised; the working Flink lane is
`FlinkPipelineRunnerTest`.

Other broken commands:
- `docs/examples/banking-everywhere.md:50` runs `QuickStartExample` as "the
  banking agent as Java". That class is a generic Ollama chat bot,
  `ToolRegistry.empty()`, prompt "You are a helpful AI assistant", javadoc "No
  Flink - just the agent engine", and it asks about Apache Flink. The real one is
  `example/banking/BankingFlinkJob`.
- `:35` runs `python -m agentic_pipeline ...` from the repo root; that package
  has no pyproject and imports pyagentic, so it needs the
  `cd ports/agentic-pipeline && PYTHONPATH=.:../pyagentic` form from
  `ports/README.md:157`.
- `:50` omits the mandatory `mvn -f ports/jagentic-core/pom.xml install`
  prerequisite.
- `examples-bin/markets/README.md:46,48` and the market run scripts do
  `flink run "$JAR" org.agentic...BondMarketAgentExample`, tokens after the jar
  are **program args**, so this launches the manifest `Main-Class`
  (`SimpleAgentExample`). Needs `flink run -c <class> "$JAR"`.
- `docs/guides/storage-quickstart.md:58` (redis) throws
  `IllegalStateException`, `getAvailableBackends(HOT)` returns only
  `{"memory"}` and `createShortTermStore` rejects non-memory outright.
  `:117-224` documents a whole PostgreSQL option that does not exist:
  `StorageIntegratedFlinkJob` only branches on `"redis"`, so `postgresql`
  silently runs in-memory.
- `docs/examples/moderation.md:106` omits `examples-bin/setup-network.sh`,
  without which the external `agentic-flink-network` does not exist and the
  compose merge fails.

Fix: declare and pin `exec-maven-plugin` with `classpathScope=test` (or add an
`examples` profile that puts Flink on the runtime classpath), then re-verify
every documented command in CI.

## AGS-41: docs/reference: delete one file, rewrite two (P1)

**`docs/reference/external-state-storage.md` is entirely fictional, delete
it. ** Every class it documents is absent repo-wide: `ExternalStateStore`,
`RedisStateStore`, `DynamoDBStateStore`, `PostgreSQLStateStore`, and the methods
`saveContextItems`/`loadContextItems`/`saveLongTermFacts`/`loadLongTermFacts`/
`saveContextAsync` (0 hits each). Its DDL matches neither the real
implementation nor `sql/schema.sql`, a third, unrelated schema. Its Redis key
`agent:items:{flowId}` does not exist. It never mentions the actual architecture
(Flink-state-first memory, the `memory/conversation` ConversationStore SPI,
`LongTermMemoryStore`).

**`docs/reference/storage-architecture.md`, rewrite.** `RedisShortTermStore` is
in the diagram AND under "Completed " but does not exist; `StorageFactory`
actively rejects any HOT backend but `"memory"`. The status table is inverted:
Postgres, Qdrant and pgvector are marked "Planned" while all three ship and are
ServiceLoader-registered (plus Milvus, Fluss, InMemory, unmentioned); "storage
metrics In Progress " is stale (`StorageMetrics` is 384 complete lines). The
real HOT tier (`FlinkStateShortTermMemory` + `ShortTermMemorySpec`) is never
mentioned. Snippets that won't compile or throw: `.withHotTier("redis", ...)` +
`createShortTermStore()`; `new AgentContext()` (only ctor is 5-arg);
`env.addSource(new KafkaSource(...))` (needs FLIP-27 `fromSource` on Flink 2.x);
`AgenticFlinkJob` (missing class). Wrong versions: flink 1.17.2 (real 2.2.1), a
phantom `slf4j-api` 1.7.36 dep, caffeine 3.1.8 which is not in the pom at all.

**`docs/reference/rag-tools.md`, rewrite.** `new AgentExecutionStream(env,
config, toolRegistry)` is 3 args; the real ctor takes 4. The `java -cp
target/agentic-flink-0.0.1-SNAPSHOT.jar` command has the wrong version AND the
wrong artifact (needs `-uber`) AND still fails because Flink is provided. All
four Qdrant config keys are silently ignored (real: `qdrant.host`,
`qdrant.port`. **default 6334 gRPC, not 6333**,`qdrant.api.key`,
`qdrant.collection`). `modelName` is documented as the LLM key but is actually
the *embedding* model key, so the LLM config is a no-op and the embedder is
misconfigured. The doc claims Qdrant is the default when it is
`InMemoryVectorStore` (and Qdrant is commented out in `docker-compose.yml`).

**`docs/reference/agent-framework.md` is near-clean**, patch 6 one-liners:
`ChatLanguageModel` → `ChatModel` (`:146`); "eight SPIs" when the table lists
ten (`:53`); "four runnable examples" when there are seven (`:236`); the
VectorStore/EmbeddingConnection/InferenceConnection default rows; a package map
missing ~20 packages.

Both storage docs also claim "Last Updated: 2025-10-17".

## AGS-42: docs/guides: broken snippets, obsolete registration chapter, wrong env vars (P2)

**`docs/guides/creating-storage-backends.md` documents an obsolete registration
flow. ** It says to "add a case to `createShortTermStore()`", that method has no
switch; it hard-rejects anything but `"memory"`. The real mechanism
(`ServiceLoader` + `META-INF/services/org.agentic.flink.storage.{LongTermMemoryStore,VectorStore}`)
is **never mentioned in the guide**. Its `getAvailableBackends()` snippet shows
hardcoded arrays that don't exist (HOT really returns `{"memory"}`; WARM builds
a TreeSet from providers). Its `StorageConfiguration` example throws in
`validate()`. Its Caffeine example needs a dependency that is not in the pom and
never says so. It lists `RedisShortTermStore` as an existing HOT backend. Its
Testcontainers test needs `@Tag("integration")` to be picked up by the
documented `-P integration-tests` profile.

`new ContextItem("item-1", "First item")` appears **8 times** and will not
compile, `ContextItem` has only `@NoArgsConstructor`, the 12-arg
`@AllArgsConstructor`, and `ContextItem(String content, ContextPriority,
MemoryType)`.

**Wrong env var names** (`storage-quickstart.md:323-327`): documents
`POSTGRES_URL`/`POSTGRES_USER`/`POSTGRES_PASSWORD` as "Recommended for
Production", but `AgenticFlinkConfig` maps `postgres.url` →
**`AGENTIC_FLINK_POSTGRES_URL`**. Bare `POSTGRES_URL` is never read. Also
documents `postgres.connection.timeout`/`postgres.idle.timeout` keys that are
not implemented (timeouts are hardcoded, and the real connection timeout is
5000 ms, not the documented 30000).

Other items: p95/p99 latency metrics are documented but `StorageMetrics` has no
percentile methods (real format is `avg=/max=`); the Redis key
`agent:shortterm:{flowId}` does not exist; `STORAGE_ARCHITECTURE.md` and
`DELIVERY_SUMMARY.md` are referenced and do not exist; `PluggableStorageExample`
is presented as a plain example but is excluded from the default build; the
VECTOR tier is called "planned" when five impls ship; the
`FlinkKafkaConsumer`/`addSource` snippet is pre-Flink-2.x.

**docs/examples fixes**: `rag.md:98` `toolRegistry.register(...)`, `ToolRegistry`
has no `register` method (it is on the nested builder as `registerTool`); the
same broken line is duplicated in
`src/main/java/org/agentic/flink/example/rag/README.md:81`.
`support-triage.md:81-82` documents `ChatLanguageModel.generate(system, user)`,
the escape hatch returns `ChatModel` and the example calls one-arg
`chat(String)`; `ChatLanguageModel` does not exist in LangChain4J 1.16.3.
`live-research.md`: `BroadcastCorpus(...)` has no such constructor (it is
`BroadcastCorpus.spec(name, vectorSpec)`); `ExternalCorpus.spec("pgvector", ...)`
puts the backend in the name slot; "two ingest streams" is one pipeline with two
records; the `crawl-requests` Kafka topic under "What you should see" is
commented out in the example. `rag.md:34` shows `Scorer.scorePair(passage,
question)`, real arity is 3.

Repo-wide: docs use `docker compose` while CLAUDE.md says Podman, and
`examples-bin/_common.sh:22` also says docker, so the repo is internally
inconsistent, not just the docs.

## AGS-43: Reconcile the headline paradigm claims with each runtime (P1)

Measured position per claim, given the bar (native state + checkpoints counts):

- **Event-sourced / state as a materialized view over an ordered log**: clears
  the bar on `agentic-pekko`, `agentic-clj` and `ports/temporal`. Does **not**
  clear it on the flagship Flink runtime (AGS-21), nor in
  jagentic-core/pyagentic/go, where `replay/` is ~1.5% of the code, referenced
  by nothing but its own test, an opt-in observer off the write path, and
  "replay" is a for-loop over `submit` that re-fires tools and re-invokes
  brains. `Event` carries no sequence/offset/timestamp in any of the three.
- **CQRS**: real only on Temporal. Elsewhere the "query side" is a getter
  returning the same mutable object the command path writes.
- **Single-writer-per-conversation**: enforced structurally by Pekko (actor
  mailbox). Elsewhere one process-local lock (lost across processes; the Redis
  store uses bare HSET/RPUSH with no WATCH/MULTI), absent entirely in Clojure,
  provably broken in the celery port. **No concurrency test exists in any core.**
- **"A dozen other engines" / "one pipeline.yaml, every runtime"**: the shared
  spec and independent loaders are genuinely real and, when fixtures resolve, 21
  parity/replay/CEP tests pass. But only `local`, `celery`, `nats` (Python),
  `local`/`nats` (Go) and `local`/`pekko` (JVM) are selectable `backend:`
  values, so `banking-everywhere.md:63`'s 11-name list is false.
- **Exactly-once**: not delivered. `ToolInvocationChannel:33`'s "exactly-once
  with checkpoints" is unfounded.

Recommendation: keep the strong claims where a runtime earns them and *name the
runtime*; downgrade the generic framing to "state as a materialized view over
the runtime's own ordered log + checkpoints, where the runtime provides it". Add
a per-runtime capability matrix that CI regenerates so it cannot drift again.

**Also stop the false advertising inside the code**: five `GoalPredicate` "Not
yet implemented" notes sit above **working** implementations;
`ValidationExecutor:19`/`CorrectionExecutor:29` say "Placeholder" on complete
code that is simply never invoked; `AgentExecutor:21-28` claims
validation/correction/supervision it never runs; the compaction path logs
"Compressed N items" while producing no summary and calling no LLM; `HnswGraph`
advertises ">90% recall at 10⁵ vectors" with a single n=300/d=64 test asserting
≥0.8, and the class is a single-layer NSW graph named Hnsw;
`PostgresA2ATaskStore:25` says "H2/PostgreSQL compatible" (see AGS-16).

**And `TEST_REPORT.md` is stale**: dated 2026-05-26, claims 487/487 on Java 17.
Measured on JDK 21: **770 tests, 0 failures, 0 skipped**. Regenerate from CI or
delete.

---

## Corrections to the audit itself

Recorded so they are not "rediscovered" as findings:

- **The langchain4j and a2a-java BOMs resolve fine.** An early offline build
  failure was misattributed to them; the only unresolvable artifact was the
  unpublished `jagentic-core`.
- **The shaded-jar setup is correct**, not an anti-pattern, thin main artifact
  plus a classified `-uber` secondary (see AGS-12).
- **`examples-bin/` is not committed build output**, it is intentionally
  tracked scripts, just misnamed.
- **A mistyped `removeEldestEntry` does not compile as a silent overload.**
  javac rejects the whole erasure-clash class, so the compiler, not a test,
  guards AGS-9's specific bug. The test added alongside it guards the *capacity
  bound*, which the compiler cannot check.
- **`tasks.uid` already existed in the tracker** and the CRDT already keyed rows
  by it, with deterministic re-keying on collision. An early claim that
  concurrent key allocation was unhandled was wrong. The real defect was that
  the GitHub marker published the *mutable* key; ai-grind PR #5 fixed it to
  publish the uid.
- `ConversationEventSource` and `StorageAwareConversationProcessor` are **not**
  missing, both are private nested classes in `StorageIntegratedFlinkJob`.
- `ToolExecutorRegistry.register(...)` **does** exist, so
  `rag-tools.md:262`'s use of it is correct; only `ToolRegistry` lacks
  `register`.
