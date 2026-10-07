# `ports/experimental/`: engine adapters that predate `agentic/v1`

The adapters in this directory predate the `agentic/v1` spec. They are not conformance
tested: none of them runs the 22 fixtures under [`spec/conformance/v1`](../../spec/conformance/v1/),
none appears in the generated capability matrix [`docs/capabilities.md`](../../docs/capabilities.md),
and none is on the acceptance path, which is: define a workflow once, select a runtime, get the
same observable behavior. They may be removed. What each one does is run the banking worked
example (`router -> path -> verifier`) on its engine, reusing one of the shared cores:
[`ports/pyagentic`](../pyagentic/) (Python), [`ports/jagentic-core`](../jagentic-core/) (JVM),
or its own Go core. The cores are conformance tested; the adapters are not.

The conformance tested runtimes are the bindings listed in `docs/capabilities.md` (reference,
jvm-core, flink, pekko, clojure, python, pyflink, python-jvm, python-flink). The Pekko runtime
of record is the top-level [`agentic-pekko/`](../../agentic-pekko/) module, not anything here.

Nothing under this directory may be imported by `ports/jagentic-core`, `ports/pyagentic`, the
main Flink module, `agentic-pekko`, `agentic-clj`, `pyflink`, or `python/`. The one allowed
consumer is the backend registry in
[`ports/agentic-pipeline/agentic_pipeline/backends.py`](../agentic-pipeline/agentic_pipeline/backends.py),
which imports the installed `agentic_celery`, `agentic_nats` and `agentic_ray` packages by name
for its `celery`, `nats` and `ray` backends (see "Pipeline backends" below).

The design note for each engine is under [`docs/portability/`](../../docs/portability/). The
comparison table across all of them is in [`ports/README.md`](../README.md).

## Python adapters (over `ports/pyagentic`)

Every Python adapter is an installable package named `agentic-<engine>` with a `pyproject.toml`
and a package directory `agentic_<engine>` (the gateway is `agentic-gateway-fastapi`, package
`gateway_fastapi`). The core `pyagentic` is a dependency declared without a version, and each
`pyproject.toml` carries a `[tool.uv.sources]` entry pointing at `../../pyagentic` as an editable
path, so `uv pip install -e` resolves the core from the checkout. The engine itself is an
optional extra of the same name, and `pytest` is the `test` extra. No adapter manipulates
`sys.path`: with the packages installed, the adapters, their tests and `ports/agentic-pipeline`
import them like any other distribution.

The package directories are named `agentic_<engine>`, never `<engine>`, so that nothing under
this directory shadows the engine it ports to. From inside `ports/experimental`,
`python -c "import celery, dask, ray"` must resolve to the installed distributions;
`tests/test_celery.py`, `test_dask.py` and `test_ray.py` assert that.

### Install (workspace-style development install)

Every command below runs from the repository root. Install the core first, then every adapter
with the engines you have, as editable packages into one virtual environment:

```bash
python -m venv .venv-ports && . .venv-ports/bin/activate
pip install -e 'ports/pyagentic[test]'
pip install -e 'ports/experimental/celery[celery]' \
            -e 'ports/experimental/nats[nats]' \
            -e 'ports/experimental/ray[ray]' \
            -e 'ports/experimental/dask[dask]' \
            -e 'ports/experimental/faust[faust]' \
            -e 'ports/experimental/airflow[airflow]' \
            -e 'ports/experimental/gateway-fastapi[celery,nats,test]'
```

With `uv`, `uv pip install -e 'ports/experimental/<engine>[<engine>]'` pulls `pyagentic` from the
relative path in `[tool.uv.sources]` without installing it separately. Leave an extra out when the
engine is not wanted: the adapter still installs and its engine-free functions and tests run;
the engine-backed tests skip and print why. `apache-airflow>=3` pins many transitive versions,
so the `airflow` extra is best installed into its own virtual environment. Nothing here is
published, there is no publishing configuration, and none of these names exist on PyPI.

| Distribution | Import | Extra | What it does | Run it |
|---|---|---|---|---|
| `agentic-celery` ([`celery/`](celery/)) | `agentic_celery` | `celery` | one Celery task per turn, conversations routed to a stable queue by `zlib.crc32` (a per-process `hash()` would split one conversation across workers); eager mode needs no broker | `python -m agentic_celery` |
| `agentic-nats` ([`nats/`](nats/)) | `agentic_nats` | `nats` | JetStream stream per turn, JetStream KV envelope per conversation (subject-safe keys), an ordered worker | start a server, then `python -m agentic_nats` |
| `agentic-ray` ([`ray/`](ray/)) | `agentic_ray` | `ray` | one Ray actor per conversation in an isolated namespace; `RayRuntime.close()` kills the actors and the local cluster it started | `python -m agentic_ray` |
| `agentic-dask` ([`dask/`](dask/)) | `agentic_dask` | `dask` | the batch data plane: parallel ingestion, recall@k eval, transcript replay (parallel across conversations, ordered within one); falls back to sequential without Dask | `python -m agentic_dask` |
| `agentic-faust` ([`faust/`](faust/)) | `agentic_faust` | `faust` | a keyed Faust agent with Faust Tables as the `ConversationStore`; the turn logic runs over in-memory tables without a broker | `python -m agentic_faust`; the worker is `faust -A agentic_faust.app worker -l info` against Kafka |
| `agentic-airflow` ([`airflow/`](airflow/)) | `agentic_airflow` | `airflow` | a branching Airflow 3 DAG (`airflow.sdk`) per turn plus an ingestion DAG; `simulate()` runs the routing without a scheduler or a model | `python -m agentic_airflow`; `airflow dags test routed_triage` with the DAGs folder set to the package |
| `agentic-gateway-fastapi` ([`gateway-fastapi/`](gateway-fastapi/)) | `gateway_fastapi` | `celery`, `nats`, `test` | FastAPI HTTP front door over the core with `local`, `celery` and `nats` backends | `python -m gateway_fastapi` |

Each adapter's own `README.md` documents its seams and the engine-specific run commands.

### Pipeline backends

`ports/agentic-pipeline` registers an adapter as a `backend:` value only when its test suite runs
`examples/pipelines/banking.yaml` end to end on the real engine, locally or in a Podman
container. That holds for `celery` (eager tasks), `nats` (a JetStream server in Podman) and
`ray` (a local cluster), alongside the always-available `local`.

`airflow` (an orchestration plane, one DAG run per turn), `dask` (a batch data plane), `faust`
(a Kafka worker; no Kafka-backed end to end test of `banking.yaml` runs in this repository) and
`gateway-fastapi` (an HTTP edge over the backends, not a runtime) are demonstration-only. The
pipeline CLI rejects them by name:

```text
$ python -m agentic_pipeline run examples/pipelines/banking.yaml --backend dask --text "hi"
error: backend 'dask' is demonstration-only (a batch data plane (ingestion, retrieval eval,
transcript replay), not a turn runtime); demonstration-only adapters under ports/experimental:
airflow, dask, faust, gateway-fastapi; supported backends: celery, local, nats, ray
```

`ports/agentic-pipeline/tests/test_pipeline.py` pins the registry to exactly those four names
and asserts the rejection message for every demonstration-only adapter.

### Tests and lint

```bash
python -m pytest ports/experimental -q -rs          # every adapter module, from the repo root
python -m pytest ports/agentic-pipeline -q          # backend registration and banking.yaml on each backend
ruff check ports/experimental                       # rules in ports/experimental/ruff.toml
```

[`pytest.ini`](pytest.ini) collects [`tests/`](tests/) and `gateway-fastapi/tests/` and declares
one marker per engine (`airflow`, `celery`, `dask`, `faust`, `gateway`, `nats`, `ray`); `-rs` is on
by default so every skip prints its reason. A test skips only when its engine is genuinely
unavailable: the import error of a missing extra, or the connection error of an unreachable
JetStream server. Everything that does not need the engine (Celery routing, Dask sequential
fallback, Faust table store, NATS key mapping and envelopes, Airflow `simulate`) always runs.
[`tests/test_event_keywords.py`](tests/test_event_keywords.py) parses every adapter and test
source and fails on a positional `Event(...)` or `AgentContext(...)` call, because the Python
core's second positional field is `text` while the Java and Go cores put `userId` there.

NATS JetStream for the live `nats` tests, the `nats` pipeline backend and the gateway's `nats`
backend runs in Podman:

```bash
podman run -d --name nats-js -p 4222:4222 nats:latest -js
# a different server: export AGENTIC_NATS_URL=nats://host:4222
```

## JVM adapters (over `ports/jagentic-core`)

Install the core first: `./mvnw -f ports/jagentic-core/pom.xml install -DskipTests`. Each
adapter is its own Maven project, not a module of the root reactor.

| Adapter | What it does | How to run it |
|---|---|---|
| [`kafka-streams/`](kafka-streams/) | Processor API topology with a state store per conversation | `./mvnw -f ports/experimental/kafka-streams/pom.xml test`; `./mvnw -f ports/experimental/kafka-streams/pom.xml exec:java` prints the topology without a broker |
| [`temporal/`](temporal/) | one durable workflow per conversation, turns as update methods | `./mvnw -f ports/experimental/temporal/pom.xml test`; `./mvnw -f ports/experimental/temporal/pom.xml -q compile exec:java` runs on an in-memory Temporal test service |
| [`pulsar/`](pulsar/) | a Pulsar Function with function state as the stores | `./mvnw -f ports/experimental/pulsar/pom.xml test`; `./mvnw -f ports/experimental/pulsar/pom.xml -q exec:java` runs with an in-memory `Context` |
| [`spring/`](spring/) | Spring Boot service hosting the core behind HTTP | `./mvnw -f ports/experimental/spring/pom.xml test` (compiles; there are no tests); `./mvnw -f ports/experimental/spring/pom.xml spring-boot:run` |
| [`quarkus/`](quarkus/) | Quarkus reactive service hosting the core behind HTTP | `./mvnw -f ports/experimental/quarkus/pom.xml test` (compiles; there are no tests); `./mvnw -f ports/experimental/quarkus/pom.xml quarkus:dev` |
| [`pekko/`](pekko/) | README only. The original `ports/pekko` proof-of-concept was deleted; `agentic-pekko/` is the Pekko runtime | `./mvnw -f agentic-pekko/pom.xml test` |

## Go (its own core)

| Adapter | What it does | How to run it |
|---|---|---|
| [`go/`](go/) | a pure-Go core with a `LocalRuntime`, a NATS JetStream engine, a Temporal engine, a stdlib HTTP gateway, and a `pipeline.yaml` loader, in one module | `cd ports/experimental/go && go build ./... && go test ./...` (the natsjs tests run only when a JetStream server is up); `go run ./cmd/demo`, `go run ./cmd/gateway`, `go run ./cmd/natsdemo`, `go run ./cmd/pipeline ../../../examples/pipelines/banking.yaml --text "what is my balance?"` |

## CI

The JVM adapters are built by the `ports-experimental` job in
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). That job is advisory
(`continue-on-error`), is not a required status check, and is separate from the core JVM job.
The Go module runs in the `go` job and the Python adapters (`pytest ports/experimental`) in the `python-experimental` job.
