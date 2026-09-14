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
which can select the `celery` and `nats` adapters below by name when their directory is on
`PYTHONPATH`; its tests pin the adapter paths.

The design note for each engine is under [`docs/portability/`](../../docs/portability/). The
comparison table across all of them is in [`ports/README.md`](../README.md).

## Python adapters (over `ports/pyagentic`)

Install the core first: `python -m pip install -e ports/pyagentic`. Every command below runs
from the repository root.

| Adapter | What it does | How to run it |
|---|---|---|
| [`celery/`](celery/) | one Celery task per turn; eager mode runs the banking example in-process with no broker | `python -m pip install celery` then `python ports/experimental/celery/agentic_celery.py`. Also selectable as `--backend celery` in `agentic-pipeline` with `ports/experimental/celery` on `PYTHONPATH` |
| [`nats/`](nats/) | NATS JetStream subjects per conversation, JetStream KV as the keyed state store | `python -m pip install nats-py`, start a server (`podman run -d -p 4222:4222 nats:latest -js`), then `python ports/experimental/nats/agentic_nats.py`. Also selectable as `--backend nats` in `agentic-pipeline`. Without a server it fails with `ConnectionRefusedError` |
| [`dask/`](dask/) | the batch data plane: ingest and embed a corpus, evaluate recall, replay a transcript | `python -m pip install dask` then `python ports/experimental/dask/agentic_dask.py` |
| [`airflow/`](airflow/) | a routing DAG; without Airflow installed the module still simulates the routing decision | `python ports/experimental/airflow/agentic_banking_dag.py` (the DAG object is only built when `airflow` imports) |
| [`faust/`](faust/) | one Faust agent per conversation over Kafka | needs Kafka and `faust-streaming`; `cd ports/experimental/faust && faust -A agentic_faust:app worker -l info`. Without Faust installed the module imports and prints the install hint |
| [`ray/`](ray/) | one Ray actor per conversation | needs `ray[default]`; `python ports/experimental/ray/agentic_ray.py`. Without Ray installed the module imports and prints the install hint |
| [`gateway-fastapi/`](gateway-fastapi/) | FastAPI HTTP front door over the Python core with `local`, `celery`, and `nats` backends | `python -m pip install fastapi httpx pydantic uvicorn` then `python -m pytest ports/experimental/gateway-fastapi/tests -q`; serve with `cd ports/experimental/gateway-fastapi && python -m gateway_fastapi` |
| [`tests/`](tests/) | pytest over the adapters above (Celery and Dask on the real engine, NATS when a server is up, Faust and Ray import only) | `python -m pytest ports/experimental/tests -q` |

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
The Go module and the Python adapter tests run in the `go` and `python` jobs at their new paths.
