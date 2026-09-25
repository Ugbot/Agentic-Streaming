# Experimental adapters

The engine adapters under `ports/experimental/` (Faust, Kafka Streams, Temporal, Pulsar
Functions, Ray, NATS JetStream, Quarkus, Spring, Celery, Dask, Airflow, a Go core, a FastAPI
gateway, and an earlier Pekko adapter) predate `agentic/v1`. They are not conformance tested: none
of them runs the fixtures under `spec/conformance/v1`, none has a column in the generated
[matrix](../capabilities.md), and none is on the acceptance path of defining a workflow once,
selecting a runtime and getting the same observable behavior. They may be removed.

What each one does, which shared core it reuses, how it is guarded in tests, and what may import
it is documented in one place: [ports/experimental/README.md](../../ports/experimental/README.md).
The comparison table across all of them is [ports/README.md](../../ports/README.md), and the
per-engine design notes are under [docs/portability/](../portability/README.md).

Two boundaries matter for readers of the runtime pages:

- The Pekko runtime of record is the top-level `agentic-pekko` module on the [Pekko page](pekko.md),
  not the adapter under `ports/experimental/pekko`.
- Nothing under `ports/experimental/` may be imported by the conformance tested cores and runtimes
  (`ports/jagentic-core`, `ports/pyagentic`, the main Flink module, `agentic-pekko`,
  `agentic-clj`, `pyflink`, `python/`). The one allowed consumer is the backend registry of
  `ports/agentic-pipeline`.

No capability claim on this page: there is no matrix row to back one.
