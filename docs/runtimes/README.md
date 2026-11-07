# Runtimes

One `agentic/v1` workflow document, one runtime chosen at deploy time, the same observable behavior
for what the fixtures cover. These pages describe what each runtime is made of.

| Page | Binding in the conformance runner |
|---|---|
| [Common primitives](common-primitives.md) | `reference` (what every binding is measured against) |
| [Flink](flink.md) | `flink` |
| [Pekko](pekko.md) | `pekko` |
| [Pure Python](python.md) | `python` |
| [Moved out of this repository](experimental.md) | none |

The JVM core that Flink and Pekko share (`ports/jagentic-core`) is the `jvm-core` binding; it is
described on the [common primitives page](common-primitives.md#the-cores).

## Conformance results

No capability matrix is committed to this repository. An earlier `docs/capabilities.md` was removed:
it named a commit that is not in this repository's history, and it reported `llm_brain: supported`
where only a scripted stub ran. It stays out until CI regenerates it from a commit here.

Until then the results are the `conformance-matrix` artifact and job summary of the
[Conformance workflow](../../.github/workflows/conformance.yml), or a local run:

```bash
python spec/tools/conformance_matrix.py --output build/conformance-matrix.json --write-docs build/capabilities.md
```

Passing the fixtures is a narrow claim. They are mostly rule brains and scripted stubs, with no real
model. They do crash turns at every event and around every call, and on Flink that is a real job
restoring a savepoint, against a journal held in the process. What a tool sees across a restore with
a durable journal is shown by `PayAfterRestoreTest` and `EffectJournalStoresIT`, not by the fixtures.
