# Runtimes

One `agentic/v1` workflow document, one runtime chosen at deploy time, the same observable
behavior. These pages describe what each runtime is made of and where it stands in the generated
conformance matrix, [capabilities.md](../capabilities.md). The matrix is the only place a
capability status is authored; each page below carries an excerpt derived from it by
`docs/tools/matrix_excerpt.py`, and `docs/tools/test_docs.py` fails when an excerpt and the matrix
disagree.

| Page | Binding column(s) in the matrix |
|---|---|
| [Common primitives](common-primitives.md) | `reference` (what every binding is measured against) |
| [Flink](flink.md) | `flink` |
| [Pekko](pekko.md) | `pekko` |
| [Clojure](clojure.md) | `clojure` |
| [Pure Python](python.md) | `python` |
| [Python facade (JVM-backed)](python-facade.md) | `python-jvm`, `python-flink` |
| [PyFlink](pyflink.md) | `pyflink` |
| [Experimental adapters](experimental.md) | none |

The JVM core that Flink, Pekko and the Python facade share (`ports/jagentic-core`) is the
`jvm-core` column; it is described on the [common primitives page](common-primitives.md#the-cores).
