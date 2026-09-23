# jagentic-core

The Flink-free Java core of `agentic/v1` (`org.jagentic:jagentic-core`, package
`org.jagentic.core`). It holds the workflow validator, the event log and its fold
(`ConversationLog`, `ConversationState`), the routed graph (`RoutedGraph`), the `Runtime` SPI and
`LocalRuntime`. The Flink framework, `agentic-pekko` and the Python facade all run this core
unchanged; it is the `jvm-core` column of the generated [capability matrix](../../docs/capabilities.md)
through `ConformanceTest` (`src/test/java/org/jagentic/core/conformance`).

What the core guarantees, and how the `jvm-core` column relates to the runtimes built on it, is on
the [common primitives page](../../docs/runtimes/common-primitives.md#the-cores). The runtimes that
host it have their own pages: [Flink](../../docs/runtimes/flink.md),
[Pekko](../../docs/runtimes/pekko.md), [Python facade](../../docs/runtimes/python-facade.md).

This module is not in the root Maven reactor and must be installed before anything else:

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -f ports/jagentic-core/pom.xml test -Dtest=ConformanceTest   # the jvm-core column
```
