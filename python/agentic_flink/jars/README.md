Package data directory for the shaded framework jar.

`python -m build` (see `python/setup.py`) places `agentic-flink-<version>-uber.jar` here
automatically: it reuses a jar already present, else copies `AGENTIC_FLINK_JAR`, else copies
`<repo>/target/agentic-flink-*-uber.jar`, else runs `./mvnw` to build it. Delete the jar from
this directory to force a fresh copy on the next build. Every `*.jar` in this directory is
added to the JVM classpath by `agentic_flink.start_jvm()`, so a clean `pip install agentic-flink`
can start the JVM without a source checkout.

The jar is ignored by git (`*.jar` in `.gitignore`); it only ever exists in build trees,
sdists and wheels.
