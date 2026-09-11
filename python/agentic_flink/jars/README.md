Package data directory for the shaded framework jar.

Copy `target/agentic-flink-<version>-uber.jar` here before building a wheel so a clean
`pip install agentic-flink` can start the JVM without a source checkout. Every `*.jar`
in this directory is added to the JVM classpath by `agentic_flink.start_jvm()`.
