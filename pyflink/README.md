# agentic-pyflink

The PyFlink binding of `agentic/v1`: Python supplies the workflow document, the Flink
configuration and the connectors, and the job runs the Java `WorkflowTurnFunction` through the
bridge jar under `java/`. It is the `pyflink` column of the generated
[capability matrix](../docs/capabilities.md).

- What the runtime is made of, how `deploy` / `submit` / `restart` work, and where it stands in
  the matrix: [docs/runtimes/pyflink.md](../docs/runtimes/pyflink.md).
- The Python API, high-level and full-control, with executed examples on this runtime and the
  others: [docs/python.md](../docs/python.md).
- The three ways Flink is reachable from Python, told apart: [docs/pyflink.md](../docs/pyflink.md).

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw install -DskipTests
./mvnw -f pyflink/java/pom.xml package
cd pyflink && python -m venv .venv && .venv/bin/pip install -e '.[test]' 'apache-flink==2.2.1'
.venv/bin/pytest
.venv/bin/python -m agentic_pyflink run ../spec/conformance/v1/workflows/support.yaml --text "what is my balance?"
```
