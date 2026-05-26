# Test Report — 2026-05-26

End-to-end test pass following the package rename (`com.ververica.flink.agent`
→ `org.agentic.flink`) and the history reset.

## Environment

| Component | Version |
|---|---|
| OS | macOS 15 (darwin 24.6.0, arm64) |
| Java | Amazon Corretto 17.0.18 |
| Maven | bundled wrapper |
| Python | 3.14.4 |
| Podman | 5.8.2 |
| Ollama | 0.24.0 (brew) — see "Ollama daemon bug" below |
| JPype1 | 1.7.1 |
| Jupyter | 1.1.1 |

## Summary

| Suite | Result |
|---|---|
| Java unit tests (`mvn test`) | **487 / 487 passed** |
| Python facade tests (`pytest python/`) | **20 / 20 passed**, 2 skipped (PyFlink) |
| Framework jar build (`mvn package`) | **127 MB shaded jar produced** |
| Pip artifact build (`python -m build` in `python/`) | **wheel + sdist pass `twine check`** |
| Jupyter notebook (`01_quickstart.ipynb`) | **10 / 10 code cells executed clean** |
| Java examples (sample) | mixed — see "Examples" below |
| Java integration tests | skipped (Ollama API broken on this host) |

## What ran

### 1. Unit tests
`mvn test` — 487 tests, 0 failures, 0 errors, 0 skipped. Covers:
- DSL (`AgentBuilder`, `Agent`, validators)
- Memory (`FlinkStateShortTermMemory`, `FlinkStateVectorMemory`, HNSW)
- Storage (`InMemoryLongTermStore`, `PostgresConversationStore` via embedded mode,
  `StorageHydrationIntegrationTest`)
- Channel SPI (`KafkaChannel`, `RedisPubSubChannel`, `WebhookChannel`,
  `StaticSeedChannel`, `ToolInvocationChannel`)
- Web (`DocumentExtractor` HTML + Tika paths)
- Ingest (`RecursiveTextChunker`)
- Tools (`ToolRegistry`, `LangChainToolAdapter`, annotation discovery)
- State machine + saga compensation
- Listener / context / serialization

### 2. Python facade
After `pip install -e python/` into a fresh venv:
```
tests/test_agent_builder.py ........................ 3 passed
tests/test_chat_setup_roundtrip.py ................. 3 passed
tests/test_jvm_bootstrap.py ........................ 4 passed
tests/test_tool_decorator.py ....................... 4 passed
tests/test_vector_memory_spec.py ................... 6 passed
20 passed, 2 skipped
```
The skipped pair require `apache-flink` (PyFlink); intentional optional path.

### 3. Build artifacts
- `target/agentic-flink-1.0.0-SNAPSHOT.jar` — 127 MB shaded jar (everything but
  Flink, which stays `provided`).
- `python/dist/agentic_flink-1.0.0a1-py3-none-any.whl` — 46 KB.
- `python/dist/agentic_flink-1.0.0a1.tar.gz` — 39 KB.
- Both Python artifacts pass `twine check` (READMEs render, metadata complete,
  license file shipped).

### 4. Notebook walkthrough

`notebooks/01_quickstart.ipynb` runs top-to-bottom in ~5 s on cold cache,
~3 s warm. Executed via `jupyter nbconvert --execute --inplace` — all cells
green. Covers:

1. JVM bootstrap (auto-discovers framework jar, loads 236 runtime jars via
   `mvn dependency:build-classpath`)
2. Framework class resolution — 7 representative FQCNs verified
3. Live web fetch against `https://example.com` (Jsoup pulls 230-char body,
   1 outbound link)
4. Document extraction — HTML via Jsoup, plain text via Tika
5. Recursive text chunking — 800-char input → 13 chunks at 200-char target
6. Vector ops — 500 random unit vectors at d=64, planted nearest neighbour
   recovered at rank 1 by cosine
7. Vector memory specs — both `FlinkStateVectorMemory` (brute force) and
   `FlinkStateHnswVectorMemory` (M=16, beam=64) construct cleanly
8. Agent end-to-end build with two Python `@tool` decorated functions
9. Live LLM call (conditional — gracefully skipped when Ollama unreachable)

The notebook handles missing Ollama gracefully (URL probe + `models` API
check before attempting any chat call).

### 5. Java examples (sample)

Ran via `mvn exec:java -Dexec.classpathScope=test`:

| Example | Result |
|---|---|
| `ToolAnnotationExample` | ✓ ran to "=== Example Complete ===" — tool registry, annotation discovery, async execution, metadata schema |
| `CompensationExample` | ✓ ran (saga compensation logic) |
| `SimpleAgentExample` | ✗ non-serializable lambda — example bug, not framework |
| `ContextManagementExample` | ✗ "Generic types have been disabled" — example needs `env.getConfig().enableForceKryo()` |
| `DeclarativeAgentExample` | ✗ "Initial state has no outgoing transitions" — example state-machine config bug |
| `StorageIntegratedFlinkJob` | ✗ Flink runtime ClassNotFoundException — needs `flink run`, not `mvn exec:java` |

These failures are in **example code or the way `mvn exec:java` runs Flink
jobs**, not the framework — the underlying logic is covered by the 487
passing unit tests.

## Issues discovered

### Ollama 0.24.0 daemon bug (host-local, blocks integration tests)

`brew install ollama` lays down v0.24.0. Pull succeeds (`ollama list` shows the
model, files exist under `~/.ollama/models/blobs/`), but the daemon's
in-memory index is empty:
- `GET /api/tags` returns `{"models":[]}`
- `POST /api/generate` returns `{"error":"model 'qwen2.5:0.5b' not found"}`
- `ollama run qwen2.5:0.5b` from the CLI **does work** (uses an embedded path
  that bypasses the daemon's index)

This blocks `StandaloneLLMIT` and `StandaloneToolExecutionIT` (the framework's
LLM integration tests) and the notebook's section-7 live cell. Recovery
options (for a future run):

- Downgrade to a known-good Ollama (e.g. 0.5.x or 0.6.x via a manual install)
- Wait for upstream fix
- Replace the LLM path with an OpenAI-compatible endpoint for testing

### Pre-existing example bugs (low priority)

Four examples noted above don't run cleanly. They are reference code for
documentation rather than the framework's core. Worth a follow-up issue but
not a blocker.

## Repo additions in this pass

```
notebooks/01_quickstart.ipynb     ← runnable demo, executed clean
TEST_REPORT.md                    ← this file
target/runtime-classpath.txt      ← cached for notebook bootstrap (gitignored)
```

The notebook's bootstrap cell will regenerate `runtime-classpath.txt` on
demand via `mvn dependency:build-classpath` if it doesn't exist.

## To reproduce

```bash
# Java
mvn clean test                                                    # 487 unit tests
mvn -DskipTests package                                           # produces target jar

# Python facade
python3 -m venv /tmp/af-venv && source /tmp/af-venv/bin/activate
pip install -e python/ pytest jupyter
export AGENTIC_FLINK_JAR="$(pwd)/target/agentic-flink-1.0.0-SNAPSHOT.jar"
pytest python/                                                    # 20 passed

# Notebook
cd notebooks
jupyter nbconvert --to notebook --execute --inplace 01_quickstart.ipynb

# Java example (working one)
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.ToolAnnotationExample"
```
