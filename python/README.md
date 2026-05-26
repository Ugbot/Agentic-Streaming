# agentic-flink (Python)

JPype-backed Python facade over the [Agentic Flink](../README.md) Java framework.

## Install

```bash
pip install agentic-flink
```

(Optional) PyFlink support:

```bash
pip install agentic-flink[pyflink]
```

You'll also need the framework jar — see [`docs/python.md`](../docs/python.md)
for the discovery rules.

## Quick start

```python
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, langchain4j_ollama, tool

af.start_jvm()

@tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

agent = (
    Agent.builder()
        .with_id("calc-bot")
        .with_system_prompt("You are a calculator.")
        .with_chat_connection(langchain4j_ollama())
        .with_chat_setup(ChatSetup(model="qwen2.5:3b"))
        .with_tools(add)
        .build()
)
```

Full guide: [`docs/python.md`](../docs/python.md).
Runnable examples: `agentic_flink.examples.quickstart`,
`agentic_flink.examples.rag`, `agentic_flink.examples.live_research`.
