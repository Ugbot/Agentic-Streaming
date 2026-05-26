"""Minimal Python agent: a calculator tool exposed to an Ollama LLM.

What you see in this file:

- ``start_jvm()`` boots the JVM with the framework jar.
- ``@tool`` turns a normal Python function into a Java ``ToolExecutor``.
- ``Agent.builder()`` fluently configures the agent.
- We bypass the agent execution loop (which requires a full Flink job graph)
  and directly invoke the tool's Java proxy to demonstrate the bridge —
  this is the same code path the agent operator uses at runtime.

Prerequisites:

* ``mvn -DskipTests package`` from the repo root (or set ``AGENTIC_FLINK_JAR``).

Run::

    python -m agentic_flink.examples.quickstart
"""

from __future__ import annotations

import sys

import agentic_flink as af
from agentic_flink import Agent, ChatSetup, langchain4j_ollama, tool


@tool(name="add", description="Add two numbers.")
def add(a: int, b: int) -> int:
    return a + b


@tool
def greet(name: str) -> str:
    """Return a friendly greeting for `name`."""
    return f"Hello, {name}!"


def main() -> int:
    af.start_jvm()

    agent = (
        Agent.builder()
        .with_id("calc-bot")
        .with_system_prompt("You are a calculator that may also greet the user.")
        .with_chat_connection(langchain4j_ollama())
        .with_chat_setup(ChatSetup(model="qwen2.5:3b", temperature=0.2))
        .with_tools(add, greet)
        .with_max_iterations(5)
        .build()
    )

    print(f"agent: {agent.id}")
    print(f"prompt: {agent.system_prompt!r}")
    print(f"tools: {sorted(agent.allowed_tools)}")

    # Demonstrate that the tool can be invoked through the Java proxy —
    # this is the exact path the agent operator takes when the LLM calls it.
    HashMap = af.jclass("java.util.HashMap")
    args = HashMap()
    args.put("a", 17)
    args.put("b", 25)
    result = add._to_java().execute(args).get()
    print(f"add(17, 25) via Java proxy = {result}")

    args = HashMap()
    args.put("name", "world")
    print(f"greet(world) via Java proxy = {greet._to_java().execute(args).get()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
