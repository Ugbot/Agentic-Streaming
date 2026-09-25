"""Legacy Flink DSL through the facade: Agent.builder() and @tool. Pure Flink flavor, outside agentic/v1.

    python docs/snippets/python/legacy_dsl.py
"""
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, flink_jars, langchain4j_ollama, tool

af.start_jvm(extra_jars=flink_jars())  # the shaded jar keeps Flink itself out (provided scope)


@tool(name="add", description="Add two integers")
def add(a: int, b: int) -> int:
    return a + b


agent = (
    Agent.builder()
    .with_id("calc-bot")
    .with_system_prompt("You are a calculator.")
    .with_chat_connection(langchain4j_ollama())
    .with_chat_setup(ChatSetup(model="qwen2.5:3b", temperature=0.3))
    .with_tools(add)
    .with_max_iterations(5)
    .build()
)

args = af.jclass("java.util.HashMap")()
args.put("a", 2)
args.put("b", 40)
print(agent.id, sorted(agent.allowed_tools), add._to_java().execute(args).get())
