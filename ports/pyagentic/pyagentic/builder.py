"""GraphBuilder — compile a declarative spec (a plain dict, e.g. parsed from YAML) into
a RoutedGraph + ToolRegistry + retriever. This is what lets a ``pipeline.yaml`` build
"the agentic system of your choice": the spec is engine-agnostic, so the same built
graph runs on any backend.

The spec shape (all sections optional except ``agent.paths``):

    agent:
      router:   {kind: keyword, rules: {cards: [card, crypto], payments: [balance]}, default: general}
      paths:
        cards:    {brain: rule|llm, prompt: "...", tools: [get_balance], tool_triggers: {balance: get_balance}}
      verifier: {kind: prefix|none}
    tools:      [{id: get_balance, kind: constant, value: 1234.56}]
    retrieval:  {embedder: hashing, dim: 256, kb: [{id: kb1, text: "..."}]}
    guardrails: [{kind: regex, deny: ["ignore (all|previous)"], reason: "...", check_outputs: false}]

LLM paths need a ``ChatClient`` (one per provider); pass a ``chat_client_factory`` to
:func:`build` so the loader can supply Ollama/OpenAI/stub without this module importing
providers.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .core import Agent, AgentContext, Event, RoutedGraph
from .guardrails import RegexGuardrail
from .llm import LlmBrain
from .retrieval import InMemoryHotVectorIndex, TwoTierRetriever, hashing_embedder
from .tools import ToolRegistry

ChatClientFactory = Callable[[Dict[str, Any]], Any]  # (llm_spec) -> ChatClient


class KeywordBrain:
    """A generic, model-free brain: fire a tool when a trigger keyword appears, else
    answer from retrieval, else echo. Reproduces the banking RuleBrain behaviour from a
    declarative spec."""

    def __init__(self, name: str, embed, tool_triggers: Optional[Dict[str, str]] = None, threshold: float = 0.15):
        self.name = name
        self._embed = embed
        self.tool_triggers = tool_triggers or {}
        self.threshold = threshold

    def turn(self, user_text: str, ctx: AgentContext) -> str:
        low = user_text.lower()
        for keyword, tool in self.tool_triggers.items():
            if keyword in low:
                result = ctx.call_tool(tool, {"user": ctx.user_id})
                return f"[{self.name}] {tool} returned {result}"
        if ctx.retriever is not None:
            hits = ctx.retriever.retrieve(self._embed(user_text), 1)
            if hits and hits[0].score > self.threshold:
                return f"[{self.name}] {hits[0].text}"
        return f"[{self.name}] I can help with {self.name} questions. You said: {user_text!r}"


def _build_tools(specs: List[Dict[str, Any]]) -> ToolRegistry:
    reg = ToolRegistry()
    for t in specs or []:
        tool_id = t["id"]
        kind = t.get("kind", "constant")
        desc = t.get("description", tool_id)
        if kind == "constant":
            value = t.get("value")
            reg.register(tool_id, desc, lambda params, v=value: v)
        elif kind in ("http", "agent"):
            # "agent" is an alias: call another agent/gateway's /agent endpoint (A2A-as-tool).
            url = _resolve_env(t["url"])
            reg.register(tool_id, desc, _http_tool(url))
        else:
            raise ValueError(f"unknown tool kind {kind!r} for {tool_id!r}")
    return reg


def _resolve_env(value: str) -> str:
    """Expand a ``${ENV}`` connection link (used for agent/http tool URLs)."""
    import os

    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.environ.get(value[2:-1], "")
    return value


def _http_tool(url: str) -> Callable[[Dict[str, Any]], Any]:
    """A tool that POSTs its params as JSON to ``url`` and returns the JSON response —
    e.g. an A2A peer call to another gateway's /agent."""
    import json
    import urllib.request

    def call(params: Dict[str, Any]) -> Any:
        req = urllib.request.Request(
            url, data=json.dumps(params or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    return call


def _build_embedder(emb_spec: Optional[Dict[str, Any]], retrieval_spec: Optional[Dict[str, Any]]):
    """Resolve the embed function + dim. An ``embeddings:`` section picks a real provider
    (litellm/ollama/openai) via the Embedder SPI; otherwise the deterministic FNV hashing
    embedder is used at the retrieval ``dim`` (default 256)."""
    if emb_spec:
        from .embeddings import make_embedder

        embedder = make_embedder(emb_spec)
        return embedder.embed, embedder.dim
    dim = int((retrieval_spec or {}).get("dim", 256))
    return hashing_embedder(dim), dim


def _build_retriever(spec: Optional[Dict[str, Any]], embed, dim: int):
    """Build the two-tier retriever. The hot tier is always an in-memory window seeded with
    the ``kb``; a ``vector_store:`` section adds a real cold tier (memory/hnsw/duckdb/qdrant),
    also seeded with the kb so cold recall works."""
    if not spec:
        return None
    hot = InMemoryHotVectorIndex()
    store = None
    vs_spec = spec.get("vector_store")
    if vs_spec:
        from .vectorstores import make_vector_store

        store = make_vector_store(vs_spec, dim)
    cold = store.cold_search() if store is not None else None
    for doc in spec.get("kb", []) or []:
        vec = embed(doc["text"])
        hot.upsert(doc["id"], vec, doc["text"])
        if store is not None:
            store.upsert(doc["id"], vec, doc["text"])
    return TwoTierRetriever(hot, cold, 4, 4)


def _register_mcp(tools: ToolRegistry, specs: Optional[List[Dict[str, Any]]]) -> None:
    """Connect to each declared MCP server and register its tools (id-prefixed by name)."""
    for m in specs or []:
        from .mcp_client import McpClient

        transport = (m.get("transport") or "stdio").lower()
        if transport != "stdio":
            raise ValueError(f"mcp transport {transport!r} not supported (use 'stdio')")
        raw = m["command"]
        command = raw if isinstance(raw, list) else [raw, *m.get("args", [])]
        client = McpClient([_resolve_env(str(c)) for c in command])
        client.register(tools, prefix=f"{m.get('name', 'mcp')}_")


def _register_a2a(tools: ToolRegistry, specs: Optional[List[Dict[str, Any]]]) -> None:
    """Register each declared peer agent as a tool (peer-as-tool over A2A HTTP)."""
    for a in specs or []:
        from .a2a import peer_tool

        url = _resolve_env(a["url"])
        tools.register(a["id"], a.get("description", f"Delegate to peer agent {a['id']}"),
                       peer_tool(url, int(a.get("retries", 2))))


def _build_guardrail(g: Dict[str, Any], embed):
    """Build one guardrail from its spec. kind = regex (default) | classifier."""
    kind = g.get("kind", "regex")
    if kind == "regex":
        return RegexGuardrail(deny=g.get("deny", []), reason=g.get("reason", "blocked by policy"),
                              check_outputs=bool(g.get("check_outputs", False)))
    if kind == "classifier":
        from .inference import ClassifierGuardrail, EmbeddingClassifier, LexiconClassifier

        ctype = (g.get("classifier") or "lexicon").lower()
        if ctype == "lexicon":
            clf = LexiconClassifier(g["lexicon"], default_label=g.get("default_label", "other"))
        elif ctype == "embedding":
            clf = EmbeddingClassifier().fit(g["examples"])
        else:
            raise ValueError(f"unknown classifier {ctype!r}; choose lexicon|embedding")
        return ClassifierGuardrail(clf, blocked_labels=g.get("blocked", []),
                                   threshold=float(g.get("threshold", 0.5)),
                                   reason=g.get("reason", "blocked by classifier policy"),
                                   check_outputs=bool(g.get("check_outputs", False)))
    raise ValueError(f"unknown guardrail kind {kind!r}; choose regex|classifier")


def _build_router(spec: Optional[Dict[str, Any]], paths: List[str]):
    spec = spec or {}
    kind = spec.get("kind", "keyword")
    default = spec.get("default") or (paths[-1] if paths else None)
    if kind != "keyword":
        raise ValueError(f"router kind {kind!r} not supported by GraphBuilder (use 'keyword')")
    rules: Dict[str, List[str]] = spec.get("rules", {})

    def router(event: Event, ctx: AgentContext) -> str:
        low = event.text.lower()
        for path, keywords in rules.items():
            if any(kw.lower() in low for kw in keywords):
                return path
        return default

    return router


def build(spec: Dict[str, Any], chat_client_factory: Optional[ChatClientFactory] = None):
    """Compile ``spec`` into ``(graph, tools, retriever)``."""
    agent_spec = spec.get("agent", {})
    path_specs: Dict[str, Dict[str, Any]] = agent_spec.get("paths", {})
    if not path_specs:
        raise ValueError("pipeline spec needs agent.paths")

    tools = _build_tools(spec.get("tools", []))
    _register_mcp(tools, spec.get("mcp"))
    _register_a2a(tools, spec.get("a2a"))

    embed, dim = _build_embedder(spec.get("embeddings"), spec.get("retrieval"))
    retriever = _build_retriever(spec.get("retrieval"), embed, dim)

    context_manager = None
    ctx_spec = spec.get("context")
    if ctx_spec:
        from .context import ContextWindowManager

        budget = int(ctx_spec.get("max_tokens", int(ctx_spec.get("max_items", 12)) * 64))
        context_manager = ContextWindowManager(budget)

    from .skills import SkillRegistry
    skills = SkillRegistry.from_specs(spec.get("skills", []))

    paths: Dict[str, Agent] = {}
    for name, ps in path_specs.items():
        brain_kind = ps.get("brain", "rule")
        prompt = ps.get("prompt", f"You answer {name} questions.")
        # Expand any skills this path declares into extra tools + a prompt fragment.
        skill_tools, fragment, _facts = skills.expand(ps.get("skills", []))
        if fragment:
            prompt = prompt + "\n" + fragment
        if brain_kind == "llm":
            if chat_client_factory is None:
                raise ValueError("spec uses an llm brain but no chat_client_factory was provided")
            client = chat_client_factory(spec.get("llm", {}))
            path_tools = list(ps.get("tools", []) or []) + [t for t in skill_tools if t not in (ps.get("tools") or [])]
            brain = LlmBrain(client, name=name, system_prompt=prompt,
                             tools=path_tools or None, max_iterations=int(ps.get("max_iterations", 6)),
                             output_schema=ps.get("output_schema"), context_manager=context_manager)
        elif brain_kind == "rule":
            brain = KeywordBrain(name, embed, tool_triggers=ps.get("tool_triggers"),
                                 threshold=float(ps.get("threshold", 0.15)))
        else:
            raise ValueError(f"unknown brain kind {brain_kind!r} for path {name!r}")
        paths[name] = Agent(name, prompt, brain)

    router = _build_router(agent_spec.get("router"), list(paths.keys()))

    verifier = None
    vspec = agent_spec.get("verifier", {})
    if vspec.get("kind", "prefix") == "prefix":
        verifier = lambda reply, ctx: (bool(reply) and reply.startswith("["), reply)  # noqa: E731

    guardrails = [_build_guardrail(g, embed) for g in spec.get("guardrails", []) or []]

    graph = RoutedGraph(router=router, paths=paths, verifier=verifier, guardrails=guardrails)
    return graph, tools, retriever
