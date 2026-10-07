"""Agentic Streaming on **Apache Airflow**, pure Python, the ORCHESTRATION plane.

See ../../../docs/portability/airflow.md. Airflow isn't streaming, but the
router->path->verifier topology maps cleanly to a branching DAG:
  router = @task.branch  ->  path tasks (cards/payments/general)  ->  verifier (fan-in).
Per-conversation state lives in an external store (Redis/Postgres) via the
``pyagentic.ConversationStore`` SPI (inject it with :func:`configure`); XCom carries
only the small per-run payload.

This module is the engine-free half: the pure routing/turn functions the DAG tasks
call, plus :func:`simulate`, which runs the same logic through ``pyagentic`` so the
wiring is verifiable without a scheduler. ``agentic_airflow.dags`` defines the two
DAGs (routed triage + RAG ingestion) against the Airflow 3 task SDK and needs the
optional ``airflow`` extra:

    pip install 'agentic-airflow[airflow]'
    export AIRFLOW__CORE__DAGS_FOLDER=$(python -c 'import agentic_airflow.dags as d; print(d.__file__)' | xargs dirname)
    airflow dags test routed_triage --conf '{"text": "what is my balance?", "conversation_id": "c1"}'
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyagentic.banking import KB, build_banking_graph, default_tools, seed_kb
from pyagentic.core import AgentContext, Event, RoutedGraph
from pyagentic.memory import ConversationStore, InMemoryConversationStore, InMemoryKeyedStateStore
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever, hashing_embedder
from pyagentic.tools import ToolRegistry

__all__ = [
    "PATH_TASK_PREFIX",
    "build_index",
    "classify",
    "configure",
    "embed_corpus",
    "load_corpus",
    "run_path",
    "simulate",
]

PATH_TASK_PREFIX = "path_"

# Injectable deps (default to the shared banking essence). configure(...) lets a YAML
# loader / a custom workflow run an arbitrary graph through this DAG with no code change.
_GRAPH: Optional[RoutedGraph] = None
_TOOLS: Optional[ToolRegistry] = None
_RETRIEVER: Optional[TwoTierRetriever] = None
_STORE: Optional[ConversationStore] = None
_EMBED = hashing_embedder(64)


def configure(graph=None, tools=None, retriever=None, store=None) -> None:
    global _GRAPH, _TOOLS, _RETRIEVER, _STORE
    _GRAPH, _TOOLS, _RETRIEVER, _STORE = graph, tools, retriever, store


def _graph() -> RoutedGraph:
    return _GRAPH if _GRAPH is not None else build_banking_graph()


def _tools() -> ToolRegistry:
    return _TOOLS if _TOOLS is not None else default_tools()


def _retriever() -> TwoTierRetriever:
    if _RETRIEVER is not None:
        return _RETRIEVER
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    return TwoTierRetriever(hot, None, 4, 4)


def _store() -> ConversationStore:
    """The durable per-conversation store. Without an injected one every DAG run gets a
    fresh in-memory store, so multi-turn memory needs :func:`configure`."""
    return _STORE if _STORE is not None else InMemoryConversationStore()


def path_task_ids() -> List[str]:
    """The branch targets, one task per path of the configured graph."""
    return [PATH_TASK_PREFIX + name for name in _graph().paths]


def classify(text: str, conversation_id: str = "airflow", user_id: str = "airflow") -> str:
    """The router as a pure function, used both by the DAG's branch task and by
    ``simulate``. Returns the task id to branch to."""
    ctx = AgentContext(
        conversation_id=conversation_id,
        user_id=user_id,
        store=_store(),
        state=InMemoryKeyedStateStore(),
        tools=_tools(),
        retriever=None,
    )
    event = Event(conversation_id=conversation_id, text=text, user_id=user_id)
    return PATH_TASK_PREFIX + _graph().router(event, ctx)


def run_path(text: str, conversation_id: str, user_id: str = "airflow") -> Dict[str, object]:
    """Run the routed graph for one turn (the branch task has already picked the path;
    the graph routes the same way, so the selected path task is the one that runs)."""
    ctx = AgentContext(
        conversation_id=conversation_id,
        user_id=user_id,
        store=_store(),
        state=InMemoryKeyedStateStore(),
        tools=_tools(),
        retriever=_retriever(),
    )
    res = _graph().handle(Event(conversation_id=conversation_id, text=text, user_id=user_id), ctx)
    return {"path": res.path, "reply": res.reply, "ok": res.ok, "tool_calls": list(res.tool_calls)}


def simulate(text: str, conversation_id: str = "sim") -> Dict[str, object]:
    """Model-free end-to-end of the DAG's logic without a scheduler (for tests)."""
    branch = classify(text, conversation_id)
    result = run_path(text, conversation_id)
    if PATH_TASK_PREFIX + str(result["path"]) != branch:
        raise RuntimeError(f"branch {branch!r} disagrees with routed path {result['path']!r}")
    return result


# ---- RAG ingestion steps (load -> embed -> index), the bodies of the ingestion DAG ----

def load_corpus() -> Dict[str, str]:
    return dict(KB)


def embed_corpus(docs: Dict[str, str]) -> Dict[str, list]:
    return {doc_id: list(_EMBED(text)) for doc_id, text in docs.items()}


def build_index(vectors: Dict[str, list], docs: Dict[str, str]) -> int:
    index = InMemoryHotVectorIndex(max_entries=10_000)
    for doc_id, vec in vectors.items():
        index.upsert(doc_id, vec, docs[doc_id])
    # production: persist to pgvector/Qdrant/Fluss here.
    return index.size()


def _demo() -> None:
    for text in ["what card types do you offer?", "dispute a charge", "hello"]:
        print(text, "->", simulate(text))
    docs = load_corpus()
    print(f"ingestion: indexed {build_index(embed_corpus(docs), docs)} docs")
