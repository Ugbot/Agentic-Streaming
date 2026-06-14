"""A hand-rolled HNSW (Hierarchical Navigable Small World) index — a real approximate
nearest-neighbour graph that runs **in-process**, so the cores don't need an external
vector database. Implements Malkov & Yashunin (2016): a multi-layer navigable small-world
graph with greedy descent through the upper layers and an ``ef``-bounded best-first search
at layer 0.

Distances are ``1 - cosine`` (so smaller = nearer); scores returned to callers are the
cosine similarity. The level-assignment RNG is **seeded** so recall is reproducible in
tests. Mirrored by the Go and Java cores (same parameters and structure) and analogous to
the Flink in-JVM ``FlinkStateHnswVectorMemory``.
"""

from __future__ import annotations

import heapq
import math
import random
from typing import Dict, List, Optional, Tuple

from .retrieval import Scored, cosine


class HnswIndex:
    """In-memory HNSW graph. ``m`` neighbours per node per layer (``2*m`` at layer 0),
    ``ef_construction`` candidate width while inserting, ``ef_search`` while querying."""

    def __init__(self, m: int = 16, ef_construction: int = 200, ef_search: int = 50, seed: int = 42) -> None:
        if m < 2:
            raise ValueError("m must be >= 2")
        self.m = m
        self.m0 = 2 * m
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self._ml = 1.0 / math.log(m)
        self._rng = random.Random(seed)
        # node id -> (vector, text)
        self._vecs: Dict[str, List[float]] = {}
        self._text: Dict[str, str] = {}
        self._level: Dict[str, int] = {}
        # per-layer adjacency: _graph[layer][node_id] -> list of neighbour ids
        self._graph: List[Dict[str, List[str]]] = []
        self._entry: Optional[str] = None
        self._top = -1

    # ----- internals -------------------------------------------------------
    def _distance(self, a: List[float], node_id: str) -> float:
        return 1.0 - cosine(a, self._vecs[node_id])

    def _neighbors(self, node_id: str, layer: int) -> List[str]:
        if layer >= len(self._graph):
            return []
        return self._graph[layer].get(node_id, [])

    def _ensure_layers(self, level: int) -> None:
        while len(self._graph) <= level:
            self._graph.append({})

    def _random_level(self) -> int:
        # geometric distribution; rng in (0,1]
        r = self._rng.random()
        while r <= 0.0:
            r = self._rng.random()
        return int(-math.log(r) * self._ml)

    def _search_layer(self, query: List[float], entry_points: List[str], ef: int, layer: int) -> List[Tuple[float, str]]:
        """Best-first search within one layer; returns up to ``ef`` (distance, id) pairs."""
        visited = set(entry_points)
        # candidate min-heap by distance; results max-heap via negated distance
        candidates: List[Tuple[float, str]] = []
        results: List[Tuple[float, str]] = []
        for ep in entry_points:
            d = self._distance(query, ep)
            heapq.heappush(candidates, (d, ep))
            heapq.heappush(results, (-d, ep))
        while candidates:
            dist_c, c = heapq.heappop(candidates)
            worst = -results[0][0]
            if dist_c > worst and len(results) >= ef:
                break
            for e in self._neighbors(c, layer):
                if e in visited:
                    continue
                visited.add(e)
                d = self._distance(query, e)
                worst = -results[0][0]
                if len(results) < ef or d < worst:
                    heapq.heappush(candidates, (d, e))
                    heapq.heappush(results, (-d, e))
                    if len(results) > ef:
                        heapq.heappop(results)
        return sorted((( -nd), nid) for nd, nid in results)

    def _select_neighbors(self, candidates: List[Tuple[float, str]], m: int) -> List[str]:
        """Simple heuristic: keep the ``m`` closest candidates."""
        return [nid for _, nid in sorted(candidates)[:m]]

    def _prune(self, node_id: str, layer: int) -> None:
        m_max = self.m0 if layer == 0 else self.m
        neigh = self._graph[layer].get(node_id, [])
        if len(neigh) <= m_max:
            return
        scored = [(self._distance(self._vecs[node_id], n), n) for n in neigh]
        self._graph[layer][node_id] = [n for _, n in sorted(scored)[:m_max]]

    # ----- public API ------------------------------------------------------
    def add(self, node_id: str, vector: List[float], text: str = "") -> None:
        update = node_id in self._vecs
        self._vecs[node_id] = list(vector)
        self._text[node_id] = text
        if update:
            return  # vector refreshed; existing links kept (approximate)

        level = self._random_level()
        self._level[node_id] = level
        self._ensure_layers(level)
        for lc in range(level + 1):
            self._graph[lc].setdefault(node_id, [])

        if self._entry is None:
            self._entry = node_id
            self._top = level
            return

        cur = self._entry
        # greedy descent through layers above the new node's level
        for lc in range(self._top, level, -1):
            cur = self._greedy(self._vecs[node_id], cur, lc)

        for lc in range(min(level, self._top), -1, -1):
            found = self._search_layer(self._vecs[node_id], [cur], self.ef_construction, lc)
            m = self.m0 if lc == 0 else self.m
            selected = self._select_neighbors(found, m)
            for nb in selected:
                self._graph[lc].setdefault(node_id, [])
                self._graph[lc].setdefault(nb, [])
                if nb not in self._graph[lc][node_id]:
                    self._graph[lc][node_id].append(nb)
                if node_id not in self._graph[lc][nb]:
                    self._graph[lc][nb].append(node_id)
                self._prune(nb, lc)
            if found:
                cur = found[0][1]

        if level > self._top:
            self._top = level
            self._entry = node_id

    def _greedy(self, query: List[float], entry: str, layer: int) -> str:
        cur = entry
        cur_d = self._distance(query, cur)
        changed = True
        while changed:
            changed = False
            for e in self._neighbors(cur, layer):
                d = self._distance(query, e)
                if d < cur_d:
                    cur, cur_d, changed = e, d, True
        return cur

    def search(self, query: List[float], k: int) -> List[Scored]:
        if self._entry is None:
            return []
        cur = self._entry
        for lc in range(self._top, 0, -1):
            cur = self._greedy(query, cur, lc)
        ef = max(self.ef_search, k)
        found = self._search_layer(query, [cur], ef, 0)
        out = [Scored(nid, 1.0 - d, self._text.get(nid, "")) for d, nid in found[: max(1, k)]]
        return out

    def __len__(self) -> int:
        return len(self._vecs)
