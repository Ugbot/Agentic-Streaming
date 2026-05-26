"""Inference wrappers — classifier / scorer / guardrail.

Covers the Java SPI plus convenience factories for the DJL backend.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from ._jvm import jclass


@dataclass(frozen=True)
class InferenceSetup:
    """Maps onto ``org.agentic.flink.inference.InferenceSetup``."""

    model: str
    model_uri: str
    device: str = "auto"
    threads: int = 1
    max_batch_size: int = 1
    warmup_inputs: tuple[str, ...] = ()

    def _to_java(self):
        IS = jclass("org.agentic.flink.inference.InferenceSetup")
        DeviceType = jclass(
            "org.agentic.flink.inference.InferenceSetup$DeviceType"
        )
        b = (
            IS.builder()
            .withModelName(self.model)
            .withModelUri(self.model_uri)
            .withDevice(DeviceType.valueOf(self.device.upper()))
            .withThreads(self.threads)
            .withMaxBatchSize(self.max_batch_size)
        )
        if self.warmup_inputs:
            ArrayList = jclass("java.util.ArrayList")
            warm = ArrayList()
            for s in self.warmup_inputs:
                warm.add(s)
            b = b.withWarmupInputs(warm)
        return b.build()


def djl_classification(model_uri: str):
    """Build a DJL classification connection."""
    Conn = jclass("org.agentic.flink.inference.djl.DjlInferenceConnection")
    return Conn.classification(model_uri)


def djl_embedding(model_uri: str):
    """Build a DJL text-embedding connection."""
    Conn = jclass("org.agentic.flink.inference.djl.DjlInferenceConnection")
    return Conn.embedding(model_uri)


def classifier_guardrail(
    name: str,
    connection,
    setup: InferenceSetup,
    block_labels: Iterable[str],
    *,
    check_input: bool = True,
    check_output: bool = False,
):
    """Build a :class:`ClassifierGuardrail` with a Python-friendly signature."""
    Guard = jclass("org.agentic.flink.inference.ClassifierGuardrail")
    HashSet = jclass("java.util.HashSet")
    labels = HashSet()
    for lbl in block_labels:
        labels.add(lbl)
    java_conn = connection._to_java() if hasattr(connection, "_to_java") else connection
    return Guard(name, java_conn, setup._to_java(), labels, check_input, check_output)


def inference_tool(
    tool_id: str,
    description: str,
    connection,
    setup: InferenceSetup,
    kind: str = "classifier",
):
    """Wrap an inference model as a ``ToolExecutor`` so the LLM can call it."""
    Adapter = jclass("org.agentic.flink.inference.InferenceToolAdapter")
    TaskKind = jclass(
        "org.agentic.flink.inference.InferenceToolAdapter$TaskKind"
    )
    kind_norm = {"classifier": "CLASSIFIER", "scorer": "SCORER"}[kind.lower()]
    java_conn = connection._to_java() if hasattr(connection, "_to_java") else connection
    return Adapter(tool_id, description, java_conn, setup._to_java(), TaskKind.valueOf(kind_norm))


__all__ = [
    "InferenceSetup",
    "djl_classification",
    "djl_embedding",
    "classifier_guardrail",
    "inference_tool",
]
