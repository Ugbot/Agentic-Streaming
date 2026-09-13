"""agentic-pyflink: run portable agent workflow documents on Apache Flink from Python.

The workflow document is the shared v1 IR (``spec/v1/workflow.schema.json``); the operator that
executes it is the Java Flink adapter of the canonical core. This package authors the Flink job
around it from Python (PyFlink): configuration, connectors, local MiniCluster vs. cluster
submission, and a conformance binding for ``spec/conformance/v1``.
"""

from .capabilities import CAPABILITIES, PROOF, CapabilityError
from .config import FlinkConfig, duration_ms
from .connectors import (
    CollectionSource,
    CollectSink,
    ConnectorUnavailableError,
    FileSink,
    FileSource,
    KafkaSink,
    KafkaSource,
    Sink,
    Source,
)
from .jars import JarNotFoundError
from .runtime import FlinkRuntime, ResultTimeoutError, RuntimeStateError
from .workflow import WorkflowLoadError, load_workflow, required_capabilities

__all__ = [
    "CAPABILITIES",
    "PROOF",
    "CapabilityError",
    "CollectSink",
    "CollectionSource",
    "ConnectorUnavailableError",
    "FileSink",
    "FileSource",
    "FlinkConfig",
    "FlinkRuntime",
    "JarNotFoundError",
    "KafkaSink",
    "KafkaSource",
    "ResultTimeoutError",
    "RuntimeStateError",
    "Sink",
    "Source",
    "WorkflowLoadError",
    "duration_ms",
    "load_workflow",
    "required_capabilities",
]
