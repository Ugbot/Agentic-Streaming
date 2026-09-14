"""Sources of JSON turn documents and sinks of normalized JSON results.

Connectors are plain dataclasses describing *where* turns come from and results go; the
runtime turns them into Flink sources/sinks when the job graph is built. The wire form on both
sides is one JSON document per record (see ``PyFlinkJob`` in ``pyflink/java``).

Availability is checked up front: a connector whose Java classes are not on the classpath raises
:class:`ConnectorUnavailableError` naming the jar to add, instead of failing deep inside Py4J.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyflink.common import Duration, Encoder, Types, WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    BucketAssigner,
    RollingPolicy,
    StreamFormat,
)
from pyflink.datastream.connectors.file_system import (
    FileSink as _FlinkFileSink,
)
from pyflink.datastream.connectors.file_system import (
    FileSource as _FlinkFileSource,
)
from pyflink.java_gateway import get_gateway

from .config import duration_ms

KAFKA_SOURCE_CLASS = "org.apache.flink.connector.kafka.source.KafkaSource"
KAFKA_JAR_HINT = (
    "the Kafka connector is not on the classpath; add flink-sql-connector-kafka "
    "(the 4.x line built for Flink 2.x) via FlinkConfig(extra_jars=[...])"
)


class ConnectorUnavailableError(RuntimeError):
    """The connector's Java classes are missing from the job classpath."""


def _java_class_present(name: str) -> bool:
    jvm = get_gateway().jvm
    loader = jvm.Thread.currentThread().getContextClassLoader()
    try:
        jvm.Class.forName(name, False, loader)
        return True
    except Exception:  # py4j raises Py4JJavaError for ClassNotFoundException
        return False


def turn_document(turn: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Normalise a user-supplied turn (a mapping or a dataclass such as ``agentic.events.Turn``)
    into the wire form; validates the required keys."""
    if not isinstance(turn, Mapping):
        if dataclasses.is_dataclass(turn) and not isinstance(turn, type):
            turn = dataclasses.asdict(turn)
        else:
            raise ValueError(f"a turn must be a mapping or a dataclass, got {type(turn).__name__}")
    cid = turn.get("conversation_id")
    turn_id = turn.get("turn_id")
    if not cid or not turn_id:
        raise ValueError("a turn needs non-empty 'conversation_id' and 'turn_id'")
    doc: dict[str, Any] = {"conversation_id": str(cid), "turn_id": str(turn_id)}
    if turn.get("signal") is not None:
        signal = turn["signal"]
        if not isinstance(signal, Mapping):
            raise ValueError("'signal' must be a mapping")
        doc["signal"] = dict(signal)
        return doc
    doc["user_id"] = str(turn.get("user_id") or "anonymous")
    doc["text"] = str(turn.get("text", ""))
    metadata = turn.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        raise ValueError("'metadata' must be a mapping")
    doc["metadata"] = {str(k): str(v) for k, v in metadata.items()}
    return doc


def encode_turn(turn: Mapping[str, Any]) -> str:
    return json.dumps(turn_document(turn), separators=(",", ":"), sort_keys=True)


# --------------------------------------------------------------------------------------- sources


class Source:
    """A source of JSON turn documents. ``bounded`` sources end the job when drained."""

    bounded: bool = False

    def check_available(self) -> None:
        """Raise :class:`ConnectorUnavailableError` if the connector cannot be built."""

    def attach(self, env: StreamExecutionEnvironment) -> DataStream:
        raise NotImplementedError


@dataclass
class CollectionSource(Source):
    """A fixed list of turns, in order. Bounded: the job finishes after the last turn."""

    turns: list[Mapping[str, Any]] = field(default_factory=list)
    bounded: bool = field(default=True, init=False)

    def attach(self, env: StreamExecutionEnvironment) -> DataStream:
        if not self.turns:
            raise ValueError("CollectionSource needs at least one turn")
        return env.from_collection([encode_turn(t) for t in self.turns], Types.STRING())


@dataclass
class FileSource(Source):
    """Continuously monitors a directory for JSON-lines files of turns (one document per line).

    Files are read in discovery order; the lines of one file are read in order, so a batch of
    turns that must be applied in sequence belongs in a single file. Processed files are tracked
    in the source's checkpointed state, so a job restored from a savepoint does not replay them.
    """

    directory: str
    poll_interval: int | str = "100ms"

    def attach(self, env: StreamExecutionEnvironment) -> DataStream:
        Path(self.directory).mkdir(parents=True, exist_ok=True)
        source = (
            _FlinkFileSource.for_record_stream_format(StreamFormat.text_line_format(), self.directory)
            .monitor_continuously(Duration.of_millis(duration_ms(self.poll_interval)))
            .build()
        )
        return env.from_source(source, WatermarkStrategy.no_watermarks(), "turns")


@dataclass
class KafkaSource(Source):
    """Turns from a Kafka topic (one JSON document per record value)."""

    bootstrap_servers: str
    topic: str
    group_id: str = "agentic-pyflink"
    starting_offsets: str = "earliest"
    properties: dict[str, str] = field(default_factory=dict)

    def check_available(self) -> None:
        if not _java_class_present(KAFKA_SOURCE_CLASS):
            raise ConnectorUnavailableError(KAFKA_JAR_HINT)

    def attach(self, env: StreamExecutionEnvironment) -> DataStream:
        self.check_available()
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
        from pyflink.datastream.connectors.kafka import KafkaSource as _KafkaSource

        offsets = {
            "earliest": KafkaOffsetsInitializer.earliest,
            "latest": KafkaOffsetsInitializer.latest,
            "committed": KafkaOffsetsInitializer.committed_offsets,
        }
        if self.starting_offsets not in offsets:
            raise ValueError(f"starting_offsets must be one of {sorted(offsets)}, got {self.starting_offsets!r}")
        builder = (
            _KafkaSource.builder()
            .set_bootstrap_servers(self.bootstrap_servers)
            .set_topics(self.topic)
            .set_group_id(self.group_id)
            .set_starting_offsets(offsets[self.starting_offsets]())
            .set_value_only_deserializer(SimpleStringSchema())
        )
        for key, value in self.properties.items():
            builder = builder.set_property(key, value)
        return env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), "turns")


# ----------------------------------------------------------------------------------------- sinks


class Sink:
    """A destination for normalized result documents."""

    def check_available(self) -> None:
        """Raise :class:`ConnectorUnavailableError` if the connector cannot be built."""

    def attach(self, results: DataStream) -> None:
        raise NotImplementedError


@dataclass
class CollectSink(Sink):
    """No Flink sink: results are pulled back into Python with ``execute_and_collect``.

    Only meaningful with a bounded source; the runtime rejects it otherwise.
    """

    def attach(self, results: DataStream) -> None:
        raise RuntimeError("CollectSink is consumed by the runtime, not attached to the graph")


@dataclass
class FileSink(Sink):
    """Normalized results as JSON lines under ``directory``.

    Part files roll on every checkpoint, so a result becomes visible to readers once the
    checkpoint that contains it completes -- the checkpoint interval is the visibility latency.
    """

    directory: str

    def attach(self, results: DataStream) -> None:
        Path(self.directory).mkdir(parents=True, exist_ok=True)
        sink = (
            _FlinkFileSink.for_row_format(self.directory, Encoder.simple_string_encoder())
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
            .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy())
            .build()
        )
        results.sink_to(sink).name("results")


@dataclass
class KafkaSink(Sink):
    """Normalized results to a Kafka topic (JSON document per record value, at-least-once)."""

    bootstrap_servers: str
    topic: str
    properties: dict[str, str] = field(default_factory=dict)

    def check_available(self) -> None:
        if not _java_class_present(KAFKA_SOURCE_CLASS):
            raise ConnectorUnavailableError(KAFKA_JAR_HINT)

    def attach(self, results: DataStream) -> None:
        self.check_available()
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors.base import DeliveryGuarantee
        from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
        from pyflink.datastream.connectors.kafka import KafkaSink as _KafkaSink

        builder = (
            _KafkaSink.builder()
            .set_bootstrap_servers(self.bootstrap_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self.topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        )
        for key, value in self.properties.items():
            builder = builder.set_property(key, value)
        results.sink_to(builder.build()).name("results")


def committed_part_files(directory: str) -> list[Path]:
    """Committed :class:`FileSink` part files, in commit order per subtask.

    In-progress and pending files are dot-prefixed and skipped; committed files are named
    ``part-<subtask uid>-<counter>`` with a per-subtask counter, and are immutable once renamed.
    """
    return sorted(
        (p for p in Path(directory).rglob("part-*") if p.is_file() and not p.name.startswith(".")),
        key=_part_order,
    )


def _part_order(path: Path):
    stem = path.name[len("part-"):]
    uid, _, counter = stem.rpartition("-")
    return (uid, int(counter) if counter.isdigit() else -1, path.name)


def read_results(directory: str) -> list[dict[str, Any]]:
    """All committed results under a :class:`FileSink` directory."""
    results: list[dict[str, Any]] = []
    for path in committed_part_files(directory):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                results.append(json.loads(line))
    return results
