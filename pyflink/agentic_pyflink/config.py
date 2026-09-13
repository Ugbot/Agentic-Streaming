"""Flink runtime configuration authored from Python.

Everything here maps one-to-one onto Flink configuration keys or ``StreamExecutionEnvironment``
calls; nothing is interpreted by the workflow operator, which reads only the portable document's
``runtime.flink`` block (state TTL, timer resume delay, timer domain) exactly like the JVM runner.

Two execution modes are distinguished explicitly:

``local``
    The job runs on a Flink MiniCluster inside the Py4J gateway JVM that PyFlink starts.
    No cluster, no ``flink run``; this is what CI exercises.

``cluster``
    The job graph is submitted over REST to an existing Flink cluster (``execution.target=remote``)
    whose TaskManagers must reach the same jars. See ``docs/python.md`` for the prerequisites;
    this mode is not exercised by the test suite.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

Mode = Literal["local", "cluster"]
StateBackend = Literal["hashmap", "rocksdb", "forst"]

STATE_BACKENDS = ("hashmap", "rocksdb", "forst")
MODES = ("local", "cluster")

_DURATION = re.compile(r"^\s*(\d+)\s*(ms|s|m|h)?\s*$")
_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def duration_ms(value: int | str) -> int:
    """``500`` / ``"500ms"`` / ``"30s"`` / ``"2m"`` / ``"1h"`` -> milliseconds."""
    if isinstance(value, bool):
        raise ValueError("duration must be an int (ms) or a string like '30s'")
    if isinstance(value, int):
        if value <= 0:
            raise ValueError(f"duration must be positive, got {value}")
        return value
    m = _DURATION.match(str(value))
    if not m:
        raise ValueError(f"unrecognised duration {value!r}; use e.g. 500, '500ms', '30s', '2m'")
    number, unit = int(m.group(1)), m.group(2) or "ms"
    if number <= 0:
        raise ValueError(f"duration must be positive, got {value!r}")
    return number * _UNIT_MS[unit]


@dataclass(frozen=True)
class FlinkConfig:
    """Runtime knobs for one deployment. Immutable; use :func:`dataclasses.replace` to derive."""

    mode: Mode = "local"
    parallelism: int = 1
    checkpoint_interval: int | str = "500ms"
    state_backend: StateBackend = "hashmap"
    incremental_checkpoints: bool = False
    checkpoint_dir: str | None = None
    savepoint_dir: str | None = None
    rest_address: str | None = None
    rest_port: int = 8081
    extra_jars: list[str] = field(default_factory=list)
    flink_options: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {self.mode!r}")
        if self.parallelism < 1:
            raise ValueError(f"parallelism must be >= 1, got {self.parallelism}")
        if self.state_backend not in STATE_BACKENDS:
            raise ValueError(f"state_backend must be one of {STATE_BACKENDS}, got {self.state_backend!r}")
        duration_ms(self.checkpoint_interval)
        if self.mode == "cluster" and not self.rest_address:
            raise ValueError("cluster mode needs rest_address (the JobManager REST host)")
        if self.incremental_checkpoints and self.state_backend == "hashmap":
            raise ValueError("incremental checkpoints need the rocksdb or forst state backend")

    @property
    def checkpoint_interval_ms(self) -> int:
        return duration_ms(self.checkpoint_interval)

    def flink_configuration(self) -> dict[str, str]:
        """The flat ``key -> value`` map handed to ``Configuration`` before the environment starts."""
        conf: dict[str, str] = {
            "parallelism.default": str(self.parallelism),
            "state.backend.type": self.state_backend,
            "execution.checkpointing.interval": f"{self.checkpoint_interval_ms} ms",
        }
        if self.state_backend != "hashmap":
            conf["execution.checkpointing.incremental"] = str(self.incremental_checkpoints).lower()
        if self.checkpoint_dir:
            conf["execution.checkpointing.dir"] = _file_uri(self.checkpoint_dir)
        if self.savepoint_dir:
            conf["execution.checkpointing.savepoint-dir"] = _file_uri(self.savepoint_dir)
        if self.mode == "cluster":
            conf["execution.target"] = "remote"
            conf["rest.address"] = str(self.rest_address)
            conf["rest.port"] = str(self.rest_port)
        conf.update(self.flink_options)
        return conf


def _file_uri(path: str) -> str:
    return path if "://" in path else Path(path).expanduser().resolve().as_uri()
