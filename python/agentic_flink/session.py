"""Python control surface for the Flink session cluster.

Wraps Flink's REST API (jar upload, job submit/cancel, plan inspection) plus a small ZeroMQ
helper for pushing :class:`DebugControl` messages into the control plane that every
``AgenticKeyedProcessFunction`` listens on.

Deps: ``requests`` (HTTP client) and ``pyzmq`` (ZMQ publisher). Both pure-Python; install via
``pip install requests pyzmq``.
"""

from __future__ import annotations

import json
import pathlib
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

try:
    import requests
except ImportError as exc:  # pragma: no cover - clearer error than NameError
    raise ImportError(
        "agentic_flink.session requires the `requests` package. "
        "Install it with: pip install requests"
    ) from exc


_DEFAULT_ENTRY_CLASS = "org.agentic.flink.session.SessionJobLauncher"


@dataclass
class JobHandle:
    """Reference to one submitted job. Bound to a SessionClient for follow-up calls."""

    job_id: str
    job_name: str
    level: str
    client: "SessionClient" = field(repr=False)

    def status(self) -> dict[str, Any]:
        return self.client.job_status(self.job_id)

    def cancel(self) -> None:
        self.client.cancel(self.job_id)

    def plan(self) -> dict[str, Any]:
        return self.client.job_plan(self.job_id)


class SessionClient:
    """Thin wrapper around the Flink REST API.

    Parameters
    ----------
    base_url:
        Flink JobManager REST base URL, e.g. ``http://localhost:8081``.
    timeout:
        Per-request HTTP timeout in seconds.
    """

    def __init__(self, base_url: str = "http://localhost:8081", *, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ---- jar management ----

    def upload_jar(self, jar_path: str | pathlib.Path) -> str:
        """Upload a jar and return the jar id (Flink's ``filename`` field minus the path prefix)."""
        path = pathlib.Path(jar_path)
        if not path.exists():
            raise FileNotFoundError(f"jar not found: {path}")
        with path.open("rb") as fh:
            r = requests.post(
                f"{self.base_url}/jars/upload",
                files={"jarfile": (path.name, fh, "application/x-java-archive")},
                timeout=self.timeout,
            )
        r.raise_for_status()
        payload = r.json()
        # Flink returns {'filename': '/tmp/.../jar-id_filename.jar', 'status': 'success'} —
        # the jar id is the basename, everything before the underscore.
        filename = payload["filename"].split("/")[-1]
        return filename

    def list_jars(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/jars", timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("files", [])

    def latest_jar(self, name_hint: str = "agentic-flink") -> Optional[str]:
        """Return the most recently uploaded jar id whose name contains ``name_hint``."""
        jars = self.list_jars()
        candidates = [j for j in jars if name_hint in j.get("name", "")]
        if not candidates:
            return None
        candidates.sort(key=lambda j: j.get("uploaded", 0), reverse=True)
        return candidates[0]["id"]

    # ---- job lifecycle ----

    def run(
        self,
        jar_id: str,
        program_args: Iterable[str],
        *,
        entry_class: str = _DEFAULT_ENTRY_CLASS,
        parallelism: Optional[int] = None,
    ) -> str:
        """Submit a job; returns the jobid Flink assigned."""
        body: dict[str, Any] = {
            "entryClass": entry_class,
            "programArgsList": list(program_args),
        }
        if parallelism is not None:
            body["parallelism"] = parallelism
        r = requests.post(
            f"{self.base_url}/jars/{jar_id}/run",
            json=body,
            timeout=self.timeout,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"submit failed ({r.status_code}): {r.text}")
        return r.json()["jobid"]

    def submit_level(
        self,
        jar_id: str,
        level: str,
        *,
        in_endpoint: Optional[str] = None,
        out_endpoint: Optional[str] = None,
        control_endpoint: Optional[str] = None,
        debug_sink_endpoint: Optional[str] = None,
        alerts_pub_endpoint: Optional[str] = None,
        anthropic_key: Optional[str] = None,
        products: Optional[Iterable[str]] = None,
        window_ms: Optional[int] = None,
        top_n: Optional[int] = None,
        name: Optional[str] = None,
        extra_args: Optional[dict[str, str]] = None,
    ) -> JobHandle:
        """Submit one ``SessionJobLauncher`` level and return a handle."""
        args: list[str] = ["--level", level]
        if in_endpoint:
            args += ["--in", in_endpoint]
        if out_endpoint:
            args += ["--out", out_endpoint]
        if control_endpoint:
            args += ["--control", control_endpoint]
        if debug_sink_endpoint:
            args += ["--debug-sink", debug_sink_endpoint]
        if alerts_pub_endpoint:
            args += ["--alerts-pub", alerts_pub_endpoint]
        if anthropic_key:
            args += ["--anthropic-key", anthropic_key]
        if products:
            args += ["--products", ",".join(products)]
        if window_ms is not None:
            args += ["--window-ms", str(window_ms)]
        if top_n is not None:
            args += ["--top-n", str(top_n)]
        if name:
            args += ["--name", name]
        if extra_args:
            for k, v in extra_args.items():
                args += [f"--{k}", str(v)]
        job_id = self.run(jar_id, args)
        return JobHandle(
            job_id=job_id, job_name=name or f"agentic-{level}", level=level, client=self
        )

    def job_status(self, job_id: str) -> dict[str, Any]:
        r = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def job_plan(self, job_id: str) -> dict[str, Any]:
        r = requests.get(f"{self.base_url}/jobs/{job_id}/plan", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def cancel(self, job_id: str) -> None:
        r = requests.patch(f"{self.base_url}/jobs/{job_id}", timeout=self.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"cancel failed ({r.status_code}): {r.text}")

    def jobs(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/jobs/overview", timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("jobs", [])

    def wait_ready(self, *, timeout_s: float = 60.0, interval_s: float = 1.0) -> dict[str, Any]:
        """Poll ``/overview`` until the cluster responds, or raise ``TimeoutError``."""
        deadline = time.time() + timeout_s
        last_err: Optional[Exception] = None
        while time.time() < deadline:
            try:
                r = requests.get(f"{self.base_url}/overview", timeout=self.timeout)
                r.raise_for_status()
                return r.json()
            except Exception as exc:  # noqa: BLE001 — we want all transient HTTP/network errors
                last_err = exc
                time.sleep(interval_s)
        raise TimeoutError(f"Flink REST not ready at {self.base_url}: {last_err}")


class DebugFlipper:
    """Pushes :class:`DebugControl` JSON envelopes onto the control-plane ZMQ endpoint.

    The L5 job binds a {@code PULL} socket on the control endpoint; this side connects with
    {@code PUSH} so the messages are delivered point-to-point.

    Example:

        flipper = DebugFlipper("tcp://localhost:5559")
        flipper.on("L5.market-agent")           # 2-minute default TTL
        flipper.pinned("L5.market-agent")       # permanent until off
        flipper.off("L5.market-agent")          # cancel
        flipper.everywhere(ttl_seconds=30)      # broadcast for 30s
    """

    def __init__(self, endpoint: str):
        try:
            import zmq
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "DebugFlipper requires the `pyzmq` package. Install it with: pip install pyzmq"
            ) from exc
        self._zmq = zmq
        self._ctx = zmq.Context.instance()
        self._sock = self._ctx.socket(zmq.PUSH)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.connect(endpoint)
        self._endpoint = endpoint

    @staticmethod
    def _envelope(operator_id: str, enabled: bool, ttl_millis: int) -> bytes:
        return json.dumps(
            {
                "type": "debug",
                "operatorId": operator_id,
                "enabled": enabled,
                "ttlMillis": ttl_millis,
            }
        ).encode("utf-8")

    def send(self, operator_id: str, enabled: bool, ttl_millis: int) -> None:
        self._sock.send(self._envelope(operator_id, enabled, ttl_millis))

    def on(self, operator_id: str, *, ttl_seconds: int = 120) -> None:
        self.send(operator_id, True, ttl_seconds * 1000)

    def pinned(self, operator_id: str) -> None:
        self.send(operator_id, True, -1)

    def off(self, operator_id: str) -> None:
        self.send(operator_id, False, 0)

    def everywhere(self, *, ttl_seconds: int = 120) -> None:
        self.send("*", True, ttl_seconds * 1000)

    def silence_all(self) -> None:
        self.send("*", False, 0)

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:  # noqa: BLE001
            pass


def operator_ids(plan: dict[str, Any]) -> list[str]:
    """Extract stable operator ids from a Flink job plan."""
    return [n.get("description") or n.get("id") for n in plan.get("nodes", [])]


__all__ = ["SessionClient", "JobHandle", "DebugFlipper", "operator_ids"]
