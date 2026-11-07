"""tools/container-engine.sh picks whichever engine is actually running, Docker or Podman."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "tools" / "container-engine.sh"


def _stub(bin_dir: Path, name: str, running: bool) -> None:
    """A fake engine: `info` succeeds only if `running`; anything else logs its arguments."""
    path = bin_dir / name
    path.write_text(
        "#!/bin/sh\n"
        f'if [ "$1" = info ]; then exit {0 if running else 1}; fi\n'
        f'echo "{name} $*" >> "$CALLS"\n'
        'if [ "$1 $2" = "network inspect" ]; then exit 1; fi\n'
    )
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _run(tmp_path: Path, script: str, env: dict | None = None) -> subprocess.CompletedProcess:
    calls = tmp_path / "calls"
    calls.touch()
    full_env = {
        "PATH": f"{tmp_path}:/usr/bin:/bin",
        "CALLS": str(calls),
        **(env or {}),
    }
    return subprocess.run(
        ["bash", "-c", f'source "{HELPER}"; use_container_engine; {script}'],
        env=full_env, capture_output=True, text=True,
    )


def test_docker_is_used_when_it_is_running(tmp_path):
    _stub(tmp_path, "docker", running=True)
    _stub(tmp_path, "podman", running=True)
    assert _run(tmp_path, 'echo "$ENGINE"').stdout.strip() == "docker"


def test_podman_is_used_when_docker_is_stopped(tmp_path):
    _stub(tmp_path, "docker", running=False)
    _stub(tmp_path, "podman", running=True)
    assert _run(tmp_path, 'echo "$ENGINE"').stdout.strip() == "podman"


def test_docker_is_used_when_the_podman_machine_is_down(tmp_path):
    _stub(tmp_path, "docker", running=True)
    _stub(tmp_path, "podman", running=False)
    assert _run(tmp_path, 'echo "$ENGINE"').stdout.strip() == "docker"


def test_an_explicit_choice_wins(tmp_path):
    _stub(tmp_path, "docker", running=True)
    _stub(tmp_path, "podman", running=True)
    result = _run(tmp_path, 'echo "$ENGINE"', {"CONTAINER_ENGINE": "podman"})
    assert result.stdout.strip() == "podman"


def test_an_explicit_engine_that_is_down_fails_rather_than_substituting(tmp_path):
    _stub(tmp_path, "docker", running=True)
    _stub(tmp_path, "podman", running=False)
    result = _run(tmp_path, 'echo reached', {"CONTAINER_ENGINE": "podman"})
    assert result.returncode != 0
    assert "reached" not in result.stdout
    assert "podman" in result.stderr


def test_no_engine_says_so(tmp_path):
    _stub(tmp_path, "docker", running=False)
    result = _run(tmp_path, 'echo reached')
    assert result.returncode != 0
    assert "no running container engine" in result.stderr


def test_compose_and_network_go_through_the_chosen_engine(tmp_path):
    _stub(tmp_path, "docker", running=False)
    _stub(tmp_path, "podman", running=True)
    assert _run(tmp_path, "compose -f x.yml up -d; ensure_network n").returncode == 0
    calls = (tmp_path / "calls").read_text().splitlines()
    assert calls == ["podman compose -f x.yml up -d", "podman network inspect n", "podman network create n"]


@pytest.mark.parametrize("script", sorted(p.name for p in (ROOT / "tools").glob("*.sh")))
def test_no_script_names_one_engine(script):
    if script == HELPER.name:
        return
    for number, line in enumerate((ROOT / "tools" / script).read_text().splitlines(), 1):
        code = line.split("#", 1)[0]
        assert not any(f"{e} " in code for e in ("podman", "docker compose", "docker run", "docker exec")), \
            f"{script}:{number} calls an engine directly; use tools/container-engine.sh"
