"""Clean-install smoke check for the agentic-flink wheel.

Run from a directory outside the repository, in a fresh virtualenv that has only the built
wheel installed (no AGENTIC_FLINK_JAR, no source checkout on sys.path):

    AGENTIC_SPEC_ROOT=/path/to/checkout/spec \
        python smoke_clean_install.py /path/to/checkout/spec/conformance/v1/fixtures/01-routing-keyword.yaml

``AGENTIC_SPEC_ROOT`` points at the spec tree (fixtures, workflows and the shared comparator
are data, not part of the wheel). The script asserts that the framework jar was found inside the installed package, starts the JVM and
runs the given fixture through the ``local-jvm`` runtime, exiting non-zero on any failure.
"""

from __future__ import annotations

import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    fixture = Path(argv[1]).resolve()
    if not fixture.is_file():
        print(f"fixture not found: {fixture}", file=sys.stderr)
        return 2

    import agentic_flink
    from agentic_flink._classpath import framework_jar
    from agentic_flink._contract import get_runtime
    from agentic_flink.conformance import run_fixture

    package_dir = Path(agentic_flink.__file__).resolve().parent
    jar = framework_jar()
    if package_dir not in jar.parents:
        print(f"framework jar {jar} is not the one bundled under {package_dir}", file=sys.stderr)
        return 1
    print(f"framework jar: {jar}")

    agentic_flink.start_jvm()
    if not agentic_flink.is_started():
        print("JVM did not start", file=sys.stderr)
        return 1
    print("JVM started")

    runtime = get_runtime("local-jvm")
    try:
        outcome = run_fixture(fixture, runtime)
    finally:
        runtime.close()
    print(f"{outcome.fixture_id}: {outcome.status} {outcome.reason or ''}".rstrip())
    return 0 if outcome.status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
