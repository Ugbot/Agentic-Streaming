#!/usr/bin/env bash
# The smoke test half of tools/release_dry_run.sh. It runs either inside the python:3.12 container
# (WORK=/work, read-only) or, from the publish workflows, in a fresh virtualenv on the runner
# (WORK=<staging dir>). WORK holds dist/*.whl, spec/, pipeline.yaml and smoke_clean_install.py;
# nothing else from the repository is visible and the current directory is a scratch directory,
# so every import resolves to the installed wheels. Every step prints what it checked; the
# script fails on the first check that does not hold.
#
#   JDK=apt    install openjdk-21-jdk-headless with apt (the container path)
#   JDK=none   use the JDK already on PATH if there is one, otherwise stop the agentic-flink check
#              after import and jar discovery
set -euo pipefail

WORK="${WORK:-/work}"
FIXTURE="$WORK/spec/conformance/v1/fixtures/01-routing-keyword.yaml"
cd "$(mktemp -d)"

echo "== python: $(python --version) ($(command -v python))"

if [ "${JDK:-apt}" = "apt" ]; then
    echo "== installing openjdk-21-jdk-headless (JDK=apt)"
    apt-get update -qq >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends openjdk-21-jdk-headless >/dev/null
fi
if command -v java >/dev/null 2>&1; then
    java -version 2>&1 | head -1
else
    echo "== no JDK on PATH (JDK=${JDK:-apt}): agentic-flink stops after import and jar discovery"
fi

echo "== pip install (wheels from $WORK/dist, third party dependencies from PyPI)"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet --find-links "$WORK/dist" "$WORK"/dist/*.whl
python -m pip list --format=freeze | grep -iE '^(pyagentic|agentic-flink|agentic-pyflink|agentic-pipeline)=='

echo "== import every package and read its installed version"
python - <<'EOF'
from importlib.metadata import version
import agentic, pyagentic, agentic_flink, agentic_pyflink, agentic_pipeline

dists = ("pyagentic", "agentic-flink", "agentic-pyflink", "agentic-pipeline")
versions = {d: version(d) for d in dists}
print(versions)
assert len(set(versions.values())) == 1, "the four distributions must carry one version"
assert agentic.__version__ == pyagentic.__version__ == agentic_pipeline.__version__ == versions["pyagentic"]
for pkg in (agentic, pyagentic, agentic_flink, agentic_pyflink, agentic_pipeline):
    import pathlib
    marker = pathlib.Path(pkg.__file__).parent / "py.typed"
    assert marker.is_file(), f"{pkg.__name__} ships no py.typed"
print("py.typed present in agentic, pyagentic, agentic_flink, agentic_pyflink, agentic_pipeline")
EOF

echo "== runtimes registered through the agentic.runtimes entry-point group"
python - <<'EOF'
from agentic.runtime import available_runtimes
names = available_runtimes()
print(sorted(names))
for expected in ("local", "local-jvm", "flink-jvm", "pekko", "pyflink"):
    assert expected in names, expected
assert "flink" not in names
EOF

echo "== pyagentic: one conformance fixture on the installed local runtime (fixtures from $WORK/spec)"
AGENTIC_SPEC_DIR="$WORK/spec" python -m agentic.conformance routing-keyword --runtime local

echo "== agentic-flink: the bundled framework jar is the one discovered"
python - <<'EOF'
import pathlib
import agentic_flink
from agentic_flink._classpath import framework_jar
jar = framework_jar()
assert pathlib.Path(agentic_flink.__file__).parent in jar.parents, jar
print("framework jar:", jar, f"({jar.stat().st_size // (1024 * 1024)} MiB)")
EOF

if command -v java >/dev/null 2>&1; then
    echo "== agentic-flink: start the JVM and run one fixture on the local-jvm runtime"
    AGENTIC_SPEC_ROOT="$WORK/spec" python "$WORK/smoke_clean_install.py" "$FIXTURE"
fi

echo "== agentic-pyflink: import, entry point and uber jar discovery through AGENTIC_FLINK_UBER_JAR"
python - <<'EOF'
import os
from importlib.metadata import entry_points
from agentic_flink._classpath import framework_jar
os.environ["AGENTIC_FLINK_UBER_JAR"] = str(framework_jar())
from agentic_pyflink import jars
from agentic_pyflink.runtime import FlinkRuntime
eps = {ep.name: ep.value for ep in entry_points().select(group="agentic.runtimes")}
assert eps["pyflink"] == "agentic_pyflink.runtime:FlinkRuntime", eps
print("uber jar for PyFlink:", jars.uber_jar())
try:
    jars.bridge_jar()
except jars.JarNotFoundError as exc:
    print("bridge jar (pyflink/java, not part of the wheel) is reported as missing:", str(exc).splitlines()[0])
else:
    raise SystemExit("the bridge jar must not be found without a checkout or AGENTIC_PYFLINK_JAR")
print("FlinkRuntime.name =", FlinkRuntime.name)
EOF

echo "== agentic-pipeline: console script runs one turn of the banking pipeline on the local backend"
agentic-pipeline run "$WORK/pipeline.yaml" --backend local --text "what is my balance?"

echo "== dry run finished: nothing was uploaded"
