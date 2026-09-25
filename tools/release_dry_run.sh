#!/usr/bin/env bash
# TestPyPI-style dry run of the four Python distributions in a clean environment, without the
# repository checkout and without uploading anything anywhere.
#
#   tools/release_dry_run.sh                 # Podman, python:3.12: the wheels already in the dist/ dirs
#   BUILD=1 tools/release_dry_run.sh         # run `python -m build` in each project first
#   RUNNER=venv tools/release_dry_run.sh     # no container: a fresh virtualenv (the publish workflows)
#
# The environment only sees a staging directory with: the wheels, the spec tree (fixtures,
# workflows and schemas are data the conformance runners read; they are not part of any wheel),
# one pipeline.yaml example, and the two smoke scripts. tools/release_smoke.sh then
# installs the wheels with pip, imports every package and runs the smallest end-to-end check per
# package; see that script and docs/release.md.
#
# Environment:
#   RUNNER         "podman" (default) or "venv".
#   DIST           a directory holding the wheels of all four projects (the publish workflows
#                  collect them there); default: each project's own dist/ directory.
#   PYTHON_IMAGE   container image for RUNNER=podman (default python:3.12).
#   JDK            RUNNER=podman: "apt" (default) installs openjdk-21-jdk-headless in the container so
#                  that agentic-flink can start the JVM and run a fixture; "none" stops the
#                  agentic-flink check after import and jar discovery. RUNNER=venv always uses the
#                  JDK on PATH, if any.
#   PYTHON         interpreter for RUNNER=venv and for BUILD=1 (default: python).
#   BUILD          "1" to rebuild the distributions before the run.
#   STAGE_KEEP     "1" to keep the staging directory (its path is printed).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="${RUNNER:-podman}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.12}"
PYTHON="${PYTHON:-python}"
JDK="${JDK:-apt}"
PROJECTS=(ports/pyagentic python pyflink ports/agentic-pipeline)

case "$RUNNER" in
    podman)
        if ! command -v podman >/dev/null 2>&1; then
            echo "podman is required for RUNNER=podman (this project does not use Docker)" >&2
            exit 2
        fi ;;
    venv) ;;
    *) echo "RUNNER must be podman or venv, not '$RUNNER'" >&2; exit 2 ;;
esac

if [ "${BUILD:-0}" = "1" ]; then
    for project in "${PROJECTS[@]}"; do
        echo "== python -m build ($project)"
        (cd "$REPO/$project" && rm -rf dist && "$PYTHON" -m build)
    done
fi

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/agentic-release-dry-run.XXXXXX")"
if [ "${STAGE_KEEP:-0}" != "1" ]; then
    trap 'rm -rf "$STAGE"' EXIT
fi
mkdir -p "$STAGE/dist"
if [ -n "${DIST:-}" ]; then
    for stem in pyagentic agentic_flink agentic_pyflink agentic_pipeline; do
        wheels=("$DIST/$stem"-*.whl)
        if [ ! -f "${wheels[0]}" ]; then
            echo "no $stem wheel in $DIST" >&2
            exit 2
        fi
        cp "${wheels[@]}" "$STAGE/dist/"
    done
else
    for project in "${PROJECTS[@]}"; do
        wheels=("$REPO/$project"/dist/*.whl)
        if [ ! -f "${wheels[0]}" ]; then
            echo "no wheel in $project/dist; run with BUILD=1 or build it first" >&2
            exit 2
        fi
        cp "${wheels[@]}" "$STAGE/dist/"
    done
fi
cp -r "$REPO/spec" "$STAGE/spec"
cp "$REPO/examples/pipelines/banking.yaml" "$STAGE/pipeline.yaml"
cp "$REPO/python/tests/smoke_clean_install.py" "$STAGE/smoke_clean_install.py"
cp "$REPO/tools/release_smoke.sh" "$STAGE/run.sh"

echo "== staged in $STAGE (no repository checkout):"
(cd "$STAGE" && find . -maxdepth 2 -not -path './spec/*' | sort)

case "$RUNNER" in
    podman)
        echo "== podman run --rm $PYTHON_IMAGE (JDK=$JDK)"
        podman run --rm -e "JDK=$JDK" -e WORK=/work -v "$STAGE:/work:ro" "$PYTHON_IMAGE" bash /work/run.sh ;;
    venv)
        echo "== fresh virtualenv $STAGE/venv ($("$PYTHON" --version))"
        "$PYTHON" -m venv "$STAGE/venv"
        # shellcheck disable=SC1091
        source "$STAGE/venv/bin/activate"
        WORK="$STAGE" JDK=none bash "$STAGE/run.sh" ;;
esac
