# Cutting a release

This page describes how a release of the packaged artifacts is produced: the four Python
distributions and the Clojure library. It covers the tag format, the checks every artifact goes
through, what the two publish workflows do, the steps that stay manual, and what is deliberately not
published. Commands marked "run for this document" were executed on a checkout of this repository
while writing it; the others describe the intended flow and have not been exercised against PyPI,
TestPyPI or Clojars.

## What is versioned

One git tag names one version for every artifact:

| Artifact | Directory | Import name / coordinates | Version source |
|---|---|---|---|
| `pyagentic` sdist and wheel | `ports/pyagentic` | `agentic`, `pyagentic` | setuptools-scm |
| `agentic-flink` sdist and wheel | `python` | `agentic_flink` (bundles the shaded framework jar) | setuptools-scm |
| `agentic-pyflink` sdist and wheel | `pyflink` | `agentic_pyflink` | setuptools-scm |
| `agentic-pipeline` sdist and wheel | `ports/agentic-pipeline` | `agentic_pipeline`, console script `agentic-pipeline` | setuptools-scm |
| `agentic-clj` jar and pom | `agentic-clj` | `io.github.ugbot/agentic-clj` | `build/agentic/build/version.clj` |

No `pyproject.toml` carries a `version` field and no Clojure file carries a version string. The
Maven modules (root `agentic-flink`, `ports/jagentic-core`, `agentic-pekko`, `pyflink/java`,
`a2a-gateway`, `banking-job`, the tool services) keep their pom versions and are not part of this
flow; see "What is not published".

## Tag format

A release tag is `v` followed by a canonical PEP 440 public version, for example `v1.0.0`,
`v1.0.0a1`, `v1.0.0b2`, `v1.0.0rc1`, `v1.0.1.post1`. The `v` prefix is what setuptools-scm strips
by default and what the Clojure build strips, so the tag `v1.0.0rc1` produces the version `1.0.0rc1`
in every artifact, including the Clojure jar (Maven treats the version as an opaque string, and the
same string is used on purpose so the two ecosystems can be compared literally).

Tags that are not canonical are rejected by `tools/check_release_version.py` and by
`clojure -T:build jar`: `v1.0`, `v1.0.0-rc1`, `v01.0.0`, `v1.0.0RC1`, `v1.0.0+local` and
`release-1` all fail. Local version labels (`+something`) are never produced:
`local_scheme = "no-local-version"` is set in all four `pyproject.toml` files.

Between tags, or with uncommitted changes, both mechanisms produce a development version and the
release checks fail on purpose. With no `v*` tag reachable from `HEAD`, as on `main` today, the
version is `0.1.dev<commit count>`. After a tag, it is the next version (the trailing number of the
tag bumped) with `.dev<commits since the tag>`: `v1.0.0` plus three commits is `1.0.1.dev3`,
`v1.0.0rc1` plus three commits is `1.0.0rc2.dev3`. The Clojure rule is pinned by
`agentic-clj/test/agentic/build_version_test.clj`; the Python rule is setuptools-scm's default
(`guess-next-dev`).

## End to end

1. Make sure `main` is green locally: the Maven build in the documented order, the Python test
   suites, `clojure -X:test`, and `spec/tools/run_conformance.py`. GitHub Actions is disabled on this
   repository, so nothing runs on push; the workflows below only run when they are triggered on a
   repository where Actions is enabled.
2. Pick the version and create an annotated tag on the commit to release:
   `git tag -a v1.0.0rc1 -m "agentic 1.0.0rc1"` and `git push origin v1.0.0rc1`.
3. Check the tag from a clean checkout of it:
   `python tools/check_release_version.py v1.0.0rc1` prints the version and exits 0 when
   setuptools-scm resolves that version in all four Python projects; `clojure -T:build version`
   in `agentic-clj` prints the same string.
4. Rehearse on TestPyPI: run the `Publish Python packages to TestPyPI` workflow by hand with the tag
   as input. Install from TestPyPI into a scratch environment and repeat the smoke test.
5. Publish a GitHub Release for the tag. The `Publish Python packages to PyPI` workflow runs on the
   `published` event.
6. Build and install the Clojure artifact locally (`clojure -T:build install`), then publish it to
   Clojars by hand following the steps below.
7. Announce the release and point the documentation at the new version where it names one.

## Python: build and check locally

Run these in the four project directories, or use the shortcuts below. All of them were run for this
document on the untagged `main` checkout, so the version they produced was `0.1.dev199`; on a tag
they produce the tagged version.

```bash
python -m pip install build twine setuptools-scm

# the shaded framework jar the agentic-flink wheel bundles (python/setup.py looks for
# target/agentic-flink-*-uber.jar, or AGENTIC_FLINK_JAR, and runs ./mvnw itself if neither exists)
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw clean package -DskipTests

for project in ports/pyagentic python pyflink ports/agentic-pipeline; do
  (cd "$project" && python -m build)
done

python tools/check_release_version.py v1.0.0rc1 --complete \
  ports/pyagentic/dist/* python/dist/* pyflink/dist/* ports/agentic-pipeline/dist/*
python -m twine check --strict \
  ports/pyagentic/dist/* python/dist/* pyflink/dist/* ports/agentic-pipeline/dist/*
```

`tools/check_release_version.py TAG` checks that the tag is canonical and that setuptools-scm
resolves the same version in the four projects. Given artifact paths it also checks that every
sdist and wheel file name carries that version; `--complete` additionally requires one sdist and
one wheel per distribution. `--no-scm` skips the setuptools-scm step for checking artifacts outside
a checkout. `python/tests/test_release_version.py` covers the script.

Every wheel ships `py.typed`. Package data is declared explicitly in each `pyproject.toml`:
`agentic/schemas/*.json` for `pyagentic`, the shaded jar and its README under `agentic_flink/jars/`
for `agentic-flink`. The conformance fixtures under `spec/` are not packaged; the runners read them
from a checkout or from the directory named by `AGENTIC_SPEC_DIR` (`pyagentic`) and
`AGENTIC_SPEC_ROOT` (`agentic-flink`). The `agentic-flink` sdist contains the jar as well, because
`python/setup.py` copies it into the package tree before either artifact is built; both artifacts
are around 250 MB.

`pyagentic` runs on Python 3.9 and newer. `agentic-flink` and `agentic-pyflink` require 3.10, so
the `flink`, `pyflink` and `jvm` extras of `pyagentic` carry a `python_version >= '3.10'` marker
and resolve to nothing on 3.9.

### Development installs with uv

Each project has a `uv.lock` next to its `pyproject.toml` (generated with `uv lock` in that
directory and refreshed the same way when dependencies change). `uv sync` in a project directory
creates `.venv` with the project, its `dev` dependency group (pytest, build, twine, and for
`pyagentic` mypy and ruff) and, for `ports/pyagentic` and `ports/agentic-pipeline`, the sibling
projects as editable installs through `[tool.uv.sources]`:

```bash
cd ports/pyagentic && uv sync && .venv/bin/pytest
cd python && uv sync && .venv/bin/pytest
cd pyflink && uv sync && .venv/bin/pytest
cd ports/agentic-pipeline && uv sync && .venv/bin/pytest
```

uv is a development tool only. The wheels declare their runtime dependencies in the usual
metadata and install with pip; nothing in a wheel or sdist refers to uv.

### Clean-environment smoke test

`tools/release_dry_run.sh` is the TestPyPI-style dry run without an upload. It stages the four
wheels, the `spec/` tree, `examples/pipelines/banking.yaml` and `python/tests/smoke_clean_install.py`
into an empty directory, so no import can resolve to the checkout, and runs `tools/release_smoke.sh`
there. That script installs the wheels with pip, then:

- imports `agentic`, `pyagentic`, `agentic_flink`, `agentic_pyflink` and `agentic_pipeline`, checks
  that the four distributions report one version and that each package ships `py.typed`;
- checks that the `agentic.runtimes` entry points `local`, `local-jvm`, `flink-jvm`, `pekko` and
  `pyflink` are registered;
- runs the `routing-keyword` fixture on the installed `local` runtime with
  `python -m agentic.conformance`;
- checks that `agentic_flink` discovers the jar bundled in its own wheel, and, when a JDK is on
  the path, starts the JVM and runs the same fixture on the `local-jvm` runtime;
- imports the PyFlink binding, checks its entry point, resolves the uber jar through
  `AGENTIC_FLINK_UBER_JAR` from the `agentic-flink` wheel, and checks that the bridge jar (built by
  Maven from `pyflink/java`, not part of any wheel) is reported as missing rather than found;
- runs one turn of the banking pipeline through the `agentic-pipeline` console script on the
  `local` backend.

Two runners are supported:

```bash
tools/release_dry_run.sh               # Podman, python:3.12, installs openjdk-21-jdk-headless with apt
JDK=none tools/release_dry_run.sh      # same container without a JDK: import and jar discovery only
RUNNER=venv tools/release_dry_run.sh   # a fresh virtualenv on this machine (what the workflows run)
BUILD=1 tools/release_dry_run.sh       # rebuild the four projects first
```

Both runners were run for this document; the Podman run pulled `python:3.12`, installed the JDK,
started the JVM and passed the fixture on both runtimes. The project uses Podman, not Docker.

## What the publish workflows do

`.github/workflows/publish-pypi.yml` runs when a GitHub Release is published (or by hand with a
tag as input). `.github/workflows/publish-testpypi.yml` runs only by hand with a tag as input and
uploads to TestPyPI. Both have the same `build` job:

1. check out exactly the tag with full history and tags (setuptools-scm needs them);
2. `python tools/check_release_version.py "$RELEASE_TAG"` (canonical tag, four projects agree);
3. Java 21, `./mvnw -f ports/jagentic-core/pom.xml install -DskipTests`, `./mvnw package -DskipTests`
   for the shaded framework jar;
4. `python -m build` in the four project directories, collected into `dist/`, plus a check that the
   `agentic-flink` wheel contains `agentic_flink/jars/agentic-flink-*-uber.jar`;
5. `python tools/check_release_version.py "$RELEASE_TAG" --complete dist/*` (one sdist and one wheel
   per distribution, all at the tagged version);
6. `python -m twine check --strict dist/*`;
7. `RUNNER=venv DIST=dist tools/release_dry_run.sh` (the smoke test above, in a fresh virtualenv);
8. upload of `dist/*` as a workflow artifact.

The `publish` job downloads that artifact, copies the sdist and wheel of every distribution named
in the workflow's `PUBLISH_DISTRIBUTIONS` variable into `upload/`, and hands that directory to
`pypa/gh-action-pypi-publish` with OIDC trusted publishing. No API token is stored in the
repository; the trusted publisher configuration on pypi.org and test.pypi.org (project, owner
`Ugbot`, repository `Agentic-Streaming`, workflow file name, environment `pypi` or `testpypi`) is
what authorizes the upload, and it has to be registered against the current repository name.

Today `PUBLISH_DISTRIBUTIONS` is `agentic-flink` in both workflows, the one project that had a
publish workflow before. The other three are built, checked and smoke tested on every run but not
uploaded, for these reasons, each of which needs a decision or an account action outside this
repository:

- `pyagentic` is already taken on PyPI by an unrelated project (`docs/audit-backlog.md`, AGS-11).
  Publishing the pure package needs a new distribution name and a trusted publisher for it.
- `agentic-pipeline` depends on `pyagentic`, so it cannot be published before the pure package is
  installable from the same index under whatever name is chosen.
- `agentic-pyflink` needs a trusted publisher registered for that project name; it also needs the
  `pyflink/java` bridge jar at runtime, which is not part of the wheel.

Adding a name to `PUBLISH_DISTRIBUTIONS` after registering its trusted publisher is the only change
needed to start uploading it. One more manual step applies to `agentic-flink` alone: its wheel and
sdist are around 250 MB because of the bundled jar, above PyPI's default per-file limit, so a file
size limit increase has to be granted for the project (https://pypi.org/help/#file-size-limit)
before the first upload can succeed. `python/PUBLISHING.md` keeps the account and trusted publisher
setup for that project. The build steps of both workflows were exercised for this document
with the workflow's shell verbatim on the local checkout (with `v0.1.dev199` as the tag argument,
the version the untagged checkout resolves to), and the files pass `actionlint`. The upload steps
were not run: GitHub Actions is disabled on this repository and nothing was uploaded to PyPI or
TestPyPI while preparing this flow.

## Clojure: jar, install, and publishing to Clojars

Build and install locally from `agentic-clj` (run for this document; the checkout had no tag, so
the version was `0.1.dev199`):

```bash
cd agentic-clj
clojure -T:build version    # 0.1.dev199 on untagged main; 1.0.0rc1 on the tag v1.0.0rc1
clojure -T:build jar        # target/agentic-clj-<version>.jar and target/pom.xml
clojure -T:build install    # ~/.m2/repository/io/github/ugbot/agentic-clj/<version>/
```

The pom (`io.github.ugbot/agentic-clj`) lists the library dependencies from `deps.edn`, the
Apache-2.0 license, the project URL and an `<scm>` block whose `<tag>` is the release tag when
the build is exactly at one and the commit otherwise. After `install`, a consumer resolves the
artifact from `~/.m2` with `{:deps {io.github.ugbot/agentic-clj {:mvn/version "<version>"}}}`; this
was checked for this document by requiring `agentic.pipeline` from an unrelated directory.

`clojure -X:test` is unchanged by the packaging work apart from the new
`agentic.build-version-test`. The suite carries one pre-existing error in `agentic.cep-weave-test`
that predates this work and is unrelated to packaging.

### Publishing to Clojars (not executed)

None of the following was run; the steps are written down so the release can be finished by hand.

1. Clojars requires a verified group name for new artifacts. According to the Clojars
   documentation (https://github.com/clojars/clojars-web/wiki/Verified-Group-Names),
   `io.github.<github-username>` is verified automatically when the Clojars account logs in
   through GitHub as that user, so `io.github.ugbot` needs the `Ugbot` GitHub account to sign in
   to Clojars once. Until the group is verified, uploads to `io.github.ugbot/agentic-clj` are
   rejected.
2. Create a deploy token on https://clojars.org/tokens (Clojars does not accept the account
   password for deploys) and export it: `CLOJARS_USERNAME=<user>` and
   `CLOJARS_PASSWORD=<deploy token>`. Do not commit either value.
3. On a clean checkout of the tag, `clojure -T:build jar` in `agentic-clj`, and check that the
   version printed equals the tag without the `v`.
4. Deploy the jar with its pom. `deps-deploy` (`slipset/deps-deploy`) reads the two environment
   variables above:

   ```bash
   clojure -Sdeps '{:deps {slipset/deps-deploy {:mvn/version "0.2.2"}}}' \
     -X deps-deploy.deps-deploy/deploy \
     :installer :remote \
     :artifact '"target/agentic-clj-1.0.0rc1.jar"' \
     :pom-file '"target/pom.xml"'
   ```

   `build.clj` intentionally has no `deploy` task, so an untested upload path is not part of the
   build file.
5. Check https://clojars.org/io.github.ugbot/agentic-clj shows the version, then resolve it from a
   scratch `deps.edn` (`{:deps {io.github.ugbot/agentic-clj {:mvn/version "1.0.0rc1"}}}`) to confirm
   the pom's dependencies resolve for a consumer.

## What is intentionally not published

- Nothing goes to Maven Central. The Maven modules (`org.agentic.flink:agentic-flink`,
  `org.jagentic:jagentic-core`, `org.jagentic.pekko:agentic-pekko`, the PyFlink bridge, the A2A
  gateway, `banking-job`, the tool services) keep their pom versions and have no
  `distributionManagement`, signing or central-publishing configuration. This is a decision, not
  an omission; the Java artifacts reach users inside the `agentic-flink` wheel (the shaded jar) and
  through a source build.
- Three of the four Python distributions are built and checked on every publish run but are not
  uploaded until the naming and trusted publisher points above are settled.
- The Clojure artifact is installed locally and its Clojars steps are documented, not automated.
- No container images are published.
- Nothing is uploaded by the tooling in this repository unless a workflow's `publish` job runs on
  a repository with GitHub Actions enabled and a registered trusted publisher.
