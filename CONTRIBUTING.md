# Contributing

Thank you for working on Agentic Streaming. This page describes the toolchain the repository is
built and tested with, how to run each check locally, and what a pull request needs. It refers
to `CLAUDE.md` and `AGENTS.md` for the module map and design rules, and `docs/versioning.md` for the versioning and
deprecation policy. By participating you agree to the `CODE_OF_CONDUCT.md`; security problems
go through `SECURITY.md`, not the issue tracker.

## Toolchain

Everything below was used to produce the current `main`. Versions are the ones the build files
pin or enforce; `docs/versioning.md` has the full compatibility matrix.

| Tool | Version | Used for |
|---|---|---|
| JDK | 21 (enforced by `reactor/pom.xml`; `java.version`) | every JVM module |
| Maven | the committed wrapper `./mvnw` (Maven 3.9.16; 3.9 or newer is enforced) | never a system `mvn` |
| Docker or Podman | any current release, with `compose` | compose stacks, Testcontainers suites, `tools/smoke-examples.sh`, `tools/release_dry_run.sh` |
| Python | 3.11 or newer for contributors (CI runs 3.12); the packages themselves declare `>=3.9` | the two Python packages, the spec tooling, the docs checks |

Docker works as well as Podman. Compose files run under either, and scripts pick whichever engine
is running through `tools/container-engine.sh`, so a script must not name one engine directly
(`tools/ci/test_container_engine.py` checks this).

If `repo.maven.apache.org` rate limits you (HTTP 429), point the wrapper and Maven at the
Google mirror: set `MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2`
and pass a `settings.xml` with a `<mirror>` whose `<mirrorOf>` is `central` and whose `<url>`
is the same address.

## Build

`reactor/pom.xml` is the parent and aggregator of every first-class JVM module
(`ports/jagentic-core`, the root `pom.xml` which is the Flink framework `agentic-flink`,
and `agentic-pekko`). One groupId, `org.jagentic`, one version, `1.0.0-SNAPSHOT`.

```bash
./mvnw -f reactor/pom.xml -DskipTests install     # compile and install everything, no tests
./mvnw -f reactor/pom.xml verify                  # every module, all unit tests, formatting check
./mvnw -f reactor/pom.xml -DskipTests package     # jars plus -sources.jar and -javadoc.jar per module
```

A single module can be built on its own once its upstream modules are installed:

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw test                                       # the Flink framework (root pom)
./mvnw -f agentic-pekko/pom.xml test
```

### Formatting

Java sources are formatted with google-java-format through Spotless. The default lifecycle
only checks formatting (`spotless:check` runs in the `verify` phase of the root module and
fails the build on a difference); it never rewrites your files. Format before committing:

```bash
./mvnw spotless:apply        # rewrite the Flink framework sources in place
./mvnw spotless:check        # what verify runs
```

### Javadoc

Every module with Java sources produces a `-javadoc.jar` at package time. Doclint is relaxed by default so a
module with older comments still packages; the `strict-javadoc` profile turns every doclint
group except `missing` into a build failure. `ports/jagentic-core` passes the strict profile
and new code in any module should as well:

```bash
./mvnw -f reactor/pom.xml -DskipTests -P strict-javadoc package -pl ../ports/jagentic-core
```

### Integration tests

The Testcontainers suites run under the `integration-tests` profile against Podman. Start the
user socket once and point Testcontainers at it:

```bash
systemctl --user start podman.socket
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
./mvnw -f agentic-pekko/pom.xml verify -P integration-tests
```

A service-backed test that skips because its service is missing is a failure in CI
(`tools/ci/skip_audit.py` against `tools/ci/skip-allowlist.txt`); do not add skips to make a
suite green.

## Python packages

Two distributions live in the repository. Install them editable from a checkout, in this order
(the pipeline package depends on `pyagentic`):

```bash
python -m venv .venv && . .venv/bin/activate
python -m pip install -e "ports/pyagentic[test]"
python -m pip install -e "ports/agentic-pipeline[test]"
```

Run their tests from the repository root:

```bash
python -m pytest ports/pyagentic/tests -q
python -m pytest ports/agentic-pipeline/tests -q
```

## The specification and conformance

`spec/v1` is the contract every runtime implements and `spec/conformance/v1` holds the shared
fixtures. The spec tooling needs `pyyaml`, `jsonschema` and `pytest` in your interpreter
(installing `ports/pyagentic[test]` provides them).

```bash
python spec/tools/validate_spec.py               # schemas, fixtures, examples/pipelines
python spec/tools/run_conformance.py             # the reference runtime against every fixture
python -m pytest spec/tools -q                   # tests of the tooling itself
```

Do not edit `spec/v1/*.schema.json` or the fixtures to make a runtime pass. Section 7 of
`spec/v1/primitives.md` says which changes are additive under `agentic/v1` and which require
`agentic/v2`.

### The conformance matrix

No matrix is committed; see `docs/runtimes/README.md`. To produce one, after the JVM modules and
`pyagentic` are installed:

```bash
./mvnw -f reactor/pom.xml -DskipTests install
python -m pip install -e ports/pyagentic
python spec/tools/conformance_matrix.py --output build/conformance-matrix.json --write-docs build/capabilities.md
```

`conformance_matrix.py` runs every binding it can find (reference, jvm-core, flink, pekko, python)
and reports a missing toolchain as `not_tested`. Pass `--require <binding>` to make a missing
toolchain a failure, or `--runtimes` to run a subset while iterating.

### The documentation checker

```bash
python -m pytest docs/tools/test_docs.py -q
```

It checks relative links and heading fragments across `docs/` and the READMEs, that the
Python snippets under `docs/snippets/python/` match the pages that show them, and that the pages it
covers contain no en or em dashes. All prose in
this repository follows the same rule: plain sentences, no en or em dashes, no emoji, no
marketing language, and no claim about a runtime that is not backed by a test.

## Examples

`examples/pipelines/` holds the workflow documents. If you touch them, run the smoke runner, which
needs `timeout` from coreutils and, for the MCP examples, `pip install "mcp<2"`:

```bash
bash tools/smoke-examples.sh
```

## Pull requests

- Work on a branch, open one pull request per change, and keep it inside one area. Stage files
  explicitly (`git add <path>`), never `git add .`, so build output and local settings do not
  end up in a commit.
- Every behavior change ships with a test or an executable check. Tests use JUnit 5 on the
  JVM and pytest in Python, with randomized data where the values are arbitrary. Do not weaken
  a test, a fixture or the reference runtime to make something pass.
- No TODOs, no placeholders, no silent no-ops: implement a working subset instead.
- Do not add publishing configuration (Sonatype, GPG, PyPI) or tags. Nothing is published.
- Deprecate before you remove, following `docs/versioning.md`, and add the entry to
  `CHANGELOG.md` under `Unreleased`.
- Before opening the pull request run, for every module you touched, its full test suite, plus
  `python spec/tools/validate_spec.py` and `python -m pytest docs/tools/test_docs.py -q`, and
  `bash tools/smoke-examples.sh` if you touched examples or scripts. Put the exact commands
  and their results in the description, including anything you could not run and why.
