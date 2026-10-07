# Contributing

Thank you for working on Agentic Streaming. This page describes the toolchain the repository is
built and tested with, how to run each check locally, and what a pull request needs. It refers
to `CLAUDE.md` and `AGENTS.md` for the module map and design rules, `docs/inventory.md` for
what is canonical and what is experimental, and `docs/versioning.md` for the versioning and
deprecation policy. By participating you agree to the `CODE_OF_CONDUCT.md`; security problems
go through `SECURITY.md`, not the issue tracker.

## Toolchain

Everything below was used to produce the current `main`. Versions are the ones the build files
pin or enforce; `docs/versioning.md` has the full compatibility matrix.

| Tool | Version | Used for |
|---|---|---|
| JDK | 21 (enforced by `reactor/pom.xml`; `java.version`) | every JVM module, the Python facades, Clojure |
| Maven | the committed wrapper `./mvnw` (Maven 3.9.16; 3.9 or newer is enforced) | never a system `mvn` |
| Podman | any current release, with `podman compose` or `podman-compose` | compose stacks, Testcontainers suites, `tools/smoke-examples.sh`, `tools/release_dry_run.sh` |
| Clojure CLI | any release that understands `deps.edn` `:aliases` and `-X` | `agentic-clj` |
| Python | 3.11 or newer for contributors (CI runs 3.12); the packages themselves declare `>=3.9` or `>=3.10` | the four Python packages, the spec tooling, the docs checker |

Docker is not used anywhere in this repository. Compose files and scripts call `podman` and
`podman compose`; do not introduce Docker commands or images that only exist for Docker.

If `repo.maven.apache.org` rate limits you (HTTP 429), point the wrapper and Maven at the
Google mirror: set `MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2`
and pass a `settings.xml` with a `<mirror>` whose `<mirrorOf>` is `central` and whose `<url>`
is the same address.

## Build

`reactor/pom.xml` is the parent and aggregator of every first-class JVM module
(`ports/jagentic-core`, the root `pom.xml` which is the Flink framework `agentic-flink`,
`agentic-pekko`, `pyflink/java`, `tool-services/tool-services-packs`,
`tool-services/tool-services-app`, `banking-job`; `a2a-gateway` behind the `a2a-gateway`
profile). One groupId, `org.jagentic`, one version, `1.0.0-SNAPSHOT`.

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

`ports/experimental/*` are not reactor modules; each has its own `pom.xml` and is built with
`./mvnw -f ports/experimental/<name>/pom.xml test`.

### Formatting

Java sources are formatted with google-java-format through Spotless. The default lifecycle
only checks formatting (`spotless:check` runs in the `verify` phase of the root module and
fails the build on a difference); it never rewrites your files. Format before committing:

```bash
./mvnw spotless:apply        # rewrite the Flink framework sources in place
./mvnw spotless:check        # what verify runs
```

### Javadoc

Every module with Java sources produces a `-javadoc.jar` at package time (`banking-job` has
none, so it only gets the `-sources.jar`). Doclint is relaxed by default so a
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
./mvnw test -P integration-tests
```

A service-backed test that skips because its service is missing is a failure in CI
(`tools/ci/skip_audit.py` against `tools/ci/skip-allowlist.txt`); do not add skips to make a
suite green. `ZeroMqChannelTest` can hang on some hosts and `PythonExecutorTest` needs a PEMJA
wheel for your interpreter; CI excludes both by name with
`-Dtest='!PythonExecutorTest,!ZeroMqChannelTest'`, and you may do the same locally as long as
you say so in the pull request.

## Python packages

Four distributions live in the repository. Install them editable from a checkout, in this
order (the pipeline package depends on `pyagentic`; the two Flink facades need the shaded jar
from the Maven build above):

```bash
python -m venv .venv && . .venv/bin/activate
python -m pip install -e "ports/pyagentic[test]"
python -m pip install -e "ports/agentic-pipeline[test]"
python -m pip install -e "python[test]"            # agentic-flink (JPype facade)
python -m pip install -e "pyflink[test]"           # agentic-pyflink
```

Run their tests from the repository root:

```bash
python -m pytest ports/pyagentic/tests -q
python -m pytest ports/agentic-pipeline/tests -q
python -m pytest python/tests -q                   # needs target/agentic-flink-*-uber.jar
python -m pytest pyflink/tests -q                  # needs the uber jar and pyflink/java's jar
```

`python/tests` can segfault at interpreter exit when the Pekko jars are on the JPype classpath;
the tests themselves have already reported by then. Report it if you see it rather than
working around it in the suite.

## Clojure

```bash
cd agentic-clj
clojure -X:test          # full suite
clojure -M:run           # banking demo on the Datomic runtime
```

`clojure -X:test` currently reports one pre-existing error in `agentic.cep-weave-test`; a pull
request that does not touch `agentic-clj` is not expected to fix it, but say so in the
description.

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

### The conformance matrix and the generated pages

`docs/capabilities.md` is generated. Never edit it by hand; regenerate it after the JVM modules
and `pyagentic` are installed:

```bash
./mvnw -f reactor/pom.xml -DskipTests install
python -m pip install -e ports/pyagentic -e python -e pyflink
python spec/tools/conformance_matrix.py --write-docs docs/capabilities.md
```

`conformance_matrix.py` runs every binding it can find (reference, jvm-core, flink, pekko,
clojure, python, pyflink, python-jvm, python-flink) and reports a missing toolchain as
`not_tested`. Pass `--require <binding>` to make a missing toolchain a failure, or `--runtimes`
to run a subset while iterating.

The runtime pages under `docs/runtimes/` carry excerpts of the matrix between
`<!-- matrix: ... -->` markers. They are generated too:

```bash
python docs/tools/matrix_excerpt.py --write     # rewrite the excerpts from docs/capabilities.md
python docs/tools/matrix_excerpt.py --check     # exit 1 when a page disagrees with the matrix
```

### The documentation checker

```bash
python -m pytest docs/tools/test_docs.py -q
```

It checks relative links and heading fragments across `docs/` and the READMEs, that the
Python snippets under `docs/snippets/python/` match the pages that show them, that the matrix
excerpts are current, and that the pages it covers contain no en or em dashes. All prose in
this repository follows the same rule: plain sentences, no en or em dashes, no emoji, no
marketing language, and no claim about a runtime that is not backed by a test or by the
generated matrix.

## Examples

`examples/` holds the workflow documents and Java showcases, `examples-bin/` the runner
scripts. If you touch either, run the smoke runner, which needs Podman for the service-backed
examples:

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
- Do not add publishing configuration (Sonatype, GPG, PyPI, Clojars) or tags; releases are
  described in `docs/release.md` and are not part of ordinary pull requests.
- Deprecate before you remove, following `docs/versioning.md`, and add the entry to
  `CHANGELOG.md` under `Unreleased`.
- The legacy Flink `AgentBuilder` DSL is a kept, supported pure-Flink API outside the
  conformance path; do not remove it or describe it as removed.
- Before opening the pull request run, for every module you touched, its full test suite, plus
  `python spec/tools/validate_spec.py` and `python -m pytest docs/tools/test_docs.py -q`, and
  `bash tools/smoke-examples.sh` if you touched examples or scripts. Put the exact commands
  and their results in the description, including anything you could not run and why.
