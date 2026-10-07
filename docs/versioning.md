# Versioning, deprecation and compatibility

This page states how the artifacts of this repository are versioned, how the `agentic/v1`
specification is versioned independently of them, what a deprecation looks like, and which
versions of the toolchain and the main dependencies the current `main` is built and tested
against. Every number below is read from a build file named next to it; when a build file
changes, this page changes in the same pull request.

Nothing is published yet (no tags, no Maven Central, PyPI or Clojars artifacts). The release
mechanics, tag format and per-artifact version sources are in `docs/release.md`.

## Artifact versions: semantic versioning

The artifacts are versioned with Semantic Versioning 2.0.0 (`MAJOR.MINOR.PATCH`):

- `MAJOR` changes when a public API is removed or changes incompatibly (a Java or Clojure
  signature, a Python function or class, a YAML key that a runtime accepted, a CLI flag).
- `MINOR` changes when public API is added, or when an API is deprecated, in a backward
  compatible way.
- `PATCH` changes for bug fixes that neither add nor remove public API.

One version applies to all artifacts of one release. The JVM modules carry it in
`reactor/pom.xml` (`1.0.0-SNAPSHOT` on `main`, inherited by every module); the four Python
distributions and the Clojure library derive it from the release tag (`v` plus a canonical
PEP 440 version) as described in `docs/release.md`. Between tags they build as development
versions and are not releasable.

Which classes, functions and keys count as public API is defined by the API stability
annotations documented in `docs/api-stability.md`. Anything not marked stable there, the
`ports/experimental/*` adapters, and the internal packages of each module may change in any
release.

## Spec version: `agentic/v1` is independent

The specification under `spec/v1` has its own version, `agentic/v1`, which appears as
`spec_version` in every workflow document. It is not tied to the artifact version: a `2.0.0`
release of the libraries can still implement `agentic/v1`, and a `1.x` release never
implements a spec other than `agentic/v1`.

The spec version changes only for an incompatible change to the primitives, as
`spec/v1/primitives.md`, section 7, defines it: removing or re-meaning a field, an event type
or a payload key, changing the expectation of an existing conformance fixture, or changing a
normative default. Such a change produces `agentic/v2` under a new directory `spec/v2` with a
new fixture set, and the `v1` fixtures keep running against `v1` runtimes. Additive changes
(new optional fields, capability ids, event types and fixtures numbered `25` and up) stay under
`agentic/v1`, subject to the unknown-field rule of the same section. `spec/tools/validate_spec.py`
and `python -m pytest spec/tools` are the gate that decides whether a change is additive.

A runtime declares the spec versions it accepts and rejects a newer major version with a
`validation` error. Implementing a new spec version in a runtime is a `MINOR` artifact change
as long as the previous version is still accepted; dropping support for a spec version is a
`MAJOR` artifact change.

## Deprecation policy

A public API is never removed without first being deprecated in a release that still supports
it. The deprecation period is one minor release: an API deprecated in `X.Y.0` keeps working,
with its warning, through the whole `X.(Y+1).z` series. Removing public API is a `MAJOR` change
under semantic versioning, so the removal itself lands in the next major release, and the one
minor release is the minimum notice a user gets before that major release may drop the API.

A deprecation ships with all of the following:

- Java and JVM-visible API: the `@Deprecated(since = "X.Y.0")` annotation with the version in
  which the deprecation was introduced, plus a `@deprecated` Javadoc tag that names the
  replacement.
- Python: a `DeprecationWarning` raised on use, naming the replacement, and a note in the
  docstring.
- Clojure: `:deprecated "X.Y.0"` in the var metadata and a docstring naming the replacement.
- Workflow document keys: the runtime accepts the old key, emits a warning naming the new key,
  and the schema documents both.
- A `CHANGELOG.md` entry under `Deprecated` that names the API, the version and the
  replacement.

A replacement must exist and be documented before an API is deprecated. Deprecating without a
replacement is not allowed; if a feature is being dropped entirely, the deprecation entry says
so and states what users should do instead. The legacy Flink `AgentBuilder` DSL is a supported
pure-Flink API outside the conformance path and is subject to this policy like every other
public API.

## Compatibility matrix

All versions are taken from the build files listed in the second column.

| Component | Source | Version |
|---|---|---|
| Framework artifacts (`org.jagentic:*`, every reactor module) | `reactor/pom.xml` `project.version` | `1.0.0-SNAPSHOT` |
| Specification | `spec/v1/primitives.md` | `agentic/v1`, frozen for fixtures 01 to 24 |
| JDK | `reactor/pom.xml` `java.version` (enforced, `maven.compiler.release`) | 21 |
| Maven | `reactor/pom.xml` enforcer rule; `.mvn/wrapper/maven-wrapper.properties` | 3.9 or newer required; wrapper 3.9.16 |
| Apache Flink | `reactor/pom.xml` `flink.version` | 2.2.1 |
| Flink Kafka connector | root `pom.xml` | 5.0.0-2.2 |
| LangChain4J | `reactor/pom.xml` `langchain4j.version` (via `langchain4j-bom`) | 1.16.3 |
| Apache Pekko | `agentic-pekko/pom.xml` `pekko.version`, Scala binary `2.13` | 1.1.3 |
| Quarkus platform (tool services, A2A gateway) | `reactor/pom.xml` `quarkus.platform.version` | 3.30.6 |
| JUnit | `reactor/pom.xml` `junit.version` | 5.10.2 |
| Testcontainers | `reactor/pom.xml` `testcontainers.version` | 1.21.4 |
| Python, `pyagentic` (`ports/pyagentic`) | `requires-python` | `>=3.9`; classifiers 3.9 to 3.13 |
| Python, `agentic-flink` (`python/`) | `requires-python` | `>=3.10`; classifiers 3.10 to 3.13 |
| Python, `agentic-pyflink` (`pyflink/`) | `requires-python` | `>=3.10`; classifiers 3.10 to 3.12 |
| Python, `agentic-pipeline` (`ports/agentic-pipeline`) | `requires-python` | `>=3.9`; classifiers 3.9 to 3.13 |
| apache-flink (PyFlink wheel) | `pyflink/pyproject.toml`, `python/pyproject.toml` extras | `>=2.0,<3` (pyflink); `>=2.0` (agentic-flink extras) |
| JPype | `python/pyproject.toml` | `JPype1>=1.5.0` |
| Clojure | `agentic-clj/deps.edn` | 1.12.0 |
| Datomic local | `agentic-clj/deps.edn` | 1.0.291 |

The CI workflows (`.github/workflows/ci.yml`) exercise the Python packages on Python 3.12; the
`requires-python` lower bounds above are what the packages declare, not what is tested on every
commit. The runtimes that pass each conformance fixture are listed in the generated
`docs/capabilities.md`, never here.
