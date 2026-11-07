# API stability

This page describes the stability markers on the Flink framework (the root Maven module,
artifact `agentic-flink`, packages under `org.agentic.flink`) and the deprecation policy that
goes with them. The markers live in `org.agentic.flink.annotation`. Every public top-level
type under `src/main` carries exactly one of them; `ApiStabilityMarkerTest` in the root module
walks the compiled classes and fails the build when a type has none or more than one. The
markers are `@Documented` with runtime retention, so they show up in Javadoc and can be read
by tooling.

The `agentic/v1` specification under `spec/v1` and the conformance fixtures are governed by
the spec, not by these markers. The canonical core in `ports/jagentic-core` and the other
runtimes are outside the scope of this page.

## The two levels

### `@Public`

A `@Public` type is the supported API of the framework. Its source and binary compatibility
are kept within a major release. A public member is removed or changed in signature only
through the deprecation cycle described below.

`@Public` covers the `agentic/v1` runtime entry points (`WorkflowTurnFunction`,
`FlinkRuntimeOptions`, `LocalWorkflowSession`, `FlinkPipelineRunner`, `KafkaPipelineJob`) and
configuration (`AgenticFlinkConfig`, `ConfigKeys`, `FlinkJobDefaults`).

### `@Internal`

An `@Internal` type is public in the Java sense only because another package of the framework
needs to reach it. It is not API and may change or disappear in any release, including patch
releases. It is not covered by the deprecation policy.

`@Internal` covers runtime plumbing (`TurnResultCodec`, `TurnResultTypeInfo`, `ChatClientFactories`)
and helpers.

## Deprecation policy

Deprecating a `@Public` type or member is the only way to remove it or to change its
signature. A deprecation:

1. adds `@Deprecated(since = "<version>")`, where `<version>` is the first release that ships
   the deprecation;
2. adds a Javadoc `@deprecated` tag that names the replacement (a type, a method or a
   documented procedure) and, where useful, why the old API is being retired;
3. keeps the deprecated API working for at least one minor release after `<version>`. A type
   deprecated in `1.0.0` may be removed in `1.2.0` at the earliest, never in `1.1.x`.

`@Internal` types can be changed or removed without this cycle.

## Adding a new public type

Pick the marker when the type is written, not later. A new type that users are expected to
call and that is ready to be supported is `@Public`; a function, codec or helper that only exists
so another package can use it is `@Internal`. `ApiStabilityMarkerTest` fails until the marker is present, and it fails if a
type carries two.
