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

## The three levels

### `@Public`

A `@Public` type is the supported API of the framework. Its source and binary compatibility
are kept within a major release. A public member is removed or changed in signature only
through the deprecation cycle described below.

`@Public` covers the DSL builder entry points (`org.agentic.flink.dsl.Agent`,
`AgentBuilder`, `SupervisorChain`, `SupervisorChainBuilder` and the `job` builders), the tool
surface (`ToolExecutor`, `AbstractToolExecutor`, `ToolExecutorRegistry`, `ToolRegistry`,
`ToolDefinition`, the MCP client and the annotation registry), configuration
(`AgenticFlinkConfig`, `ConfigKeys`, `StorageConfiguration`), the channel SPI
(`Channel`, `KeyedContextItem`, `ChannelRegistry`, the shipped channels and sinks), the
storage and memory SPIs (`StorageProvider`, `LongTermMemoryStore`, `ShortTermMemoryStore`,
`VectorStore`, `ConversationStore`, `ShortTermMemory`, `VectorMemory` and their shipped
implementations), the chat and embedding SPIs (`ChatConnection`, `ChatClient`,
`EmbeddingConnection`), the context model (`ContextItem`, `AgentContext`,
`ContextWindowManager`), the operator base classes and the `agentic/v1` runtime entry points
(`WorkflowTurnFunction`, `FlinkRuntimeOptions`, `LocalWorkflowSession`,
`FlinkPipelineRunner`).

The legacy Flink DSL execution path (`dsl`, `execution`, `job`, `statemachine`,
`cep.CepSpecTranslator`) is `@Public` and deprecated at the same time: it is a supported
pure-Flink API outside the `agentic/v1` conformance path, it receives no new features, and
every type in it names its replacement in the Javadoc. It is not scheduled for removal.

### `@Experimental`

An `@Experimental` type can be called by users but its shape is still settling. It may change
or be removed in any minor release without a deprecation cycle. Such changes are listed in the
release notes. Once an experimental type has kept its shape for a release it is promoted to
`@Public`.

`@Experimental` covers `inference` (the traditional model SPI and guardrails), the web
toolkit (`web`, `net.OutboundUrlPolicy`), `a2a` and its bridge and storage packages,
`corpus`, `retrieve`, `ingest`, `rag` and `tools.rag`, `context.inverse`, the DJL backends,
the embedded Python executor (`python`) and the optional Flink Agents plugin
(`plugins.flintagents`).

### `@Internal`

An `@Internal` type is public in the Java sense only because another package of the framework
needs to reach it. It is not API and may change or disappear in any release, including patch
releases. It is not covered by the deprecation policy.

`@Internal` covers the Flink functions and operators that the builders wire up on the user's
behalf (`function`, `AgentPlanProcessFunction`, `ContextCompactionFunction`,
`SupervisorTierFunction`, the `stream` functions), runtime plumbing (`TurnResultCodec`,
`TurnResultTypeInfo`, `ChatClientFactories`, `LangChain4jChatClient`), helpers
(`CompileUtils`, `HnswGraph`, `MetricsWrapper`, `ZeroMqProxy`), the session job launcher
and everything under `example`.

## Deprecation policy

Deprecating a `@Public` type or member is the only way to remove it or to change its
signature. A deprecation:

1. adds `@Deprecated(since = "<version>")`, where `<version>` is the first release that ships
   the deprecation;
2. adds a Javadoc `@deprecated` tag that names the replacement (a type, a method or a
   documented procedure) and, where useful, why the old API is being retired;
3. keeps the deprecated API working for at least one minor release after `<version>`. A type
   deprecated in `1.0.0` may be removed in `1.2.0` at the earliest, never in `1.1.x`.

`@Experimental` and `@Internal` types can be changed or removed without this cycle. When an
experimental type is retired rather than promoted, the release notes say so.

At `1.0.0` every deprecated type in the framework states `since = "1.0.0"` and names its
replacement in the Javadoc. Most of them belong to the legacy Flink DSL execution path and
point at `org.agentic.flink.runtime.WorkflowTurnFunction`; `InMemoryShortTermStore` points
at `org.agentic.flink.memory.FlinkStateShortTermMemory`; `ConversationStore`, `ToolRegistry`
and `ToolDefinition` point at their counterparts in the canonical core `org.jagentic.core`.

## Adding a new public type

Pick the marker when the type is written, not later. A new type that users are expected to
call and that is ready to be supported is `@Public`; one that still needs feedback is
`@Experimental`; a function, codec or helper that only exists so another package can use it
is `@Internal`. `ApiStabilityMarkerTest` fails until the marker is present, and it fails if a
type carries two.
