# The Agentic Streaming spec

One workflow definition, several runtimes, the same observable behavior. This directory
holds the parts of that promise that are not written in any one language.

| Path | What it is |
|---|---|
| `v1/primitives.md` | the normative vocabulary: nouns, verbs, ordering, idempotency, errors, capability terms |
| `v1/workflow.schema.json` | the portable workflow IR, normative for YAML, JSON, and EDN |
| `v1/result.schema.json` | the normalized turn result every runtime emits for comparison |
| `conformance/v1/fixture.schema.json` | the fixture format |
| `conformance/v1/workflows/` | shared workflow documents used by fixtures |
| `conformance/v1/fixtures/` | the fixtures themselves |
| `tools/reference_runtime.py` | the executable reference semantics, the oracle for the fixtures |
| `tools/run_conformance.py` | fixture runner and the comparator other bindings reuse |
| `tools/validate_spec.py` | schema validation for workflow documents and fixtures |

## Running it

```
pip install jsonschema pyyaml pytest
python spec/tools/validate_spec.py        # validates spec/ and examples/pipelines/
python spec/tools/run_conformance.py      # runs all fixtures on the reference runtime
pytest spec/tools/test_conformance.py     # both, as tests
```

## One document, three syntaxes

The IR is a data structure, not a file format. YAML is the authoring default, JSON is the
wire and schema format, EDN is the Clojure reading of the same data. The mapping is
mechanical and lossless:

| IR | YAML | JSON | EDN |
|---|---|---|---|
| key | `spec_version` | `"spec_version"` | `:spec-version` |
| map | block mapping | object | map |
| sequence | block sequence | array | vector |
| null | `null` | `null` | `nil` |

EDN keys are kebab-case keywords; a reader converts `:spec-version` to `spec_version`
before validation, so one JSON Schema governs all three. Round-tripping any document
through all three syntaxes must produce the same structure.

## Versioning

- Every document declares `spec_version: agentic/v1`. Documents written before the spec
  existed omit it and are read as v1.
- Minor versions only add optional fields. A runtime must accept a document that omits
  them and must apply the documented defaults.
- Unknown keys are rejected everywhere except the extension points: any key prefixed
  `x-`, and the `runtime:` block keyed by runtime name. A runtime ignores `runtime:`
  blocks that are not its own.
- Removing a field, renaming it, or changing its meaning requires `agentic/v2`. A runtime
  that is asked to load a newer major version fails with a `validation` error rather than
  parsing what it recognises.
- The reference runtime, the schemas, and the fixtures version together. A fixture change
  that alters expected behavior is a spec change and needs review from the JVM, Clojure,
  and Python owners.

## Validation rules beyond the schema

The schema cannot express these; every loader must enforce them:

1. Every path named in `agent.router.rules` and `agent.router.default` exists in `agent.paths`.
2. Every tool id named in a path's `tools`, `tool_triggers`, `skills`, or a saga step is
   registered in `tools`, `mcp`, or `a2a`.
3. Tool ids are unique across `tools`, `mcp`-derived tools, and `a2a` peers.
4. A path with `brain: llm` requires an `llm` block.
5. `retrieval.dim` and `embeddings.dim` agree when both are present.
6. A saga step's `compensate_with` names a registered tool.
7. A configured store that cannot be reached fails the build unless it sets
   `on_unavailable: degrade`. No loader silently substitutes an in-memory store.

Failing any of these is a `validation` error, reported with the offending key path.
