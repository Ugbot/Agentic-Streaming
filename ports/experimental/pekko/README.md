# ports/experimental/pekko: pointer to `agentic-pekko/`

> Status: this directory holds no code. The original Pekko proof-of-concept that lived at
> `ports/pekko/` (in-memory actor state, no persistence, a `LocalDemo` and one test) was
> superseded by the top-level [`agentic-pekko/`](../../../agentic-pekko/) module and has been
> deleted. `agentic-pekko/` is the Pekko runtime of record: it is conformance tested against
> the `agentic/v1` fixtures as the `pekko` binding in the generated
> [`docs/capabilities.md`](../../../docs/capabilities.md), and nothing else in this repository
> maintains a second Pekko path.

## Where to go

- Runtime code and tests: [`agentic-pekko/`](../../../agentic-pekko/) (`./mvnw -f agentic-pekko/pom.xml test`).
- Design note: [`docs/portability/pekko.md`](../../../docs/portability/pekko.md).
- Running any spec on the actor runtime: see the "Other first-class runtimes" section of
  the root [`CLAUDE.md`](../../../CLAUDE.md).

## What the deleted proof-of-concept contained

For readers following an old link: `ConversationActor.java` (a typed actor per
`conversationId` running `Banking.buildGraph().handle(...)` over private fields),
`BankingSharding.java` (Cluster Sharding wiring), `LocalDemo.java` (single-node demo) and
`ConversationActorTest.java`. It kept state only in actor memory and did not use Pekko
Persistence, so it never delivered the durability that `agentic-pekko/` does. The last
revision with the sources is in git history under `ports/pekko/`.
