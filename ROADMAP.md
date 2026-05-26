# Agentic Flink - Roadmap

**Last Updated:** 2025-10-22
**Current Version:** 1.0.0-SNAPSHOT (Focus: Core Working Implementation)

## What This Document Is

This roadmap captures **planned features** that are not yet fully implemented. Everything listed here is either:
- Not yet started
- Partially implemented but not production-ready
- Dependent on external projects that aren't mature yet

For what's actually working now, see [STATUS.md](STATUS.md).

---

## Current Focus (v1.x)

**Goal**: Make the core framework real and production-ready

- ✅ Core Flink CEP-based agent orchestration
- ✅ LangChain4J integration for LLM calls
- ✅ PostgreSQL + Redis two-tier storage
- ✅ Real tiered agent examples
- ✅ Comprehensive test coverage
- ✅ Production deployment documentation

---

## Future Roadmap

### Phase 1: Advanced Storage Backends (v1.1 - Q1 2026)

**Additional Backend Support:**
- DynamoDB implementation (AWS)
- Cassandra implementation (distributed)
- MongoDB implementation (document store)
- S3 for long-term archival
- Vector store integration (Qdrant, Pinecone, Weaviate)

**Why Not Now?**
- Current PG + Redis covers 80% of use cases
- Need production validation of core architecture first
- Additional complexity not justified yet

---

### Phase 2: Apache Flink Agents Integration (v2.0 - Q2 2026)

**Status**: Adapters exist, but Flink Agents is still 0.2-SNAPSHOT

**Goals:**
- Full integration with Apache Flink Agents when stable
- ReAct agent pattern support
- MCP protocol compatibility
- Workflow and autonomous agent modes
- Enhanced observability via meta-events

**What's Already Done:**
- Event adapters (bidirectional conversion)
- Tool adapters (wrapping mechanism)
- Architecture designed
- Integration documentation written

**Blockers:**
- Waiting for Flink Agents v1.0 stable release
- Need Maven Central artifacts
- Requires production testing of Flink Agents

**Migration Path:**
- Current architecture designed to support Flink Agents as plugin
- No breaking changes to existing code required
- Opt-in migration for users

---

### Phase 3: Advanced RAG & Embeddings (v1.2 - Q2 2026)

**Real Vector Embeddings:**
- Replace keyword matching in RelevancyScorer with actual embeddings
- Cosine similarity for semantic relevance
- Batch embedding operations
- Integration with vector stores

**Enhanced Document Processing:**
- Multiple document format support
- Advanced chunking strategies
- Multi-language support
- Document version management

**Why Not Now?**
- Current keyword-based relevancy works for MVP
- Requires vector store infrastructure
- Need real use cases to validate approach

---

### Phase 4: Kafka & Real-Time Streaming (v1.3 - Q3 2026)

**Kafka Integration:**
- Flink Kafka connectors
- Event serialization/deserialization
- Schema registry integration
- Exactly-once guarantees end-to-end

**Stream Processing:**
- Replace mock data sources with real Kafka streams
- Multi-topic support
- Dynamic topic routing
- Backpressure handling

**Why Not Now?**
- Focus on getting core agent logic right first
- Can test with mock sources initially
- Kafka adds operational complexity

---

### Phase 5: Production Features (v1.4 - Q4 2026)

**Observability:**
- Metrics dashboards
- Distributed tracing
- Real-time monitoring
- Alert integration

**Operations:**
- Blue/green deployment
- Canary releases
- Auto-scaling policies
- Disaster recovery procedures

**Security:**
- API key rotation
- Encryption at rest
- Audit logging
- Rate limiting

---

### Phase 6: Advanced Agent Patterns (v2.1 - Q1 2027)

**Multi-Agent Coordination:**
- Agent-to-agent communication
- Shared context management
- Consensus mechanisms
- Parallel execution patterns

**Advanced Validation:**
- LLM-based validation
- Multi-validator consensus
- Adaptive validation thresholds
- Automated correction

**Supervisor Enhancements:**
- Human-in-the-loop UI
- Escalation policies
- Approval workflows
- Manual override capabilities

---

### Phase 7: Community & Contributions (Ongoing)

**Open Source Contributions:**
- Contribute MoSCoW context management to Apache Flink Agents
- Share 5-phase compaction algorithm
- Publish validation/correction patterns
- Contribute to LangChain4J ecosystem

**Community Building:**
- Example applications
- Tutorial series
- Video guides
- Conference talks

---

## Deferred / Not Planned

**Features we explicitly decided NOT to pursue:**

1. **Custom LLM Training** - Use existing models via LangChain4J
2. **Built-in Vector Database** - Use external services (Qdrant, etc.)
3. **GUI/Web Interface** - Focus on backend framework, UIs are application-specific
4. **Multi-Cloud Abstraction** - Users can implement using storage abstraction
5. **Graph Databases** - Out of scope, users can add via storage interface

---

## How to Influence the Roadmap

**Want a feature prioritized?**

1. Open a GitHub issue describing your use case
2. Contribute a PR with implementation
3. Share your production experience
4. Help with documentation

**Active discussions welcome on:**
- Storage backend priorities
- Agent pattern improvements
- Integration opportunities
- Performance optimizations

---

## Version History

| Version | Focus | Target | Status |
|---------|-------|--------|--------|
| 1.0 | Core framework + PG/Redis + Real examples | Q4 2025 | 🚧 In Progress |
| 1.1 | Additional storage backends | Q1 2026 | 📋 Planned |
| 1.2 | Advanced RAG & embeddings | Q2 2026 | 📋 Planned |
| 1.3 | Kafka integration | Q3 2026 | 📋 Planned |
| 1.4 | Production features | Q4 2026 | 📋 Planned |
| 2.0 | Flink Agents integration | Q2 2026 | ⏳ Waiting on upstream |
| 2.1 | Advanced agent patterns | Q1 2027 | 💭 Concept |

---

**Remember**: This is an experimental research project. Timelines are estimates, not commitments. Priorities will shift based on community needs and upstream project maturity.

For what's working **right now**, see [STATUS.md](STATUS.md).
