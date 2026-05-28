# Docker Compose scenarios

Every scenario uses the same external podman network `agentic-flink-network`.
Create it once with `bash examples-bin/setup-network.sh`, then layer on the
scenario-specific compose. The "umbrella" composes use Compose's `include`
directive to pull in the per-service files — no duplication.

| Scenario                                  | Compose file                 | Services                                                         | Helper                                  |
|-------------------------------------------|------------------------------|------------------------------------------------------------------|-----------------------------------------|
| Base infra (LLM + KV + DB)                | `docker-compose.yml`         | Postgres, Redis, Ollama                                          | —                                       |
| Kafka broker                              | `docker-compose-kafka.yml`   | Kafka, ZooKeeper                                                 | —                                       |
| Fluss cluster                             | `docker-compose-fluss.yml`   | Fluss coordinator, Fluss tablet, ZooKeeper                       | —                                       |
| Flink session cluster                     | `docker-compose-session.yml` | Flink JobManager (REST :8081), TaskManager (16 slots)            | —                                       |
| **RAG + Fluss** (notebooks 02 / 03)       | `docker-compose-rag.yml`     | base + Fluss                                                     | `examples-bin/run-rag-stack.sh`         |
| **Session cluster** (notebook 09)         | `docker-compose-cluster.yml` | Fluss + Flink session                                            | `examples-bin/run-session-cluster.sh`   |
| **Markets via Kafka** (`run-*-market.sh`) | `docker-compose-markets.yml` | Kafka + Flink session                                            | `examples-bin/run-crypto-market.sh`     |
| **Everything**                            | `docker-compose-all.yml`     | All of the above                                                 | —                                       |

## Ports the host sees

| Port  | Service                                            |
|-------|----------------------------------------------------|
| 5432  | Postgres                                           |
| 6379  | Redis                                              |
| 8081  | Flink REST                                         |
| 9092  | Kafka                                              |
| 9123  | Fluss coordinator (client bootstrap)               |
| 9124  | Fluss tablet                                       |
| 11434 | Ollama                                             |
| 5557  | L5 alerts PUB (notebook 09 live tail)              |
| 5558  | L5 debug PUB                                       |
| 5559  | L5 control PULL (DebugFlipper push target)         |
| 5560–5564 | ZeroMQ chain hops L0→L5                        |

## Common commands

```bash
# Create the network (idempotent)
bash examples-bin/setup-network.sh

# Bring up just the session cluster + Fluss
bash examples-bin/run-session-cluster.sh

# Bring up the RAG stack (Ollama + Fluss + Postgres + Redis)
bash examples-bin/run-rag-stack.sh

# Bring up everything (~5GB pulled on first run)
podman compose -f docker-compose-all.yml up -d

# Tear everything down (preserves the network)
bash examples-bin/down-all.sh
```

## Which notebook needs what

| Notebook                                   | Needs                              |
|--------------------------------------------|------------------------------------|
| `01_quickstart.ipynb`                      | nothing (in-JVM only)              |
| `02_live_scrape_rag.ipynb`                 | RAG stack                          |
| `03_scraper_researcher_fluss.ipynb`        | RAG stack                          |
| `04_escalation_cascade.ipynb`              | nothing (Claude API only)          |
| `05_layered_screening.ipynb`               | nothing                            |
| `06_feedback_refinement.ipynb`             | nothing                            |
| `07_market_depth_agents.ipynb`             | nothing (in-JVM)                   |
| `08_coinbase_live_screening.ipynb`         | nothing (in-JVM)                   |
| `09_session_cluster_levels.ipynb`          | session cluster                    |
