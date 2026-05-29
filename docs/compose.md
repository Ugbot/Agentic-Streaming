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
| **Jupyter + cluster** (zero host Python)  | `docker-compose-notebook.yml`| Fluss + Flink session + Jupyter Lab in a container               | `podman compose -f docker-compose-notebook.yml up -d` |

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

## Runtime modes (notebook side)

The notebooks all bootstrap through `af.bootstrap()`, which picks one of three
modes from `AGENTIC_FLINK_MODE` in `.env`:

| Mode       | Where work runs                            | Cluster needed?                            | Use when                                |
|------------|--------------------------------------------|--------------------------------------------|-----------------------------------------|
| `inproc`   | JVM in this Python process (JPype)         | No                                         | Notebooks 07 / 08 — operator called directly, no Flink job |
| `session`  | A Flink session cluster over REST          | Yes (local podman or remote)               | Notebook 09 — multi-job session-cluster demos |
| `embedded` | JPype JVM + in-process MiniCluster         | No                                         | Offline testing of the full job graph (slower than `inproc`, no docker) |

Copy-paste-ready `.env` files at the repo root:

```bash
cp .env.inproc.example          .env    # JVM in-process, no cluster
cp .env.cluster.local.example   .env    # session mode, local cluster (run-session-cluster.sh)
cp .env.cluster.remote.example  .env    # session mode, REMOTE cluster — edit FLINK_REST_URL
cp .env.embedded.example        .env    # MiniCluster in-process
```

### Pointing a notebook at a remote cluster

```bash
cp .env.cluster.remote.example .env
# Edit:
#   FLINK_REST_URL=http://10.0.0.5:8081
#   FLUSS_BOOTSTRAP=10.0.0.5:9123
# No podman compose on this machine. The notebook's af.bootstrap() reads .env,
# resolves SessionRuntime against the remote URL, uploads the jar (one-time),
# and submission cells proceed as normal.
```

### Dockerised notebook (zero host-side Python)

```bash
bash examples-bin/setup-network.sh
podman compose -f docker-compose-notebook.yml up -d
# Open http://localhost:8888/?token=agentic
```

The notebook container shares the `agentic-flink-network` with the cluster, so
`FLINK_REST_URL=http://flink-jobmanager:8081` works automatically — no `.env`
edit needed unless you're pointing at a remote cluster instead.

## Which notebook needs what

| Notebook                                   | Needs                              |
|--------------------------------------------|------------------------------------|
| `01_quickstart.ipynb`                      | nothing (in-JVM only)              |
| `02_live_scrape_rag.ipynb`                 | RAG stack                          |
| `03_scraper_researcher_fluss.ipynb`        | RAG stack                          |
| `04_escalation_cascade.ipynb`              | nothing (Claude API only)          |
| `05_layered_screening.ipynb`               | nothing                            |
| `06_feedback_refinement.ipynb`             | nothing                            |
| `07_market_depth_agents.ipynb`             | nothing (default `inproc` mode)    |
| `08_coinbase_live_screening.ipynb`         | nothing (default `inproc` mode)    |
| `09_session_cluster_levels.ipynb`          | session cluster (`session` mode)   |
