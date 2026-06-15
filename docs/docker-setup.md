# Docker Setup Guide

**Quick start guide for running Agentic Flink infrastructure with Docker Compose**

## 📦 What's Included

This Docker Compose setup provides:

- **PostgreSQL 15** - Durable storage for conversations and context
- **Redis 7** - Fast caching layer for active contexts
- **Ollama** - Local LLM inference (no API keys needed!)
- **(Optional) Qdrant** - Vector database for RAG (commented out by default)

## 🚀 Quick Start

### 1. Start All Services

```bash
# Start all services in the background
docker compose up -d

# View logs
docker compose logs -f

# Check service health
docker compose ps
```

### 2. Verify Services

Wait for all services to be healthy (about 30 seconds):

```bash
# Check PostgreSQL
docker compose exec postgres pg_isready -U flink_user -d agentic_flink

# Check Redis
docker compose exec redis redis-cli -a flink_redis_password ping

# Check Ollama
curl http://localhost:11434/api/version
```

### 3. Download an LLM Model

Ollama starts without any models. Download one:

```bash
# Recommended: Small, fast model (1.9GB)
docker compose exec ollama ollama pull qwen2.5:3b

# Alternative: Larger, more capable (4.7GB)
docker compose exec ollama ollama pull qwen2.5:7b

# Alternative: Tiny model for testing (636MB)
docker compose exec ollama ollama pull qwen2.5:0.5b
```

### 4. Configure Your Application

Copy the environment template:

```bash
cp .env.example .env
```

The defaults in `.env.example` match the Docker Compose configuration, so they should work out of the box!

### 5. Verify Database Schema

Check that the schema was created:

```bash
docker compose exec postgres psql -U flink_user -d agentic_flink -c "\dt"
```

You should see tables: `conversations`, `context_items`, `messages`, `tool_executions`, `validation_results`

---

## 📊 Service Details

### PostgreSQL

- **Port:** 5432
- **Database:** `agentic_flink`
- **User:** `flink_user`
- **Password:** `flink_password` (change in `.env`)
- **Data:** Persisted in `postgres_data` volume

**Connect:**
```bash
docker compose exec postgres psql -U flink_user -d agentic_flink
```

### Redis

- **Port:** 6379
- **Password:** `flink_redis_password` (change in docker-compose.yml)
- **Persistence:** AOF enabled
- **Data:** Persisted in `redis_data` volume

**Connect:**
```bash
docker compose exec redis redis-cli -a flink_redis_password
```

**Useful commands:**
```bash
# Check keys
redis-cli -a flink_redis_password KEYS "agent:*"

# Monitor commands in real-time
redis-cli -a flink_redis_password MONITOR

# Get stats
redis-cli -a flink_redis_password INFO
```

### Ollama

- **Port:** 11434
- **API:** http://localhost:11434
- **Data:** Models stored in `ollama_data` volume

**Useful commands:**
```bash
# List downloaded models
docker compose exec ollama ollama list

# Test model
docker compose exec ollama ollama run qwen2.5:3b "Hello, how are you?"

# Remove a model
docker compose exec ollama ollama rm qwen2.5:3b
```

**Model recommendations:**
- **Development:** `qwen2.5:3b` - Good balance of speed and capability
- **Production:** `qwen2.5:7b` - Better reasoning and responses
- **Testing:** `qwen2.5:0.5b` - Fast, minimal resource usage

---

## 🔧 Common Operations

### Stop Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (DELETES ALL DATA!)
docker compose down -v
```

### Restart a Service

```bash
# Restart PostgreSQL
docker compose restart postgres

# Restart with fresh data (removes volume)
docker compose down
docker volume rm agentic-flink_postgres_data
docker compose up -d
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f postgres
docker compose logs -f redis
docker compose logs -f ollama
```

### Resource Usage

```bash
# View resource usage
docker stats

# View volumes
docker volume ls | grep agentic-flink
```

---

## 🔐 Security Notes

**For Development:**
- Default passwords are fine
- Services are bound to localhost only

**For Production:**
1. **Change all passwords** in docker-compose.yml and `.env`
2. **Use secrets management** (Docker secrets, vault, etc.)
3. **Enable TLS/SSL** for PostgreSQL and Redis
4. **Restrict network access** using Docker networks
5. **Regular backups** of PostgreSQL and Redis data
6. **Monitor resource usage** and set limits

---

## 🐛 Troubleshooting

### Port Already in Use

If you get "port already allocated" errors:

```bash
# Check what's using the port
lsof -i :5432  # PostgreSQL
lsof -i :6379  # Redis
lsof -i :11434 # Ollama

# Stop the conflicting service or change the port in docker-compose.yml
```

### Service Won't Start

```bash
# Check logs for the specific service
docker compose logs postgres

# Check if volume has permission issues
ls -la /var/lib/docker/volumes/
```

### Can't Connect from Application

1. Make sure Docker services are running: `docker compose ps`
2. Verify network: `docker network ls | grep agentic-flink`
3. Check that you're using `localhost` not `postgres` from your host machine
4. Try connecting manually:
   ```bash
   psql -h localhost -U flink_user -d agentic_flink
   redis-cli -h localhost -p 6379 -a flink_redis_password
   curl http://localhost:11434/api/version
   ```

### PostgreSQL Schema Not Created

```bash
# Check if schema file exists
ls -la sql/schema.sql

# Manually run schema
docker compose exec postgres psql -U flink_user -d agentic_flink -f /docker-entrypoint-initdb.d/01-schema.sql
```

### Ollama Model Download Fails

```bash
# Check disk space
df -h

# Try pulling directly
docker compose exec ollama ollama pull qwen2.5:3b

# Check Ollama logs
docker compose logs ollama
```

---

## 📦 Data Persistence

All data is stored in Docker volumes:

- `agentic-flink_postgres_data` - PostgreSQL database
- `agentic-flink_redis_data` - Redis cache
- `agentic-flink_ollama_data` - Ollama models

**Backup:**
```bash
# Backup PostgreSQL
docker compose exec postgres pg_dump -U flink_user agentic_flink > backup.sql

# Backup Redis
docker compose exec redis redis-cli -a flink_redis_password --rdb /data/dump.rdb
docker cp agentic-flink-redis:/data/dump.rdb ./redis-backup.rdb
```

**Restore:**
```bash
# Restore PostgreSQL
docker compose exec -T postgres psql -U flink_user -d agentic_flink < backup.sql

# Restore Redis
docker cp ./redis-backup.rdb agentic-flink-redis:/data/dump.rdb
docker compose restart redis
```

---

## 🎯 Optional: Qdrant Vector Database

If you need vector search for RAG:

1. Uncomment the `qdrant` service in `docker-compose.yml`
2. Uncomment the `qdrant_data` volume
3. Restart services:
   ```bash
   docker compose up -d
   ```

4. Access Qdrant dashboard: http://localhost:6333/dashboard

---

## 🧹 Cleanup

```bash
# Stop and remove everything
docker compose down -v

# Remove images
docker rmi postgres:15-alpine redis:7-alpine ollama/ollama:latest

# Remove all project volumes
docker volume ls | grep agentic-flink | awk '{print $2}' | xargs docker volume rm
```

---

## 📚 Next Steps

Once your infrastructure is running:

1. Build the project: `mvn clean compile`
2. Run tests: `mvn test` (once implemented)
3. Try the examples in `src/main/java/org/agentic/flink/example/`
4. Read [getting-started.md](getting-started.md) for usage guide

---

## 💡 Tips

- **Use `docker compose` not `docker-compose`** - The hyphenated version is deprecated
- **Check health status**: `docker compose ps` shows health checks
- **Resource limits**: Add memory/CPU limits in docker-compose.yml for production
- **Log rotation**: Configure Docker logging drivers for production
- **Monitoring**: Consider adding Prometheus + Grafana for metrics

---

**Questions?** Check [reference/troubleshooting.md](reference/troubleshooting.md) or open an issue!
