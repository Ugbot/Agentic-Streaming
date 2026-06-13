package org.agentic.flink.config;

/**
 * Constants class defining all configuration keys and their default values
 * for the Agentic Flink framework.
 *
 * <p>Configuration keys follow a dot-separated naming convention (e.g., {@code ollama.base.url}).
 * When resolved via environment variables, keys are transformed by uppercasing, replacing dots
 * with underscores, and prepending {@code AGENTIC_FLINK_} (e.g., {@code AGENTIC_FLINK_OLLAMA_BASE_URL}).
 *
 * @author Agentic Flink Team
 * @see AgenticFlinkConfig
 */
public final class ConfigKeys {

  private ConfigKeys() {}

  // Ollama
  public static final String OLLAMA_BASE_URL = "ollama.base.url";
  public static final String OLLAMA_MODEL = "ollama.model";
  public static final String DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434";
  public static final String DEFAULT_OLLAMA_MODEL = "qwen2.5:3b";
  public static final String DEFAULT_OLLAMA_EMBED_MODEL = "nomic-embed-text";
  public static final int DEFAULT_OLLAMA_EMBED_DIMENSION = 768;

  // Anthropic (Claude)
  public static final String ANTHROPIC_API_KEY = "anthropic.api.key";
  public static final String ANTHROPIC_MODEL = "anthropic.model";
  public static final String DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-6";

  // Redis
  public static final String REDIS_HOST = "redis.host";
  public static final String REDIS_PORT = "redis.port";
  public static final String REDIS_PASSWORD = "redis.password";
  public static final String DEFAULT_REDIS_HOST = "localhost";
  public static final String DEFAULT_REDIS_PORT = "6379";

  // Per-conversation memory (ConversationStore) backend selection.
  // "memory" (default) = in-JVM; "redis" = RedisConversationStore (cross-process state spine).
  public static final String CONVERSATION_STORE = "conversation.store";
  public static final String DEFAULT_CONVERSATION_STORE = "memory";
  public static final String CONVERSATION_STORE_TTL_SECONDS = "conversation.store.ttl.seconds";
  public static final String DEFAULT_CONVERSATION_STORE_TTL_SECONDS = "86400"; // 24h
  public static final String CONVERSATION_STORE_MAX_MESSAGES = "conversation.store.max.messages";
  public static final String DEFAULT_CONVERSATION_STORE_MAX_MESSAGES = "200";

  // PostgreSQL
  public static final String POSTGRES_URL = "postgres.url";
  public static final String POSTGRES_USER = "postgres.user";
  public static final String POSTGRES_PASSWORD = "postgres.password";
  public static final String DEFAULT_POSTGRES_URL = "jdbc:postgresql://localhost:5432/agentic_flink";
  public static final String DEFAULT_POSTGRES_USER = "flink_user";
  public static final String DEFAULT_POSTGRES_PASSWORD = "flink_password";

  // Qdrant
  public static final String QDRANT_HOST = "qdrant.host";
  public static final String QDRANT_PORT = "qdrant.port";
  public static final String DEFAULT_QDRANT_HOST = "localhost";
  public static final String DEFAULT_QDRANT_PORT = "6333";

  // Memory (Flink-state-first)
  public static final String MEMORY_SHORTTERM_TTL_SECONDS = "memory.shortterm.ttl.seconds";
  public static final String MEMORY_SHORTTERM_MAX_ITEMS = "memory.shortterm.max.items";
  public static final String MEMORY_VECTOR_DIMENSION = "memory.vector.dimension";
  public static final String MEMORY_VECTOR_MAX_ITEMS = "memory.vector.max.items";
  public static final String MEMORY_VECTOR_M = "memory.vector.m";
  public static final String MEMORY_VECTOR_BEAM_WIDTH = "memory.vector.beam.width";
  public static final long DEFAULT_MEMORY_SHORTTERM_TTL_SECONDS = 3600L;
  public static final int DEFAULT_MEMORY_SHORTTERM_MAX_ITEMS = 50;
  public static final int DEFAULT_MEMORY_VECTOR_DIMENSION = 768;
  public static final int DEFAULT_MEMORY_VECTOR_MAX_ITEMS = 10_000;
  public static final int DEFAULT_MEMORY_VECTOR_M = 16;
  public static final int DEFAULT_MEMORY_VECTOR_BEAM_WIDTH = 100;

  // OpenAI (optional)
  public static final String OPENAI_API_KEY = "openai.api.key";
  public static final String OPENAI_MODEL = "openai.model";
  public static final String DEFAULT_OPENAI_MODEL = "gpt-5.4-mini";

  // A2A (Agent2Agent) — outbound client + inbound Quarkus gateway. See docs/a2a.md.
  public static final String A2A_PROTOCOL_VERSION = "a2a.protocol.version";
  public static final String DEFAULT_A2A_PROTOCOL_VERSION = "1.0";
  public static final String A2A_CLIENT_DEFAULT_TRANSPORT = "a2a.client.default.transport";
  public static final String DEFAULT_A2A_CLIENT_TRANSPORT = "JSONRPC";

  // A2A gateway (Quarkus): each binding listens on its own port.
  public static final String A2A_GATEWAY_ENABLED = "a2a.gateway.enabled";
  public static final String A2A_GATEWAY_HOST = "a2a.gateway.host";
  public static final String A2A_GATEWAY_JSONRPC_PORT = "a2a.gateway.jsonrpc.port";
  public static final String A2A_GATEWAY_GRPC_PORT = "a2a.gateway.grpc.port";
  public static final String A2A_GATEWAY_REST_PORT = "a2a.gateway.rest.port";
  public static final String A2A_GATEWAY_PUBLIC_URL = "a2a.gateway.public.url";
  public static final boolean DEFAULT_A2A_GATEWAY_ENABLED = false;
  public static final String DEFAULT_A2A_GATEWAY_HOST = "0.0.0.0";
  public static final int DEFAULT_A2A_GATEWAY_JSONRPC_PORT = 9999;
  public static final int DEFAULT_A2A_GATEWAY_GRPC_PORT = 9998;
  public static final int DEFAULT_A2A_GATEWAY_REST_PORT = 9997;

  // A2A bridge (gateway <-> Flink job transport). Default ZeroMQ for localhost/in-host.
  public static final String A2A_BRIDGE_TRANSPORT = "a2a.bridge.transport";
  public static final String A2A_BRIDGE_REQUEST_ENDPOINT = "a2a.bridge.request.endpoint";
  public static final String A2A_BRIDGE_RESPONSE_ENDPOINT = "a2a.bridge.response.endpoint";
  public static final String DEFAULT_A2A_BRIDGE_TRANSPORT = "zeromq";
  public static final String DEFAULT_A2A_BRIDGE_REQUEST_ENDPOINT = "tcp://127.0.0.1:5760";
  public static final String DEFAULT_A2A_BRIDGE_RESPONSE_ENDPOINT = "tcp://127.0.0.1:5761";

  // A2A task store (gateway-side task lifecycle persistence).
  public static final String A2A_TASK_STORE = "a2a.task.store";
  public static final String DEFAULT_A2A_TASK_STORE = "memory";
}
