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

  // Redis
  public static final String REDIS_HOST = "redis.host";
  public static final String REDIS_PORT = "redis.port";
  public static final String REDIS_PASSWORD = "redis.password";
  public static final String DEFAULT_REDIS_HOST = "localhost";
  public static final String DEFAULT_REDIS_PORT = "6379";

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
  public static final String DEFAULT_OPENAI_MODEL = "gpt-4";
}
