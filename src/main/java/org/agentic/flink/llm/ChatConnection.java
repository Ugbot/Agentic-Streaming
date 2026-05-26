package org.agentic.flink.llm;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Serializable spec for a chat provider transport.
 *
 * <p>Mirrors the connection/setup split made canonical upstream by Apache Flink Agents
 * ({@code BaseChatModelConnection} / {@code BaseChatModelSetup}). One {@code ChatConnection}
 * represents a vendor deployment (HTTP base URL, credentials, retry policy) and is reusable
 * across many agents; the per-agent shape (model name, temperature, response format) lives in
 * {@link ChatSetup} and is supplied at each {@link ChatClient#chat} call.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}. The default implementation is
 * {@code LangChain4jChatConnection}, which delegates to the existing LangChain4J integration.
 */
public interface ChatConnection extends Serializable {

  /** Construct the operator-scoped client. Called once per task in {@code RichFunction.open()}. */
  ChatClient bind(RuntimeContext runtimeContext) throws Exception;

  /** Human-readable provider name for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}
