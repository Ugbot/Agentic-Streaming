package org.agentic.flink.llm.langchain4j;

import org.agentic.flink.llm.ChatClient;
import dev.langchain4j.model.chat.ChatLanguageModel;

/**
 * Escape hatch interface implemented by {@code ChatClient}s backed by LangChain4J.
 *
 * <p>The public framework API never accepts or returns {@code dev.langchain4j.*} types — but
 * advanced users who want to reach into LangChain4J specifics (custom output parsers, structured
 * tool definitions, framework-level streaming behaviour) can do an explicit downcast:
 *
 * <pre>{@code
 * ChatClient client = connection.bind(ctx);
 * if (client instanceof LangChain4jChatClient lc) {
 *     ChatLanguageModel raw = lc.getUnderlyingModel();
 *     // ... LangChain4J-specific code here ...
 * }
 * }</pre>
 *
 * <p>Implementations of {@link ChatClient} may but are not required to provide this accessor.
 * Code that casts to this interface accepts the coupling.
 */
public interface LangChain4jChatClient extends ChatClient {

  /** The underlying LangChain4J chat model for the most recently invoked setup. */
  ChatLanguageModel getUnderlyingModel();
}
