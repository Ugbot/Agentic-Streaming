package org.agentic.flink.langchain.client;

import org.agentic.flink.langchain.model.language.LangChainLanguageModel;
import org.agentic.flink.langchain.config.LLMConfig;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.output.Response;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LangChainAsyncClient implements Serializable {

  private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

  private final List<LangChainLanguageModel> supportedModels;

  public LangChainAsyncClient(List<LangChainLanguageModel> supportedModels) {
    this.supportedModels = supportedModels;
  }

  public CompletableFuture<Response<AiMessage>> generate(
      List<ChatMessage> chatMessages, LLMConfig llmConfig) {

    for (LangChainLanguageModel supportedModel : supportedModels) {
      if (llmConfig.getAiModel().equals(supportedModel.getName())) {
        final var langChainModel = supportedModel.getModel(llmConfig.getProperties());
        return CompletableFuture.supplyAsync(
            () -> langChainModel.generate(chatMessages), EXECUTOR_SERVICE);
      }
    }

    throw new RuntimeException(
        "LangChain language model " + llmConfig.getAiModel() + " not supported!");
  }
}
