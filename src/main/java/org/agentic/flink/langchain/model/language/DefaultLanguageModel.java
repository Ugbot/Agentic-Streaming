package org.agentic.flink.langchain.model.language;

import org.agentic.flink.langchain.model.AiModel;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import java.util.List;
import java.util.Map;

public class DefaultLanguageModel implements LangChainLanguageModel {

  @Override
  public ChatLanguageModel getModel(final Map<String, String> properties) {
    return new ChatLanguageModel() {
      @Override
      public Response<AiMessage> generate(List<ChatMessage> list) {
        final var lastMessage = list.get(list.size() - 1);
        final var aiMessage =
            AiMessage.from(
                "You shared this message with the AI model: "
                    + ((UserMessage) lastMessage).singleText());

        return Response.from(aiMessage);
      }
    };
  }

  @Override
  public AiModel getName() {
    return AiModel.DEFAULT;
  }
}
