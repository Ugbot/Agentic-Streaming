package org.agentic.flink.langchain.model.language;

import org.agentic.flink.langchain.model.AiModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import java.io.Serializable;
import java.util.Map;

public interface LangChainLanguageModel extends Serializable {

  LangChainLanguageModel DEFAULT_MODEL = new DefaultLanguageModel();

  ChatLanguageModel getModel(Map<String, String> properties);

  AiModel getName();
}
