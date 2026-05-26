package org.agentic.flink.langchain.model.embedding;

import org.agentic.flink.langchain.model.AiModel;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultEmbeddingModel implements LangChainEmbeddingModel {

  @Override
  public EmbeddingModel getModel(final Map<String, String> properties) {
    return new EmbeddingModel() {

      @Override
      public Response<List<Embedding>> embedAll(final List<TextSegment> textSegments) {
        List<Embedding> result = new ArrayList<>();

        textSegments.forEach(
            textSegment -> {
              result.add(Embedding.from(generateEmbedding(textSegment.text())));
            });

        return Response.from(result);
      }
    };
  }

  @Override
  public AiModel getName() {
    return AiModel.DEFAULT;
  }

  public static List<Float> generateEmbedding(String input) {
    List<Float> embedding = new ArrayList<>();
    try {
      // Hash the input using SHA-256
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      // Convert the hash bytes to a floating-point vector of the desired size
      for (int i = 0; i < 768; i++) {
        int byteIndex = i % hash.length;
        float value = (hash[byteIndex] & 0xFF) / 255.0f; // Normalize byte to [0, 1]
        embedding.add(value);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return embedding;
  }
}
