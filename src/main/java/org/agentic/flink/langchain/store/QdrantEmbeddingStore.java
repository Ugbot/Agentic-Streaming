package org.agentic.flink.langchain.store;

import dev.langchain4j.data.segment.TextSegment;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class QdrantEmbeddingStore implements LangChainEmbeddingStore {

  @Override
  public dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> getStore(
      final Map<String, String> properties) {

    final var host = properties.get("host");
    final int port = Integer.parseInt(properties.get("port"));
    final var apiKey = properties.get("apiKey");
    final var collectionName = properties.get("collectionName");

    // Create embeddings collection
    createEmbeddingCollection(host, port, apiKey, collectionName);

    return dev.langchain4j.store.embedding.qdrant.QdrantEmbeddingStore.builder()
        .host(host)
        .port(port)
        .useTls(true)
        .collectionName(collectionName)
        .apiKey(apiKey)
        .build();
  }

  @Override
  public EmbeddingStore getName() {
    return EmbeddingStore.QDRANT;
  }

  private void createEmbeddingCollection(
      final String host, final int port, final String apiKey, final String collectionName) {
    try {
      QdrantClient client =
          new QdrantClient(
              QdrantGrpcClient.newBuilder(host, port, true).withApiKey(apiKey).build());

      final var collectionExistsResponse = client.collectionExistsAsync(collectionName).get();

      if (!collectionExistsResponse) {
        client
            .createCollectionAsync(
                collectionName,
                Collections.VectorParams.newBuilder()
                    .setDistance(Collections.Distance.Cosine)
                    .setSize(768)
                    .build())
            .get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
