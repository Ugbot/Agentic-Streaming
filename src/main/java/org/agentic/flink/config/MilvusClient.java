package org.agentic.flink.config;

import org.agentic.flink.annotation.Public;

/**
 * Whether the optional Milvus SDK is on the classpath. The default uber jar leaves it out, so the
 * Milvus entry points call {@link #require()} before they touch a Milvus class. This class must not
 * reference one.
 */
@Public
public final class MilvusClient {

  private static final String PROBE_CLASS = "io.milvus.v2.client.MilvusClientV2";

  /** What a job sees when it selects the Milvus backend without the SDK. */
  public static final String MISSING_MESSAGE =
      "The Milvus SDK (io.milvus:milvus-sdk-java) is not on the classpath. The default uber jar"
          + " leaves it out: build with `./mvnw -P milvus package`, or add the SDK to the job"
          + " classpath.";

  private MilvusClient() {}

  public static boolean isPresent() {
    try {
      Class.forName(PROBE_CLASS, false, MilvusClient.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * @throws IllegalStateException with {@link #MISSING_MESSAGE} when the SDK is absent
   */
  public static void require() {
    if (!isPresent()) {
      throw new IllegalStateException(MISSING_MESSAGE);
    }
  }
}
