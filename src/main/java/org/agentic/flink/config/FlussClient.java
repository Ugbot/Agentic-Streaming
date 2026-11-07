package org.agentic.flink.config;

import org.agentic.flink.annotation.Public;

/**
 * Whether the optional Fluss client is on the classpath. The default uber jar leaves it out, so
 * every Fluss entry point calls {@link #require()} before it touches a Fluss class. This class must
 * not reference one.
 */
@Public
public final class FlussClient {

  private static final String PROBE_CLASS = "com.alibaba.fluss.client.ConnectionFactory";

  /** What a job sees when it selects a Fluss backend without the client. */
  public static final String MISSING_MESSAGE =
      "The Fluss client (com.alibaba.fluss:fluss-client) is not on the classpath. The default uber"
          + " jar leaves it out: build with `./mvnw -P fluss package`, or add the client to the job"
          + " classpath.";

  private FlussClient() {}

  public static boolean isPresent() {
    try {
      Class.forName(PROBE_CLASS, false, FlussClient.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * @throws IllegalStateException with {@link #MISSING_MESSAGE} when the client is absent
   */
  public static void require() {
    if (!isPresent()) {
      throw new IllegalStateException(MISSING_MESSAGE);
    }
  }
}
