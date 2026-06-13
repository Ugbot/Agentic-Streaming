package org.agentic.flink.a2a.bridge;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agentic.flink.channel.Channel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * In-process {@link A2ABridge}: gateway and Flink job share JVM-static queues keyed by endpoint
 * name. The simplest correct transport — the natural fit for the "embedded" runtime mode (gateway +
 * Flink minicluster in one process) and for tests, with no broker or sockets.
 *
 * <p>Requests published by the gateway land in a {@link BlockingQueue} drained by the Flink request
 * source; responses written to the sink are fanned out to the gateway's registered listeners. State
 * is global to the JVM (see {@link Hub}); use distinct endpoint names to isolate concurrent bridges,
 * or {@link Hub#reset()} between tests.
 */
public final class InProcA2ABridge implements A2ABridge {
  private static final long serialVersionUID = 1L;

  private final String requestEndpoint;
  private final String responseEndpoint;

  public InProcA2ABridge(String requestEndpoint, String responseEndpoint) {
    this.requestEndpoint = requestEndpoint;
    this.responseEndpoint = responseEndpoint;
  }

  @Override
  public String transport() {
    return "inproc";
  }

  @Override
  public Channel<A2ARequest> requestChannel() {
    return new InProcRequestChannel(requestEndpoint);
  }

  @Override
  public SinkFunction<A2AResponse> responseSink() {
    return new InProcResponseSink(responseEndpoint);
  }

  @Override
  public A2AGatewayConnector openGateway() {
    return new InProcConnector(requestEndpoint, responseEndpoint);
  }

  // ==================== JVM-static hub ====================

  /** Shared in-JVM transport state. Package-visible for tests ({@link #reset()}). */
  public static final class Hub {
    private static final Map<String, BlockingQueue<A2ARequest>> REQUESTS = new ConcurrentHashMap<>();
    private static final Map<String, List<Consumer<A2AResponse>>> SUBS = new ConcurrentHashMap<>();

    private Hub() {}

    static BlockingQueue<A2ARequest> requestQueue(String endpoint) {
      return REQUESTS.computeIfAbsent(endpoint, k -> new LinkedBlockingQueue<>());
    }

    static void subscribe(String endpoint, Consumer<A2AResponse> listener) {
      SUBS.computeIfAbsent(endpoint, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    static void unsubscribe(String endpoint, Consumer<A2AResponse> listener) {
      List<Consumer<A2AResponse>> l = SUBS.get(endpoint);
      if (l != null) {
        l.remove(listener);
      }
    }

    static void deliver(String endpoint, A2AResponse response) {
      for (Consumer<A2AResponse> l : SUBS.getOrDefault(endpoint, List.of())) {
        l.accept(response);
      }
    }

    /** Clear all in-JVM bridge state. For test isolation. */
    public static void reset() {
      REQUESTS.clear();
      SUBS.clear();
    }
  }

  // ==================== Flink request source ====================

  static final class InProcRequestChannel implements Channel<A2ARequest> {
    private static final long serialVersionUID = 1L;
    private final String endpoint;

    InProcRequestChannel(String endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public DataStream<A2ARequest> open(StreamExecutionEnvironment env) {
      return env.addSource(new InProcRequestSource(endpoint), elementType())
          .name("a2a-bridge-inproc-requests");
    }

    @Override
    public TypeInformation<A2ARequest> elementType() {
      return A2AJsonTypeInfo.of(A2ARequest.class);
    }

    @Override
    public String providerName() {
      return "a2a-inproc";
    }
  }

  static final class InProcRequestSource implements SourceFunction<A2ARequest> {
    private static final long serialVersionUID = 1L;
    private final String endpoint;
    private volatile boolean running = true;

    InProcRequestSource(String endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public void run(SourceContext<A2ARequest> ctx) throws Exception {
      BlockingQueue<A2ARequest> queue = Hub.requestQueue(endpoint);
      while (running) {
        A2ARequest req = queue.poll(200, TimeUnit.MILLISECONDS);
        if (req != null) {
          synchronized (ctx.getCheckpointLock()) {
            ctx.collect(req);
          }
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  // ==================== Flink response sink ====================

  static final class InProcResponseSink implements SinkFunction<A2AResponse> {
    private static final long serialVersionUID = 1L;
    private final String endpoint;

    InProcResponseSink(String endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public void invoke(A2AResponse value, Context context) {
      Hub.deliver(endpoint, value);
    }
  }

  // ==================== Gateway connector ====================

  static final class InProcConnector extends AbstractA2AGatewayConnector {
    private final String requestEndpoint;
    private final String responseEndpoint;
    private final Consumer<A2AResponse> dispatcher = this::deliver;

    InProcConnector(String requestEndpoint, String responseEndpoint) {
      this.requestEndpoint = requestEndpoint;
      this.responseEndpoint = responseEndpoint;
      Hub.subscribe(responseEndpoint, dispatcher);
    }

    @Override
    public void publishRequest(A2ARequest request) {
      Hub.requestQueue(requestEndpoint).offer(request);
    }

    @Override
    public void close() {
      Hub.unsubscribe(responseEndpoint, dispatcher);
      clearListeners();
    }
  }
}
