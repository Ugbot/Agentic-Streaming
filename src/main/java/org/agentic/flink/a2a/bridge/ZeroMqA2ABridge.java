package org.agentic.flink.a2a.bridge;

import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.ZeroMqChannel;
import org.agentic.flink.channel.ZeroMqSink;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * ZeroMQ {@link A2ABridge} — the default distributed/localhost transport. No broker required.
 *
 * <p>Flink binds a {@code PULL} socket for requests ({@link ZeroMqChannel#pull}) and {@code
 * PUSH}es responses ({@link ZeroMqSink#push}); the gateway connects a {@code PUSH} for requests and
 * binds a {@code PULL} for responses. Envelopes are JSON via {@link A2AWireSerde}/{@link A2AJson}.
 */
public final class ZeroMqA2ABridge implements A2ABridge {
  private static final long serialVersionUID = 1L;

  private final String requestEndpoint;
  private final String responseEndpoint;

  public ZeroMqA2ABridge(String requestEndpoint, String responseEndpoint) {
    this.requestEndpoint = requestEndpoint;
    this.responseEndpoint = responseEndpoint;
  }

  @Override
  public String transport() {
    return "zeromq";
  }

  @Override
  public Channel<A2ARequest> requestChannel() {
    return ZeroMqChannel.builder(ZeroMqChannel.Pattern.PULL, requestEndpoint, A2ARequest.class)
        .deserializer(A2AWireSerde.deserializer(A2ARequest.class))
        .build();
  }

  @Override
  public SinkFunction<A2AResponse> responseSink() {
    return ZeroMqSink.<A2AResponse>builder(ZeroMqSink.Pattern.PUSH, responseEndpoint)
        .serializer(A2AWireSerde.serializer())
        .build();
  }

  @Override
  public A2AGatewayConnector openGateway() {
    return new ZeroMqConnector(requestEndpoint, responseEndpoint);
  }

  /** Gateway side: PUSH→requests (connect), PULL→responses (bind, drained on a daemon thread). */
  static final class ZeroMqConnector extends AbstractA2AGatewayConnector {
    private static final Logger LOG = LoggerFactory.getLogger(ZeroMqConnector.class);

    private final ZContext context;
    private final ZMQ.Socket pushRequests;
    private final ZMQ.Socket pullResponses;
    private final Thread receiver;
    private volatile boolean running = true;

    ZeroMqConnector(String requestEndpoint, String responseEndpoint) {
      this.context = new ZContext();
      this.pushRequests = context.createSocket(SocketType.PUSH);
      this.pushRequests.connect(requestEndpoint);
      this.pullResponses = context.createSocket(SocketType.PULL);
      this.pullResponses.setReceiveTimeOut(200);
      this.pullResponses.bind(responseEndpoint);
      this.receiver = new Thread(this::receiveLoop, "a2a-zmq-responses");
      this.receiver.setDaemon(true);
      this.receiver.start();
    }

    private void receiveLoop() {
      while (running) {
        byte[] frame;
        try {
          frame = pullResponses.recv(0);
        } catch (RuntimeException e) {
          if (running) {
            LOG.debug("ZMQ response recv error: {}", e.getMessage());
          }
          return;
        }
        if (frame == null) {
          continue; // receive timeout
        }
        try {
          deliver(A2AJson.mapper().readValue(frame, A2AResponse.class));
        } catch (Exception e) {
          LOG.warn("Dropping malformed A2A response frame: {}", e.getMessage());
        }
      }
    }

    @Override
    public synchronized void publishRequest(A2ARequest request) throws Exception {
      pushRequests.send(A2AJson.mapper().writeValueAsBytes(request), 0);
    }

    @Override
    public void close() {
      running = false;
      try {
        receiver.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      context.close();
      clearListeners();
    }
  }
}
