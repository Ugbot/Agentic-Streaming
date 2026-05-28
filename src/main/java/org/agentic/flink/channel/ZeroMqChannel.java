package org.agentic.flink.channel;

import java.util.Objects;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * ZeroMQ-backed {@link Channel} for in-JVM / localhost job-to-job chaining without a Kafka or
 * Fluss topic in the middle.
 *
 * <p>Exposes static factories for every common ZMQ source pattern:
 *
 * <ul>
 *   <li>{@link #pull(String, Class)} — {@code PULL} socket, point-to-point, load-balanced fan-in
 *       from N {@code PUSH}-ers. <b>Default for L→L pipeline chaining</b> — no event loss while
 *       both ends are alive.
 *   <li>{@link #sub(String, Class, String)} — {@code SUB} socket, broadcast fan-out with topic
 *       prefix filter. Late subscribers miss messages.
 *   <li>{@link #xsub(String, Class)} — {@code XSUB} subscriber side of a broker (subscribe-all).
 *   <li>{@link #router(String, Class)} — {@code ROUTER} server side of an async req/rep
 *       (identity-framed). The payload is read from the last frame.
 *   <li>{@link #dealer(String, Class)} — {@code DEALER} client side of an async req/rep.
 * </ul>
 *
 * <p>Bind-vs-connect defaults to the "server" side of each pattern (PULL/ROUTER bind; SUB/XSUB/
 * DEALER connect) but can be overridden via {@link Builder#bind(boolean)}.
 *
 * <p>Wire format: JSON by default (via {@link KafkaChannel.JsonSchema}). Supply a custom
 * {@link DeserializationSchema} for binary / Avro / Protobuf transports.
 *
 * <p>Single-parallelism by design: a ZMQ socket per Flink subtask would round-robin (PULL) or
 * duplicate (SUB) the stream in ways the caller almost certainly doesn't expect.
 */
public final class ZeroMqChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 1L;

  /** ZeroMQ socket pattern for the source side. */
  public enum Pattern {
    PULL,
    SUB,
    XSUB,
    ROUTER,
    DEALER
  }

  private final Pattern pattern;
  private final String endpoint;
  private final boolean bind;
  private final String subscribePrefix;
  private final int hwm;
  private final int linger;
  private final int receiveTimeoutMs;
  private final TypeInformation<T> typeInfo;
  private final DeserializationSchema<T> deserializer;

  private ZeroMqChannel(Builder<T> b) {
    this.pattern = b.pattern;
    this.endpoint = Objects.requireNonNull(b.endpoint, "endpoint");
    this.bind = b.bind;
    this.subscribePrefix = b.subscribePrefix == null ? "" : b.subscribePrefix;
    this.hwm = b.hwm;
    this.linger = b.linger;
    this.receiveTimeoutMs = b.receiveTimeoutMs;
    this.typeInfo = Objects.requireNonNull(b.typeInfo, "typeInfo");
    this.deserializer =
        b.deserializer != null ? b.deserializer : new KafkaChannel.JsonSchema<>(b.type, b.typeInfo);
  }

  /** {@code PULL} socket — point-to-point, load-balanced fan-in. Source binds by default. */
  public static <T> ZeroMqChannel<T> pull(String endpoint, Class<T> type) {
    return builder(Pattern.PULL, endpoint, type).build();
  }

  /** {@code SUB} socket — broadcast fan-out with topic prefix filter. Source connects by default. */
  public static <T> ZeroMqChannel<T> sub(String endpoint, Class<T> type, String topicPrefix) {
    return builder(Pattern.SUB, endpoint, type).subscribe(topicPrefix).build();
  }

  /** {@code XSUB} — subscriber side of a broker; auto-sends a subscribe-all frame at start. */
  public static <T> ZeroMqChannel<T> xsub(String endpoint, Class<T> type) {
    return builder(Pattern.XSUB, endpoint, type).build();
  }

  /** {@code ROUTER} — async req/rep server. Reads the last frame as payload. */
  public static <T> ZeroMqChannel<T> router(String endpoint, Class<T> type) {
    return builder(Pattern.ROUTER, endpoint, type).build();
  }

  /** {@code DEALER} — async req/rep client; reads payloads from a connected ROUTER. */
  public static <T> ZeroMqChannel<T> dealer(String endpoint, Class<T> type) {
    return builder(Pattern.DEALER, endpoint, type).build();
  }

  /** Full builder if you need non-default bind/HWM/etc. */
  public static <T> Builder<T> builder(Pattern pattern, String endpoint, Class<T> type) {
    return new Builder<>(pattern, endpoint, type);
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) {
    return env.addSource(
            new Source<>(
                pattern,
                endpoint,
                bind,
                subscribePrefix,
                hwm,
                linger,
                receiveTimeoutMs,
                deserializer),
            typeInfo)
        .name("zeromq-" + pattern.name().toLowerCase() + "[" + endpoint + "]")
        .setParallelism(1);
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "zeromq-" + pattern.name().toLowerCase();
  }

  /** Builder for {@link ZeroMqChannel}. */
  public static final class Builder<T> {
    private final Pattern pattern;
    private final String endpoint;
    private final Class<T> type;
    private TypeInformation<T> typeInfo;
    private boolean bind;
    private String subscribePrefix = "";
    private int hwm = 1000;
    private int linger = 0;
    private int receiveTimeoutMs = 1000;
    private DeserializationSchema<T> deserializer;

    Builder(Pattern pattern, String endpoint, Class<T> type) {
      this.pattern = pattern;
      this.endpoint = endpoint;
      this.type = type;
      this.typeInfo = TypeInformation.of(type);
      // Defaults: bind for "server" patterns, connect for "client" patterns.
      this.bind = pattern == Pattern.PULL || pattern == Pattern.ROUTER;
    }

    public Builder<T> typeInfo(TypeInformation<T> typeInfo) {
      this.typeInfo = typeInfo;
      return this;
    }

    public Builder<T> bind(boolean bind) {
      this.bind = bind;
      return this;
    }

    public Builder<T> subscribe(String topicPrefix) {
      this.subscribePrefix = topicPrefix == null ? "" : topicPrefix;
      return this;
    }

    public Builder<T> hwm(int hwm) {
      this.hwm = hwm;
      return this;
    }

    public Builder<T> linger(int linger) {
      this.linger = linger;
      return this;
    }

    public Builder<T> receiveTimeoutMs(int ms) {
      this.receiveTimeoutMs = ms;
      return this;
    }

    public Builder<T> deserializer(DeserializationSchema<T> d) {
      this.deserializer = d;
      return this;
    }

    public ZeroMqChannel<T> build() {
      return new ZeroMqChannel<>(this);
    }
  }

  static final class Source<T> implements SourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final Pattern pattern;
    private final String endpoint;
    private final boolean bind;
    private final String subscribePrefix;
    private final int hwm;
    private final int linger;
    private final int receiveTimeoutMs;
    private final DeserializationSchema<T> deserializer;

    private volatile boolean running = true;

    Source(
        Pattern pattern,
        String endpoint,
        boolean bind,
        String subscribePrefix,
        int hwm,
        int linger,
        int receiveTimeoutMs,
        DeserializationSchema<T> deserializer) {
      this.pattern = pattern;
      this.endpoint = endpoint;
      this.bind = bind;
      this.subscribePrefix = subscribePrefix;
      this.hwm = hwm;
      this.linger = linger;
      this.receiveTimeoutMs = receiveTimeoutMs;
      this.deserializer = deserializer;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
      try (ZContext zc = new ZContext()) {
        ZMQ.Socket sock = zc.createSocket(toSocketType(pattern));
        sock.setRcvHWM(hwm);
        sock.setLinger(linger);
        sock.setReceiveTimeOut(receiveTimeoutMs);
        if (bind) {
          sock.bind(endpoint);
        } else {
          sock.connect(endpoint);
        }
        if (pattern == Pattern.SUB) {
          sock.subscribe(subscribePrefix.getBytes(ZMQ.CHARSET));
        }
        if (pattern == Pattern.XSUB) {
          // Subscribe-all so XSUB downstream of a broker sees everything.
          sock.send(new byte[] {1}, 0);
        }
        LOG.info(
            "zeromq source open pattern={} endpoint={} bind={} sub='{}'",
            pattern,
            endpoint,
            bind,
            subscribePrefix);

        while (running) {
          byte[] payload = recvPayload(sock);
          if (payload == null) {
            continue;
          }
          try {
            T value = deserializer.deserialize(payload);
            synchronized (ctx.getCheckpointLock()) {
              ctx.collect(value);
            }
          } catch (Exception e) {
            LOG.warn("zeromq source deserialize failed: {}", e.getMessage());
          }
        }
      } catch (Exception e) {
        if (running) {
          throw e;
        }
      }
    }

    /**
     * Reads one logical message. For ROUTER and DEALER patterns the wire layout is multi-frame
     * (identity / empty / payload); we drain the envelope and return the last frame.
     */
    private byte[] recvPayload(ZMQ.Socket sock) {
      byte[] frame = sock.recv(0);
      if (frame == null) {
        return null;
      }
      if (pattern == Pattern.ROUTER || pattern == Pattern.DEALER) {
        byte[] last = frame;
        while (sock.hasReceiveMore()) {
          byte[] next = sock.recv(0);
          if (next == null) {
            break;
          }
          last = next;
        }
        return last;
      }
      // SUB / XSUB carry an optional topic prefix in frame 0 when used through XPUB. Tolerate
      // both single-frame and topic+payload layouts.
      if ((pattern == Pattern.SUB || pattern == Pattern.XSUB) && sock.hasReceiveMore()) {
        byte[] body = sock.recv(0);
        return body != null ? body : frame;
      }
      return frame;
    }

    @Override
    public void cancel() {
      running = false;
    }

    private static SocketType toSocketType(Pattern p) {
      switch (p) {
        case PULL:
          return SocketType.PULL;
        case SUB:
          return SocketType.SUB;
        case XSUB:
          return SocketType.XSUB;
        case ROUTER:
          return SocketType.ROUTER;
        case DEALER:
          return SocketType.DEALER;
        default:
          throw new IllegalArgumentException("unknown pattern: " + p);
      }
    }
  }
}
