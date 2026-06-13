package org.agentic.flink.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agentic.flink.channel.sink.ForEachSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * ZeroMQ producer side, mirroring {@link ZeroMqChannel}. A native Flink 2.x
 * ({@link org.apache.flink.api.connector.sink2 FLIP-143}) sink: the factories return a
 * {@link ForEachSink} backed by a {@link ZmqWriteFn}; wire it with {@code stream.sinkTo(...)}.
 *
 * <p>Static factories for every common sink pattern:
 *
 * <ul>
 *   <li>{@link #push(String)} — {@code PUSH} socket; round-robin load-balance to attached PULLers.
 *       Pairs with {@link ZeroMqChannel#pull}. <b>Default for chaining</b>.
 *   <li>{@link #pub(String, String)} — {@code PUB} socket; fan-out broadcast to all SUBs. Pairs
 *       with {@link ZeroMqChannel#sub}. Topic is prepended as a separate frame.
 *   <li>{@link #xpub(String)} — {@code XPUB} publisher side of a broker.
 *   <li>{@link #router(String)} — {@code ROUTER} reply side; routes by client identity.
 *   <li>{@link #dealer(String)} — {@code DEALER} request side; load-balanced across peers.
 * </ul>
 *
 * <p>Bind-vs-connect defaults to the "server" side: PUSH connects (to a bound PULL), PUB binds
 * (so multiple SUBs can connect), XPUB binds, ROUTER binds, DEALER connects. Override with
 * {@link Builder#bind(boolean)}.
 *
 * <p>Wire format: JSON via Jackson by default; supply a custom {@link SerializationSchema} for
 * binary / Avro / Protobuf. The ZMQ socket is thread-affined, so the {@link ForEachSink} writer
 * opens/sends/closes it on a single subtask thread; keep parallelism at 1 ({@code setParallelism(1)})
 * since N>1 ZMQ sockets round-robin/duplicate in surprising ways.
 */
public final class ZeroMqSink<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ZeroMqSink.class);

  private ZeroMqSink() {}

  /** Sink-side ZeroMQ socket pattern. */
  public enum Pattern {
    PUSH,
    PUB,
    XPUB,
    ROUTER,
    DEALER
  }

  /** {@code PUSH} — round-robin to N attached {@code PULL}-ers. */
  public static <T> ForEachSink<T> push(String endpoint) {
    return new Builder<T>(Pattern.PUSH, endpoint).build();
  }

  /** {@code PUB} — fan-out broadcast to all {@code SUB}-ers. Optional topic prefix per message. */
  public static <T> ForEachSink<T> pub(String endpoint, String topic) {
    return new Builder<T>(Pattern.PUB, endpoint).topic(topic).build();
  }

  /** {@code XPUB} — publisher side of a broker; sees subscribe/unsubscribe acks. */
  public static <T> ForEachSink<T> xpub(String endpoint) {
    return new Builder<T>(Pattern.XPUB, endpoint).build();
  }

  /** {@code ROUTER} — async req/rep server; pairs with {@link ZeroMqChannel#dealer}. */
  public static <T> ForEachSink<T> router(String endpoint) {
    return new Builder<T>(Pattern.ROUTER, endpoint).build();
  }

  /** {@code DEALER} — async req/rep client; pairs with {@link ZeroMqChannel#router}. */
  public static <T> ForEachSink<T> dealer(String endpoint) {
    return new Builder<T>(Pattern.DEALER, endpoint).build();
  }

  /** Full builder if you need non-default bind/HWM/etc. */
  public static <T> Builder<T> builder(Pattern pattern, String endpoint) {
    return new Builder<>(pattern, endpoint);
  }

  /**
   * Variant of {@link #pub} that sends already-encoded {@code String} payloads as raw UTF-8
   * bytes (no extra JSON wrapping). Use when upstream operators already emit JSON envelopes —
   * the default Jackson serializer would otherwise double-encode the string.
   */
  public static ForEachSink<String> pubRaw(String endpoint, String topic) {
    return new Builder<String>(Pattern.PUB, endpoint)
        .topic(topic)
        .serializer(new RawStringSerializer())
        .build();
  }

  /** {@link SerializationSchema} that writes a {@code String} as raw UTF-8 bytes. */
  public static final class RawStringSerializer implements SerializationSchema<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(String element) {
      return element == null ? new byte[0] : element.getBytes(StandardCharsets.UTF_8);
    }
  }

  /**
   * The per-subtask ZeroMQ write behavior: opens the socket on the writer thread, sends one message
   * per element, and closes on that same thread. Public so tests can drive it directly.
   */
  public static final class ZmqWriteFn<T> implements ForEachSink.WriteFn<T> {
    private static final long serialVersionUID = 1L;

    private final Pattern pattern;
    private final String endpoint;
    private final boolean bind;
    private final String publishTopic;
    private final int hwm;
    private final int linger;
    private final int sendTimeoutMs;
    private final SerializationSchema<T> serializer;

    private transient ZContext zc;
    private transient ZMQ.Socket sock;

    ZmqWriteFn(Builder<T> b) {
      this.pattern = b.pattern;
      this.endpoint = Objects.requireNonNull(b.endpoint, "endpoint");
      this.bind = b.bind;
      this.publishTopic = b.publishTopic == null ? "" : b.publishTopic;
      this.hwm = b.hwm;
      this.linger = b.linger;
      this.sendTimeoutMs = b.sendTimeoutMs;
      this.serializer = b.serializer != null ? b.serializer : new JsonSerializer<>();
    }

    @Override
    public void open(int subtaskIndex) {
      this.zc = new ZContext();
      this.sock = zc.createSocket(toSocketType(pattern));
      sock.setSndHWM(hwm);
      sock.setLinger(linger);
      sock.setSendTimeOut(sendTimeoutMs);
      if (bind) {
        sock.bind(endpoint);
      } else {
        sock.connect(endpoint);
      }
      LOG.info("zeromq sink open pattern={} endpoint={} bind={}", pattern, endpoint, bind);
    }

    @Override
    public void write(T value) {
      if (value == null) {
        return;
      }
      byte[] payload = serializer.serialize(value);
      if (payload == null) {
        return;
      }
      if (pattern == Pattern.PUB && !publishTopic.isEmpty()) {
        sock.sendMore(publishTopic.getBytes(StandardCharsets.UTF_8));
      }
      boolean sent = sock.send(payload, 0);
      if (!sent) {
        LOG.warn("zeromq sink send returned false (HWM or send timeout)");
      }
    }

    @Override
    public void close() {
      if (zc != null) {
        try {
          zc.close();
        } catch (Exception ignored) {
          // best-effort
        }
        zc = null;
        sock = null;
      }
    }
  }

  /** Builder for a ZeroMQ {@link ForEachSink}. */
  public static final class Builder<T> {
    private final Pattern pattern;
    private final String endpoint;
    private boolean bind;
    private String publishTopic = "";
    private int hwm = 1000;
    private int linger = 0;
    private int sendTimeoutMs = 1000;
    private SerializationSchema<T> serializer;

    Builder(Pattern pattern, String endpoint) {
      this.pattern = pattern;
      this.endpoint = endpoint;
      // Defaults: bind for "server" patterns, connect for "client" patterns.
      this.bind = pattern == Pattern.PUB || pattern == Pattern.XPUB || pattern == Pattern.ROUTER;
    }

    public Builder<T> bind(boolean bind) {
      this.bind = bind;
      return this;
    }

    public Builder<T> topic(String topic) {
      this.publishTopic = topic == null ? "" : topic;
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

    public Builder<T> sendTimeoutMs(int ms) {
      this.sendTimeoutMs = ms;
      return this;
    }

    public Builder<T> serializer(SerializationSchema<T> s) {
      this.serializer = s;
      return this;
    }

    /** The per-subtask write function, for tests that drive open/write/close directly. */
    public ZmqWriteFn<T> writeFn() {
      return new ZmqWriteFn<>(this);
    }

    public ForEachSink<T> build() {
      return new ForEachSink<>(new ZmqWriteFn<>(this));
    }
  }

  private static SocketType toSocketType(Pattern p) {
    switch (p) {
      case PUSH:
        return SocketType.PUSH;
      case PUB:
        return SocketType.PUB;
      case XPUB:
        return SocketType.XPUB;
      case ROUTER:
        return SocketType.ROUTER;
      case DEALER:
        return SocketType.DEALER;
      default:
        throw new IllegalArgumentException("unknown pattern: " + p);
    }
  }

  /** Default Jackson-JSON serializer; reused by {@link ZeroMqSink} when none is supplied. */
  public static final class JsonSerializer<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    private ObjectMapper mapper() {
      if (mapper == null) {
        mapper = new ObjectMapper().registerModule(new ParameterNamesModule());
      }
      return mapper;
    }

    @Override
    public byte[] serialize(T element) {
      try {
        return mapper().writeValueAsBytes(element);
      } catch (IOException e) {
        throw new RuntimeException("zeromq sink: JSON encode failed: " + e.getMessage(), e);
      }
    }
  }

  // Unused but useful when subclasses want the typed elementType().
  @SuppressWarnings("unused")
  private static <T> TypeInformation<T> ti(Class<T> c) {
    return TypeInformation.of(c);
  }
}
