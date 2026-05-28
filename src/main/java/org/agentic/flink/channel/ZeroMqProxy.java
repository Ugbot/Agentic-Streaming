package org.agentic.flink.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Tiny in-process ZeroMQ proxy. Lets you put a stable broker address in front of dynamic
 * publishers / workers so subscribers (or workers) can connect once and not care which producer
 * cycle they're attached to.
 *
 * <p>Two flavours:
 *
 * <ul>
 *   <li>{@link #pubSubProxy(String, String)} — {@code XSUB ↔ XPUB}. Publishers send to the
 *       front-end; subscribers attach to the back-end.
 *   <li>{@link #routerDealerProxy(String, String)} — {@code ROUTER ↔ DEALER}. Clients (DEALER)
 *       send requests to the front-end; workers (DEALER) read from the back-end.
 * </ul>
 *
 * <p>Each call spawns a daemon thread that runs {@link ZMQ#proxy} until {@link #stop()}. The
 * proxy is intended for the notebook control plane / dev loop; for production stand up a
 * dedicated broker.
 */
public final class ZeroMqProxy implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZeroMqProxy.class);

  private final ZContext zc;
  private final Thread thread;

  private ZeroMqProxy(
      String frontEndpoint, String backEndpoint, SocketType frontType, SocketType backType) {
    this.zc = new ZContext();
    ZMQ.Socket front = zc.createSocket(frontType);
    ZMQ.Socket back = zc.createSocket(backType);
    front.bind(frontEndpoint);
    back.bind(backEndpoint);
    LOG.info(
        "zeromq proxy {}↔{} front={} back={}", frontType, backType, frontEndpoint, backEndpoint);
    this.thread =
        new Thread(
            () -> {
              try {
                ZMQ.proxy(front, back, null);
              } catch (Throwable t) {
                if (!Thread.currentThread().isInterrupted()) {
                  LOG.warn("zeromq proxy exited: {}", t.getMessage());
                }
              }
            },
            "zeromq-proxy[" + frontEndpoint + "->" + backEndpoint + "]");
    thread.setDaemon(true);
    thread.start();
  }

  /** XSUB front-end (publishers connect/send) ↔ XPUB back-end (subscribers connect/recv). */
  public static ZeroMqProxy pubSubProxy(String pubFront, String subBack) {
    return new ZeroMqProxy(pubFront, subBack, SocketType.XSUB, SocketType.XPUB);
  }

  /** ROUTER front-end (clients/DEALER connect) ↔ DEALER back-end (workers/DEALER connect). */
  public static ZeroMqProxy routerDealerProxy(String clientFront, String workerBack) {
    return new ZeroMqProxy(clientFront, workerBack, SocketType.ROUTER, SocketType.DEALER);
  }

  /** Stops the proxy and releases its ZContext. */
  public void stop() {
    try {
      zc.close();
    } catch (Exception ignored) {
      // best-effort
    }
    thread.interrupt();
  }

  @Override
  public void close() {
    stop();
  }
}
