package org.agentic.flink.channel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.agentic.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Tiny in-process ZeroMQ proxy. Lets you put a stable broker address in front of dynamic publishers
 * / workers so subscribers (or workers) can connect once and not care which producer cycle they're
 * attached to.
 *
 * <p>Two flavours:
 *
 * <ul>
 *   <li>{@link #pubSubProxy(String, String)} — {@code XSUB ↔ XPUB}. Publishers send to the
 *       front-end; subscribers attach to the back-end.
 *   <li>{@link #routerDealerProxy(String, String)} — {@code ROUTER ↔ DEALER}. Clients (DEALER) send
 *       requests to the front-end; workers (DEALER) read from the back-end.
 * </ul>
 *
 * <p>Each call spawns a daemon thread that runs {@link ZMQ#proxy} until {@link #stop()}. Both
 * endpoints are bound before the factory returns, so peers may connect immediately. The proxy is
 * steered through a {@code PAIR} control socket: {@link #stop()} sends {@code TERMINATE}, waits for
 * the proxy thread to close its own sockets, and only then releases the {@link ZContext}. Sockets
 * are therefore only ever touched by the thread that owns them, which JeroMQ requires.
 *
 * <p>The proxy is intended for the notebook control plane / dev loop; for production stand up a
 * dedicated broker.
 */
@Internal
public final class ZeroMqProxy implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZeroMqProxy.class);
  private static final AtomicLong IDS = new AtomicLong();
  private static final long STOP_TIMEOUT_MS = 5_000;

  private final ZContext zc;
  private final Thread thread;
  private final String controlEndpoint;
  private final AtomicBoolean stopped = new AtomicBoolean();

  private ZeroMqProxy(
      String frontEndpoint, String backEndpoint, SocketType frontType, SocketType backType) {
    this.zc = new ZContext();
    this.controlEndpoint = "inproc://zeromq-proxy-control-" + IDS.incrementAndGet();
    ZMQ.Socket front = zc.createSocket(frontType);
    ZMQ.Socket back = zc.createSocket(backType);
    ZMQ.Socket control = zc.createSocket(SocketType.PAIR);
    front.setLinger(0);
    back.setLinger(0);
    control.setLinger(0);
    try {
      front.bind(frontEndpoint);
      back.bind(backEndpoint);
      control.bind(controlEndpoint);
    } catch (RuntimeException e) {
      zc.close();
      throw e;
    }
    LOG.info(
        "zeromq proxy {}↔{} front={} back={}", frontType, backType, frontEndpoint, backEndpoint);
    this.thread =
        new Thread(
            () -> {
              try {
                ZMQ.proxy(front, back, null, control);
              } catch (Throwable t) {
                if (!stopped.get()) {
                  LOG.warn("zeromq proxy exited: {}", t.toString());
                }
              } finally {
                zc.destroySocket(front);
                zc.destroySocket(back);
                zc.destroySocket(control);
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

  /** True while the forwarding thread is alive. */
  public boolean isRunning() {
    return thread.isAlive();
  }

  /** Waits up to the given time for the forwarding thread to exit; true when it has. */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    unit.timedJoin(thread, timeout);
    return !thread.isAlive();
  }

  /**
   * Stops the proxy and releases its ZContext. Idempotent. Returns once the forwarding thread has
   * exited or {@value #STOP_TIMEOUT_MS} ms have passed, so a caller is never blocked indefinitely.
   */
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }
    if (thread.isAlive()) {
      ZMQ.Socket steer = zc.createSocket(SocketType.PAIR);
      try {
        steer.setLinger(0);
        steer.setSendTimeOut((int) STOP_TIMEOUT_MS);
        steer.connect(controlEndpoint);
        if (!steer.send(ZMQ.PROXY_TERMINATE, 0)) {
          LOG.warn("zeromq proxy did not accept TERMINATE");
        }
      } catch (RuntimeException e) {
        LOG.warn("zeromq proxy control send failed: {}", e.toString());
      } finally {
        zc.destroySocket(steer);
      }
      try {
        if (!awaitTermination(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          LOG.warn("zeromq proxy thread did not exit within {} ms", STOP_TIMEOUT_MS);
          thread.interrupt();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    try {
      zc.close();
    } catch (Exception e) {
      LOG.warn("zeromq proxy context close failed: {}", e.toString());
    }
  }

  @Override
  public void close() {
    stop();
  }
}
