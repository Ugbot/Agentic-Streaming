package org.jagentic.core.net;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * One HTTP/1.1 request to an address that was checked. The host is resolved once, every address it
 * resolved to is checked, and the socket connects to one of those, so a name whose answer changes
 * after the check (DNS rebinding) cannot move the call. The host name is kept for everything else:
 * the {@code Host} header, TLS server name indication and the certificate's name check.
 *
 * <p>The JDK's {@code HttpClient} resolves a name itself at connect time and has no way to be told an
 * address, which is why the tools, peers, models and the JWKS fetch use this instead. It speaks
 * HTTP/1.1 with {@code Connection: close}, follows no redirect, and reads a response of at most 16
 * MiB.
 */
public final class PinnedHttp {

  private static final int MAX_RESPONSE = 16 << 20;
  private static final int MAX_LINE = 64 << 10;

  private PinnedHttp() {}

  /** What came back. Header names are lower case. */
  public record Response(int status, Map<String, List<String>> headers, byte[] body) {
    public String text() {
      return new String(body, StandardCharsets.UTF_8);
    }

    public String header(String name) {
      List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
      return values == null || values.isEmpty() ? null : values.get(0);
    }
  }

  /** Resolves a URL's host once and returns the addresses it may connect to, or throws {@link Egress.Blocked}. */
  @FunctionalInterface
  public interface Addresses {
    InetAddress[] of(URI uri);
  }

  /** The operator's egress policy, read on each call: what tools, peers and models connect under. */
  public static Addresses egress() {
    return uri -> {
      try {
        return Egress.current().addresses(uri);
      } catch (OutboundUrlPolicy.BlockedUrlException e) {
        throw new Egress.Blocked("blocked by egress policy: " + e.getMessage(), e);
      }
    };
  }

  /** Any address the host resolves to, once. For a URL the operator chose and checks otherwise. */
  public static Addresses anyAddress() {
    return uri -> {
      try {
        return InetAddress.getAllByName(bare(uri.getHost()));
      } catch (java.net.UnknownHostException e) {
        throw new Egress.Blocked("host does not resolve: " + uri.getHost(), e);
      }
    };
  }

  /** Sends one request; {@code body} may be null. Throws {@link Egress.Blocked} before anything is sent. */
  public static Response send(String method, URI uri, Map<String, String> headers, byte[] body,
                              Duration timeout, Addresses addresses) throws IOException {
    return send(method, uri, headers, body, timeout, addresses, null);
  }

  static Response send(String method, URI uri, Map<String, String> headers, byte[] body, Duration timeout,
                       Addresses addresses, SSLContext tls) throws IOException {
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    if (!scheme.equals("http") && !scheme.equals("https")) {
      throw new Egress.Blocked("scheme not allowed: " + scheme, null);
    }
    if (uri.getHost() == null || uri.getHost().isEmpty()) {
      throw new Egress.Blocked("url has no host", null);
    }
    boolean secure = scheme.equals("https");
    String host = bare(uri.getHost());
    int port = uri.getPort() != -1 ? uri.getPort() : secure ? 443 : 80;
    InetAddress[] checked = addresses.of(uri);
    if (checked == null || checked.length == 0) {
      throw new Egress.Blocked("host does not resolve: " + host, null);
    }
    int millis = (int) Math.min(Integer.MAX_VALUE, Math.max(1, timeout.toMillis()));
    Socket socket = connect(checked, port, Math.min(millis, connectTimeoutMs()));
    try {
      socket.setSoTimeout(millis);
      if (secure) {
        socket = handshake(socket, host, port, tls);
      }
      OutputStream out = socket.getOutputStream();
      out.write(head(method, uri, host, port, secure, headers, body).getBytes(StandardCharsets.UTF_8));
      if (body != null) {
        out.write(body);
      }
      out.flush();
      return read(new BufferedInputStream(socket.getInputStream()), method);
    } finally {
      socket.close();
    }
  }

  /**
   * How long a connect may take, at most: {@code AGENTIC_HTTP_CONNECT_TIMEOUT_MS} or the system property
   * {@code agentic.http.connect_timeout_ms}, ten seconds by default, and never longer than the call.
   */
  static int connectTimeoutMs() {
    String set = System.getProperty("agentic.http.connect_timeout_ms", System.getenv("AGENTIC_HTTP_CONNECT_TIMEOUT_MS"));
    try {
      return set == null || set.isBlank() ? 10_000 : Math.max(1, Integer.parseInt(set.strip()));
    } catch (NumberFormatException e) {
      return 10_000;
    }
  }

  private static Socket connect(InetAddress[] checked, int port, int millis) throws IOException {
    IOException last = null;
    for (InetAddress address : checked) {
      Socket s = new Socket();
      try {
        s.connect(new InetSocketAddress(address, port), millis);
        return s;
      } catch (IOException e) {
        s.close();
        last = e;
      }
    }
    throw last;
  }

  private static Socket handshake(Socket raw, String host, int port, SSLContext tls) throws IOException {
    SSLSocketFactory factory = tls == null ? (SSLSocketFactory) SSLSocketFactory.getDefault() : tls.getSocketFactory();
    SSLSocket ssl = (SSLSocket) factory.createSocket(raw, host, port, true);
    SSLParameters p = ssl.getSSLParameters();
    p.setEndpointIdentificationAlgorithm("HTTPS"); // the certificate must name the host, not the address
    if (!isLiteral(host)) {
      p.setServerNames(List.of(new SNIHostName(host)));
    }
    ssl.setSSLParameters(p);
    ssl.startHandshake();
    return ssl;
  }

  private static String head(String method, URI uri, String host, int port, boolean secure,
                             Map<String, String> headers, byte[] body) {
    String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
    if (uri.getRawQuery() != null) {
      path += "?" + uri.getRawQuery();
    }
    String authority = (host.contains(":") ? "[" + host + "]" : host)
        + (port == (secure ? 443 : 80) ? "" : ":" + port);
    StringBuilder h = new StringBuilder();
    h.append(token(method)).append(' ').append(path).append(" HTTP/1.1\r\n");
    h.append("Host: ").append(authority).append("\r\n");
    Map<String, String> all = new LinkedHashMap<>();
    all.put("User-Agent", "jagentic");
    all.put("Accept", "*/*");
    headers.forEach(all::put);
    all.put("Connection", "close");
    if (body != null) {
      all.put("Content-Length", Integer.toString(body.length));
    }
    for (Map.Entry<String, String> e : all.entrySet()) {
      if (e.getKey().equalsIgnoreCase("Host")) {
        continue;
      }
      h.append(token(e.getKey())).append(": ").append(value(e.getValue())).append("\r\n");
    }
    return h.append("\r\n").toString();
  }

  private static Response read(InputStream in, String method) throws IOException {
    String status = line(in);
    String[] parts = status.split(" ", 3);
    if (parts.length < 2 || !parts[0].startsWith("HTTP/1.")) {
      throw new IOException("not an HTTP response");
    }
    int code;
    try {
      code = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IOException("not an HTTP status: " + parts[1]);
    }
    Map<String, List<String>> headers = new LinkedHashMap<>();
    for (String l = line(in); !l.isEmpty(); l = line(in)) {
      int colon = l.indexOf(':');
      if (colon > 0) {
        headers.computeIfAbsent(l.substring(0, colon).strip().toLowerCase(Locale.ROOT), k -> new ArrayList<>())
            .add(l.substring(colon + 1).strip());
      }
    }
    byte[] body;
    String encoding = first(headers, "transfer-encoding");
    String length = first(headers, "content-length");
    if (method.equals("HEAD") || code == 204 || code == 304 || code / 100 == 1) {
      body = new byte[0];
    } else if (encoding != null && encoding.toLowerCase(Locale.ROOT).contains("chunked")) {
      body = chunked(in);
    } else if (length != null) {
      long n;
      try {
        n = Long.parseLong(length);
      } catch (NumberFormatException e) {
        throw new IOException("bad Content-Length");
      }
      if (n < 0 || n > MAX_RESPONSE) {
        throw new IOException("response too large");
      }
      body = in.readNBytes((int) n);
      if (body.length != n) {
        throw new IOException("response ended early");
      }
    } else {
      body = in.readNBytes(MAX_RESPONSE + 1);
      if (body.length > MAX_RESPONSE) {
        throw new IOException("response too large");
      }
    }
    return new Response(code, headers, body);
  }

  private static byte[] chunked(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (true) {
      String size = line(in);
      int semi = size.indexOf(';');
      int n;
      try {
        n = Integer.parseInt((semi < 0 ? size : size.substring(0, semi)).strip(), 16);
      } catch (NumberFormatException e) {
        throw new IOException("bad chunk size");
      }
      if (n < 0 || out.size() + (long) n > MAX_RESPONSE) {
        throw new IOException("response too large");
      }
      if (n == 0) {
        while (!line(in).isEmpty()) {
          // trailers
        }
        return out.toByteArray();
      }
      byte[] chunk = in.readNBytes(n);
      if (chunk.length != n) {
        throw new IOException("response ended early");
      }
      out.write(chunk);
      line(in);
    }
  }

  private static String line(InputStream in) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    int c;
    while ((c = in.read()) != -1) {
      if (c == '\n') {
        break;
      }
      if (b.size() >= MAX_LINE) {
        throw new IOException("header line too long");
      }
      b.write(c);
    }
    if (c == -1 && b.size() == 0) {
      throw new IOException("connection closed");
    }
    String s = b.toString(StandardCharsets.ISO_8859_1);
    return s.endsWith("\r") ? s.substring(0, s.length() - 1) : s;
  }

  private static String first(Map<String, List<String>> headers, String name) {
    List<String> v = headers.get(name);
    return v == null || v.isEmpty() ? null : v.get(0);
  }

  /** A header name or method: no separators, so nothing can be smuggled in. */
  private static String token(String s) {
    if (s == null || s.isEmpty() || !s.chars().allMatch(c -> c > 32 && c < 127 && "()<>@,;:\\\"/[]?={}".indexOf(c) < 0)) {
      throw new IllegalArgumentException("not an HTTP token: " + s);
    }
    return s;
  }

  /** A header value: no line break, so no header can be smuggled in. */
  private static String value(String s) {
    if (s == null || s.indexOf('\r') >= 0 || s.indexOf('\n') >= 0) {
      throw new IllegalArgumentException("a header value may not hold a line break");
    }
    return s;
  }

  private static String bare(String host) {
    String h = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
    return h.endsWith(".") ? h.substring(0, h.length() - 1) : h;
  }

  private static boolean isLiteral(String host) {
    return host.contains(":") || host.matches("[0-9.]+");
  }
}
