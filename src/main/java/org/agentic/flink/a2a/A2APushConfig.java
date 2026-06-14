package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.Objects;

/**
 * A webhook push-notification configuration registered against an A2A task
 * ({@code tasks/pushNotificationConfig/set}).
 *
 * <p>For long-running tasks where holding an SSE stream is impractical, the caller registers a
 * webhook the gateway {@code POST}s task updates to. {@code token} lets the caller validate the
 * callback; {@code auth} lets the gateway authenticate to the webhook. Immutable + {@link
 * Serializable}; persisted by {@link org.agentic.flink.a2a.storage.A2ATaskStore}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2APushConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String url;
  private final String token;
  private final AuthSpec auth;

  public A2APushConfig(String id, String url, String token, AuthSpec auth) {
    this.id = id == null ? "default" : id;
    this.url = Objects.requireNonNull(url, "url");
    this.token = token;
    this.auth = auth == null ? AuthSpec.none() : auth;
  }

  public String getId() {
    return id;
  }

  public String getUrl() {
    return url;
  }

  public String getToken() {
    return token;
  }

  public AuthSpec getAuth() {
    return auth;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2APushConfig)) {
      return false;
    }
    A2APushConfig that = (A2APushConfig) o;
    return Objects.equals(id, that.id)
        && Objects.equals(url, that.url)
        && Objects.equals(token, that.token);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, url, token);
  }

  @Override
  public String toString() {
    return "A2APushConfig{id=" + id + ", url=" + url + '}';
  }
}
