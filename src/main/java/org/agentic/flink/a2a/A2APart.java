package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A single content part of an A2A {@link A2AMessage} or {@link A2AArtifact}.
 *
 * <p>The A2A protocol discriminates parts by {@code kind}:
 *
 * <ul>
 *   <li>{@link Kind#TEXT} — plain text in {@link #getText()}.
 *   <li>{@link Kind#DATA} — structured JSON in {@link #getData()}.
 *   <li>{@link Kind#FILE} — a file referenced by {@link #getFileUri()} <em>or</em> inlined as
 *       base64 in {@link #getFileBytes()} (exactly one), with {@link #getMimeType()} / {@link
 *       #getFileName()} metadata.
 * </ul>
 *
 * <p>This is a framework-internal representation; {@code SdkA2AClient} and the gateway translate
 * between it and the A2A SDK's {@code Part} hierarchy so the rest of the codebase never touches SDK
 * types. Immutable and {@link Serializable} so it can ride in Flink state and bridge envelopes.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2APart implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Kind {
    TEXT,
    DATA,
    FILE
  }

  private final Kind kind;
  private final String text;
  private final Map<String, Object> data;
  private final String fileUri;
  private final String fileBytes; // base64
  private final String mimeType;
  private final String fileName;

  // Jackson constructor — all fields nullable, kind required.
  public A2APart(
      Kind kind,
      String text,
      Map<String, Object> data,
      String fileUri,
      String fileBytes,
      String mimeType,
      String fileName) {
    this.kind = Objects.requireNonNull(kind, "kind");
    this.text = text;
    this.data = data == null ? null : Collections.unmodifiableMap(data);
    this.fileUri = fileUri;
    this.fileBytes = fileBytes;
    this.mimeType = mimeType;
    this.fileName = fileName;
  }

  public static A2APart text(String text) {
    return new A2APart(Kind.TEXT, Objects.requireNonNull(text, "text"), null, null, null, null, null);
  }

  public static A2APart data(Map<String, Object> data) {
    return new A2APart(Kind.DATA, null, Objects.requireNonNull(data, "data"), null, null, null, null);
  }

  public static A2APart fileUri(String uri, String mimeType, String fileName) {
    return new A2APart(
        Kind.FILE, null, null, Objects.requireNonNull(uri, "uri"), null, mimeType, fileName);
  }

  public static A2APart fileBytes(String base64, String mimeType, String fileName) {
    return new A2APart(
        Kind.FILE, null, null, null, Objects.requireNonNull(base64, "base64"), mimeType, fileName);
  }

  public Kind getKind() {
    return kind;
  }

  public String getText() {
    return text;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public String getFileUri() {
    return fileUri;
  }

  public String getFileBytes() {
    return fileBytes;
  }

  public String getMimeType() {
    return mimeType;
  }

  public String getFileName() {
    return fileName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2APart)) {
      return false;
    }
    A2APart that = (A2APart) o;
    return kind == that.kind
        && Objects.equals(text, that.text)
        && Objects.equals(data, that.data)
        && Objects.equals(fileUri, that.fileUri)
        && Objects.equals(fileBytes, that.fileBytes)
        && Objects.equals(mimeType, that.mimeType)
        && Objects.equals(fileName, that.fileName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, text, data, fileUri, fileBytes, mimeType, fileName);
  }

  @Override
  public String toString() {
    switch (kind) {
      case TEXT:
        return "A2APart{text=" + text + '}';
      case DATA:
        return "A2APart{data=" + data + '}';
      case FILE:
        return "A2APart{file=" + (fileUri != null ? fileUri : "<bytes>") + ", mime=" + mimeType + '}';
      default:
        return "A2APart{kind=" + kind + '}';
    }
  }
}
