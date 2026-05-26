package org.agentic.flink.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DocumentExtractorTest {

  @Test
  @DisplayName("HTML extraction pulls title, body text, and absolute links")
  void htmlExtractsTitleTextLinks() {
    String html =
        "<!doctype html><html><head><title>Sample Page</title></head>"
            + "<body><h1>Hello</h1><p>This is the body.</p>"
            + "<a href=\"https://example.com/a\">A</a>"
            + "<a href=\"https://example.com/b\">B</a>"
            + "<a href=\"mailto:nope@example.com\">no</a>"
            + "</body></html>";
    DocumentExtractor ex = new DocumentExtractor();
    DocumentExtractor.ExtractedDocument doc =
        ex.extract("https://example.com/", html.getBytes(StandardCharsets.UTF_8), "text/html");
    assertEquals("Sample Page", doc.getTitle());
    assertTrue(doc.getText().contains("Hello"));
    assertTrue(doc.getText().contains("This is the body"));
    assertEquals(2, doc.getLinks().size());
    assertTrue(doc.getLinks().contains("https://example.com/a"));
    assertTrue(doc.getLinks().contains("https://example.com/b"));
  }

  @Test
  @DisplayName("HTML extraction resolves relative links against the base URL")
  void resolvesRelativeLinks() {
    String html =
        "<html><body><a href=\"/about\">About</a><a href=\"docs/x\">X</a></body></html>";
    DocumentExtractor ex = new DocumentExtractor();
    DocumentExtractor.ExtractedDocument doc =
        ex.extract("https://example.com/", html.getBytes(StandardCharsets.UTF_8), "text/html");
    assertTrue(doc.getLinks().contains("https://example.com/about"));
    assertTrue(doc.getLinks().contains("https://example.com/docs/x"));
  }

  @Test
  @DisplayName("Tika path extracts plain-text bytes correctly")
  void tikaExtractsPlainText() throws Exception {
    DocumentExtractor ex = new DocumentExtractor();
    DocumentExtractor.ExtractedDocument doc =
        ex.extractBytes("hello world from the corpus".getBytes(StandardCharsets.UTF_8), "text/plain");
    assertNotNull(doc);
    assertTrue(doc.getText().contains("hello world"));
  }

  @Test
  @DisplayName("Errors don't throw — they surface in metadata")
  void errorsDoNotThrow() {
    DocumentExtractor ex = new DocumentExtractor();
    DocumentExtractor.ExtractedDocument doc =
        ex.extract("urn:bad", new byte[] {0x01, 0x02, 0x03}, "application/x-totally-fake");
    assertNotNull(doc);
    // Tika may parse or fail; either way we never throw and we return a non-null result.
    assertEquals(0, doc.getLinks().size());
  }
}
