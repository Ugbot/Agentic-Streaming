# Web toolkit

The framework ships a small, well-behaved web stack — Jsoup for HTML, Apache
Tika for multi-format content extraction (PDF, DOC, PPT, EPUB, RTF, plain
text, …), and crawler-commons for robots.txt + sitemap.xml. All optional
dependencies; none load unless you opt in.

The goal is "StormCrawler's capabilities, without StormCrawler's Storm
dependency." You get parsing + politeness + multi-format support; you don't
get a Storm topology.

## Optional deps

Add to your downstream pom:

```xml
<dependency>
  <groupId>org.jsoup</groupId>
  <artifactId>jsoup</artifactId>
  <version>1.18.1</version>
</dependency>
<dependency>
  <groupId>com.github.crawler-commons</groupId>
  <artifactId>crawler-commons</artifactId>
  <version>1.4</version>
</dependency>
<dependency>
  <groupId>org.apache.tika</groupId>
  <artifactId>tika-core</artifactId>
  <version>2.9.2</version>
</dependency>
<dependency>
  <groupId>org.apache.tika</groupId>
  <artifactId>tika-parsers-standard-package</artifactId>
  <version>2.9.2</version>
</dependency>
<!-- Tika needs commons-io 2.15+; older versions on the Flink classpath break it. -->
<dependency>
  <groupId>commons-io</groupId>
  <artifactId>commons-io</artifactId>
  <version>2.16.1</version>
</dependency>
```

The framework's pom marks all five as `<optional>true</optional>` so users
who don't run a crawler pay nothing transitively.

## Building blocks

| Class | Role |
|-------|------|
| `WebToolkitOptions` | User agent, fetch timeout, max page bytes, max depth, robots policy, redirect policy. |
| `RobotsCache` | Per-host `BaseRobotRules` cache with 24h TTL. Falls open on fetch failure. |
| `Fetcher` | HTTP GET via `java.net.http.HttpClient`. Honours robots.txt + body-size cap + redirects. |
| `DocumentExtractor` | Jsoup for HTML (title, body text, absolute links); Tika for everything else. |
| `WebFetchTool` | `ToolExecutor`: GET a URL, return title + text + links. |
| `ExtractLinksTool` | Cheaper variant: links only, no body. |
| `CrawlerCore` | Multi-source fetch + extract loop. Consumes from any `Channel<UrlRequest>`s. |

## Crawler frontier — multi-source by design

`CrawlerCore.builder()` takes one or more `Channel<UrlRequest>`s. They're
unioned into a single input stream. The crawler doesn't own its inputs:

```java
CrawlerCore.builder()
    .frontier(
        seedChannel,                                       // static URLs
        agentCrawlChannel,                                 // ToolInvocationChannel — LLM-driven
        new KafkaChannel<>(brokers, "crawl-requests",      // external producers
                           "agent", UrlRequest.class))
    .options(WebToolkitOptions.defaults().withMaxDepth(2))
    .open(env);
```

Add or remove channels without touching the crawler operator. That's how the
agent and an external producer can target the same crawler — they're just
different channels into the same union.

## Robots policy

Default: enabled. Each host's `/robots.txt` is fetched once, cached for 24h,
and parsed via crawler-commons' `SimpleRobotRulesParser`. Behaviour mirrors
RFC 9309 + the StormCrawler convention:

- 2xx → parse and apply rules.
- 4xx → fall-open (allow all).
- 5xx → deny all until next refresh (24h TTL is overridden by the failure
  state).
- Network error → fall-open with a debug log.

Disable with `options.withRespectRobots(false)`.

## Tika and large files

`WebToolkitOptions.maxPageBytes` (default 10 MB) caps the body before
extraction. Tika streams what it gets, so you don't pay extraction cost on
the truncated tail; titles, the first N pages of a PDF, and the visible
text of an oversized HTML page all still extract correctly.

For very-large-document corpora (>10 MB single files) raise the cap or pre-
process upstream of the framework — Tika can stream PDFs but holding a 500
MB report in operator memory is rarely a good idea.

## Building your own ingestion shape

The framework's `IngestionPipeline` consumes `DataStream<CrawledPage>`; for
non-web sources, drop the crawler and feed `CrawledPage` records directly:

```java
DataStream<CrawledPage> docs = filesChannel.open(env)
    .map(localPath -> {
        byte[] bytes = Files.readAllBytes(localPath);
        DocumentExtractor.ExtractedDocument doc =
            new DocumentExtractor().extract("file://" + localPath, bytes, "auto");
        return new CrawledPage(localPath, localPath, "auto",
            doc.getTitle(), doc.getText(), List.of(),
            System.currentTimeMillis(), 0, doc.getMetadata());
    });

IngestionPipeline.from(docs)
    .chunk(new RecursiveTextChunker(512))
    .embed(djlEmbeddings)
    .into(corpus)
    .build();
```

The point: the crawler is one source of `CrawledPage`s. Any other source
that produces them feeds the same downstream pipeline unchanged.
