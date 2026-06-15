package org.jagentic.core.suspend;

/** A turn held awaiting external input (human approval, an async result). {@code pendingText} is the
 * held turn's text, replayed on resume; {@code since} is when it suspended (for timeouts). */
public record Suspension(String conversationId, String reason, String pendingText, long since) {}
