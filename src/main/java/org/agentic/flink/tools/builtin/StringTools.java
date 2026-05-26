package org.agentic.flink.tools.builtin;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/**
 * String manipulation tools using LangChain4j @Tool annotations.
 *
 * <p>This class provides common string operations that can be used by agents during workflow
 * execution.
 *
 * @author Agentic Flink Team
 */
public class StringTools {

  /**
   * Converts a string to uppercase.
   *
   * @param text The text to convert
   * @return The uppercase version of the text
   */
  @Tool("Converts a string to uppercase")
  public String toUpperCase(@P("Text to convert") String text) {
    if (text == null) {
      return null;
    }
    return text.toUpperCase();
  }

  /**
   * Converts a string to lowercase.
   *
   * @param text The text to convert
   * @return The lowercase version of the text
   */
  @Tool("Converts a string to lowercase")
  public String toLowerCase(@P("Text to convert") String text) {
    if (text == null) {
      return null;
    }
    return text.toLowerCase();
  }

  /**
   * Gets the length of a string.
   *
   * @param text The text to measure
   * @return The length of the text
   */
  @Tool("Gets the length of a string")
  public int length(@P("Text to measure") String text) {
    if (text == null) {
      return 0;
    }
    return text.length();
  }

  /**
   * Concatenates two strings.
   *
   * @param first First string
   * @param second Second string
   * @return The concatenated string
   */
  @Tool("Concatenates two strings")
  public String concat(@P("First string") String first, @P("Second string") String second) {
    if (first == null) first = "";
    if (second == null) second = "";
    return first + second;
  }

  /**
   * Checks if a string contains a substring.
   *
   * @param text The text to search in
   * @param substring The substring to search for
   * @return true if text contains substring
   */
  @Tool("Checks if a string contains a substring")
  public boolean contains(@P("Text to search in") String text, @P("Substring to search for") String substring) {
    if (text == null || substring == null) {
      return false;
    }
    return text.contains(substring);
  }

  /**
   * Replaces all occurrences of a substring with another string.
   *
   * @param text The text to modify
   * @param target The substring to replace
   * @param replacement The replacement string
   * @return The modified text
   */
  @Tool("Replaces all occurrences of a substring")
  public String replace(
      @P("Text to modify") String text,
      @P("Substring to replace") String target,
      @P("Replacement string") String replacement) {
    if (text == null) {
      return null;
    }
    if (target == null || replacement == null) {
      return text;
    }
    return text.replace(target, replacement);
  }

  /**
   * Trims whitespace from both ends of a string.
   *
   * @param text The text to trim
   * @return The trimmed text
   */
  @Tool("Trims whitespace from both ends of a string")
  public String trim(@P("Text to trim") String text) {
    if (text == null) {
      return null;
    }
    return text.trim();
  }

  /**
   * Extracts a substring from a string.
   *
   * @param text The text to extract from
   * @param startIndex The starting index (inclusive)
   * @param endIndex The ending index (exclusive)
   * @return The substring
   */
  @Tool("Extracts a substring from a string")
  public String substring(
      @P("Text to extract from") String text,
      @P("Starting index (inclusive)") int startIndex,
      @P("Ending index (exclusive)") int endIndex) {
    if (text == null) {
      return null;
    }
    if (startIndex < 0) startIndex = 0;
    if (endIndex > text.length()) endIndex = text.length();
    if (startIndex >= endIndex) {
      return "";
    }
    return text.substring(startIndex, endIndex);
  }

  /**
   * Splits a string by a delimiter.
   *
   * @param text The text to split
   * @param delimiter The delimiter to split by
   * @return A comma-separated string of parts
   */
  @Tool("Splits a string by a delimiter")
  public String split(@P("Text to split") String text, @P("Delimiter to split by") String delimiter) {
    if (text == null || delimiter == null) {
      return text;
    }
    String[] parts = text.split(delimiter);
    return String.join(", ", parts);
  }

  /**
   * Checks if a string starts with a prefix.
   *
   * @param text The text to check
   * @param prefix The prefix to check for
   * @return true if text starts with prefix
   */
  @Tool("Checks if a string starts with a prefix")
  public boolean startsWith(@P("Text to check") String text, @P("Prefix to check for") String prefix) {
    if (text == null || prefix == null) {
      return false;
    }
    return text.startsWith(prefix);
  }

  /**
   * Checks if a string ends with a suffix.
   *
   * @param text The text to check
   * @param suffix The suffix to check for
   * @return true if text ends with suffix
   */
  @Tool("Checks if a string ends with a suffix")
  public boolean endsWith(@P("Text to check") String text, @P("Suffix to check for") String suffix) {
    if (text == null || suffix == null) {
      return false;
    }
    return text.endsWith(suffix);
  }
}
