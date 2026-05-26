package org.agentic.flink.tools.builtin;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/**
 * Example tool class demonstrating LangChain4j @Tool annotation usage.
 *
 * <p>This class provides basic mathematical operations that are automatically discovered by the
 * ToolAnnotationRegistry and can be invoked within the Flink agent CEP saga flow.
 *
 * <p>Each method annotated with @Tool becomes a callable tool in the agent framework.
 *
 * @author Agentic Flink Team
 */
public class CalculatorTools {

  /**
   * Adds two numbers together.
   *
   * @param a First number
   * @param b Second number
   * @return The sum of a and b
   */
  @Tool("Performs addition of two numbers")
  public double add(@P("First number") double a, @P("Second number") double b) {
    return a + b;
  }

  /**
   * Subtracts the second number from the first.
   *
   * @param a First number (minuend)
   * @param b Second number (subtrahend)
   * @return The difference a - b
   */
  @Tool("Performs subtraction of two numbers")
  public double subtract(@P("First number (minuend)") double a, @P("Second number (subtrahend)") double b) {
    return a - b;
  }

  /**
   * Multiplies two numbers together.
   *
   * @param a First number
   * @param b Second number
   * @return The product of a and b
   */
  @Tool("Performs multiplication of two numbers")
  public double multiply(@P("First number") double a, @P("Second number") double b) {
    return a * b;
  }

  /**
   * Divides the first number by the second.
   *
   * @param a First number (dividend)
   * @param b Second number (divisor)
   * @return The quotient a / b
   * @throws ArithmeticException if b is zero
   */
  @Tool("Performs division of two numbers")
  public double divide(
      @P("First number (dividend)") double a, @P("Second number (divisor)") double b) {
    if (b == 0) {
      throw new ArithmeticException("Cannot divide by zero");
    }
    return a / b;
  }

  /**
   * Raises a number to a power.
   *
   * @param base The base number
   * @param exponent The exponent
   * @return base raised to the power of exponent
   */
  @Tool("Raises a number to a power")
  public double power(@P("Base number") double base, @P("Exponent") double exponent) {
    return Math.pow(base, exponent);
  }

  /**
   * Calculates the square root of a number.
   *
   * @param number The number to calculate square root of
   * @return The square root of number
   * @throws ArithmeticException if number is negative
   */
  @Tool("Calculates the square root of a number")
  public double sqrt(@P("Number to calculate square root of") double number) {
    if (number < 0) {
      throw new ArithmeticException("Cannot calculate square root of negative number");
    }
    return Math.sqrt(number);
  }

  /**
   * Calculates the absolute value of a number.
   *
   * @param number The number
   * @return The absolute value of number
   */
  @Tool("Calculates the absolute value of a number")
  public double abs(@P("Number") double number) {
    return Math.abs(number);
  }

  /**
   * Rounds a number to the nearest integer.
   *
   * @param number The number to round
   * @return The rounded value
   */
  @Tool("Rounds a number to the nearest integer")
  public long round(@P("Number to round") double number) {
    return Math.round(number);
  }

  /**
   * Finds the maximum of two numbers.
   *
   * @param a First number
   * @param b Second number
   * @return The larger of a and b
   */
  @Tool("Finds the maximum of two numbers")
  public double max(@P("First number") double a, @P("Second number") double b) {
    return Math.max(a, b);
  }

  /**
   * Finds the minimum of two numbers.
   *
   * @param a First number
   * @param b Second number
   * @return The smaller of a and b
   */
  @Tool("Finds the minimum of two numbers")
  public double min(@P("First number") double a, @P("Second number") double b) {
    return Math.min(a, b);
  }
}
