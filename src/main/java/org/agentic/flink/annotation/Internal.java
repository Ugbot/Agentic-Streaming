package org.agentic.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type that is public in the Java sense only because other packages of the framework need
 * to reach it.
 *
 * <p>An {@code @Internal} type is not API. It may change or disappear in any release, including
 * patch releases, and it is not covered by the deprecation policy. Flink functions and operators
 * that the builders wire up, codecs, type information, helpers and the examples fall in this
 * category. See {@code docs/api-stability.md}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Public
public @interface Internal {}
