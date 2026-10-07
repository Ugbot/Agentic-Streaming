package org.agentic.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type that users may call but whose API is still settling.
 *
 * <p>An {@code @Experimental} type may change or be removed in any minor release without a
 * deprecation cycle. Changes are listed in the release notes but the framework makes no
 * compatibility promise for them. Once an experimental type has held its shape for a release it is
 * promoted to {@link Public}. See {@code docs/api-stability.md}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Public
public @interface Experimental {}
