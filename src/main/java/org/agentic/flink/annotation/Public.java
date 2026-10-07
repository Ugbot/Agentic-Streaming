package org.agentic.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type as stable public API.
 *
 * <p>A {@code @Public} type keeps its source and binary compatibility within a major release.
 * Removing or changing the signature of a public member first requires a {@link Deprecated}
 * annotation that names the release the deprecation started in ({@code since}) and a Javadoc
 * {@code @deprecated} tag that names the replacement; the deprecated member stays for at least one
 * minor release after that. See {@code docs/api-stability.md}.
 *
 * <p>Every public top-level type under {@code src/main} carries exactly one of {@link Public},
 * {@link Experimental} or {@link Internal}; {@code ApiStabilityMarkerTest} enforces this.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Public
public @interface Public {}
