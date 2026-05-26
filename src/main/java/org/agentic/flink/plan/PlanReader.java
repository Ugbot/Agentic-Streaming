package org.agentic.flink.plan;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the FQN+config descriptors in an {@link AgentPlan} into live Java SPI instances.
 *
 * <p>Instantiation strategy, in order:
 *
 * <ol>
 *   <li>Single-arg constructor accepting a {@code Map<String, String>} (or {@code Map}).
 *   <li>No-arg constructor, then call {@code initialize(Map<String, String>)} if such a method
 *       exists. Mirrors the
 *       {@link org.agentic.flink.storage.StorageFactory#createLongTermStore} pattern.
 * </ol>
 *
 * <p>Reflection is used (rather than ServiceLoader) because the plan itself names a fully
 * qualified class — the user has already chosen which implementation they want.
 */
public final class PlanReader {

  private static final Logger LOG = LoggerFactory.getLogger(PlanReader.class);

  private PlanReader() {}

  public static AgentPlan parse(String json) {
    return AgentPlan.fromJson(json);
  }

  /** Instantiate a Java SPI from its {@link ResourceSpec}. Throws if the class cannot be loaded. */
  public static Object instantiate(ResourceSpec spec) {
    if (spec == null) {
      throw new IllegalArgumentException("ResourceSpec must not be null");
    }
    Class<?> cls;
    try {
      cls = Class.forName(spec.getFqn(), true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Class not found for ResourceSpec fqn='" + spec.getFqn() + "'", e);
    }
    return instantiate(cls, spec.getConfig());
  }

  /** Instantiate from a class + config map, applying the constructor / initialize fallbacks. */
  public static Object instantiate(Class<?> cls, Map<String, String> config) {
    // Strategy 1: Map-arg constructor.
    for (Constructor<?> c : cls.getDeclaredConstructors()) {
      if (c.getParameterCount() == 1 && Map.class.isAssignableFrom(c.getParameterTypes()[0])) {
        try {
          c.setAccessible(true);
          return c.newInstance(config);
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(
              "Failed to invoke Map-arg constructor of " + cls.getName(), e);
        }
      }
    }
    // Strategy 2: no-arg constructor + optional initialize(Map).
    Object instance;
    try {
      Constructor<?> ctor = cls.getDeclaredConstructor();
      ctor.setAccessible(true);
      instance = ctor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Class " + cls.getName()
              + " has no no-arg or Map-arg constructor; cannot instantiate from plan", e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed to invoke no-arg constructor of " + cls.getName(), e);
    }
    Method init = findInitialize(cls);
    if (init != null) {
      try {
        init.setAccessible(true);
        init.invoke(instance, config);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(
            "Failed to call initialize(Map) on " + cls.getName(), e);
      }
    } else {
      LOG.debug(
          "{} has no initialize(Map) method; constructed with no-arg ctor only", cls.getName());
    }
    return instance;
  }

  private static Method findInitialize(Class<?> cls) {
    for (Method m : cls.getMethods()) {
      if ("initialize".equals(m.getName())
          && m.getParameterCount() == 1
          && Map.class.isAssignableFrom(m.getParameterTypes()[0])) {
        return m;
      }
    }
    return null;
  }
}
