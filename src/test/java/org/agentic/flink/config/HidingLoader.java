package org.agentic.flink.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Hides one optional client from the framework. A {@code provided} client is on the test classpath
 * but not in the default uber jar, so this redefines the framework classes in a loader that refuses
 * the client's package, which is what a job on the default jar sees.
 */
final class HidingLoader extends ClassLoader {

  private final String hiddenPrefix;

  HidingLoader(String hiddenPrefix) {
    super(HidingLoader.class.getClassLoader());
    this.hiddenPrefix = hiddenPrefix;
  }

  /** Runs {@code probe} inside a fresh loader that hides {@code hiddenPrefix}. */
  static String run(String hiddenPrefix, Class<? extends Callable<String>> probe) throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    ClassLoader hiding = new HidingLoader(hiddenPrefix);
    Thread.currentThread().setContextClassLoader(hiding);
    try {
      Object instance = hiding.loadClass(probe.getName()).getConstructor().newInstance();
      return ((Callable<?>) instance).call().toString();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  boolean hides(String name) {
    return name.startsWith(hiddenPrefix);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (hides(name)) {
      throw new ClassNotFoundException(name);
    }
    if (!name.startsWith("org.agentic.flink.")) {
      return super.loadClass(name, resolve);
    }
    synchronized (getClassLoadingLock(name)) {
      Class<?> loaded = findLoadedClass(name);
      if (loaded != null) {
        return loaded;
      }
      try (InputStream in = getResourceAsStream(name.replace('.', '/') + ".class")) {
        if (in == null) {
          throw new ClassNotFoundException(name);
        }
        byte[] bytes = in.readAllBytes();
        return defineClass(name, bytes, 0, bytes.length);
      } catch (IOException e) {
        throw new ClassNotFoundException(name, e);
      }
    }
  }
}
