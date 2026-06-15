package org.jagentic.tools;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

import org.jagentic.core.ToolRegistry;

/** Binds LangChain4j {@code @Tool}/{@code @P}-annotated methods on a plain object into a
 * {@link ToolRegistry}, building a real input JSON-schema per method and a
 * {@code Map<String,Object> -> Object} invoker that coerces the argument map to the method's
 * parameter types. This is the Flink-free, scan-free analogue of the Flink
 * {@code ToolAnnotationRegistry} (no {@code org.reflections}; register instances explicitly).
 *
 * <p>Requires the declaring module to be compiled with {@code -parameters} so reflective
 * parameter names match the keys callers pass; descriptions come from {@code @P}.</p>
 */
public final class AnnotatedToolBinder {

  private AnnotatedToolBinder() {}

  /** Register every {@code @Tool} method on {@code instance} into {@code reg} (id-prefixed);
   * returns the registered ids. */
  public static List<String> bind(ToolRegistry reg, Object instance, String prefix) {
    List<String> ids = new ArrayList<>();
    String p = prefix == null ? "" : prefix;
    for (Method method : instance.getClass().getMethods()) {
      Tool tool = method.getAnnotation(Tool.class);
      if (tool == null) {
        continue;
      }
      method.setAccessible(true);
      String id = p + method.getName();
      String description = describe(tool, method.getName());
      Map<String, Object> schema = inputSchema(method);
      Parameter[] params = method.getParameters();
      reg.register(id, description, schema, args -> invoke(instance, method, params, args));
      ids.add(id);
    }
    return ids;
  }

  private static String describe(Tool tool, String fallback) {
    String[] v = tool.value();
    if (v != null && v.length > 0 && !v[0].isBlank()) {
      return String.join(" ", v);
    }
    return fallback;
  }

  private static Map<String, Object> inputSchema(Method method) {
    Map<String, Object> properties = new LinkedHashMap<>();
    List<String> required = new ArrayList<>();
    for (Parameter param : method.getParameters()) {
      String pname = param.getName();
      Map<String, Object> prop = new LinkedHashMap<>();
      prop.put("type", jsonType(param.getType()));
      P pAnn = param.getAnnotation(P.class);
      if (pAnn != null && pAnn.value() != null && !pAnn.value().isBlank()) {
        prop.put("description", pAnn.value());
      }
      properties.put(pname, prop);
      required.add(pname);
    }
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    return schema;
  }

  private static Object invoke(Object instance, Method method, Parameter[] params,
                               Map<String, Object> args) {
    Object[] call = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      Object raw = args == null ? null : args.get(params[i].getName());
      call[i] = coerce(raw, params[i].getType());
    }
    try {
      return method.invoke(instance, call);
    } catch (ReflectiveOperationException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new RuntimeException(method.getName() + ": " + cause.getMessage(), cause);
    }
  }

  /** JSON-schema type string for a Java type — mirrors the Flink registry's mapping. */
  private static String jsonType(Class<?> type) {
    if (type == boolean.class || type == Boolean.class) {
      return "boolean";
    }
    if (type == int.class || type == Integer.class || type == long.class || type == Long.class
        || type == short.class || type == Short.class || type == byte.class || type == Byte.class) {
      return "integer";
    }
    if (type == double.class || type == Double.class || type == float.class || type == Float.class) {
      return "number";
    }
    if (Number.class.isAssignableFrom(type)) {
      return "number";
    }
    return "string";
  }

  /** Coerce a JSON/map value to the target parameter type. */
  private static Object coerce(Object raw, Class<?> type) {
    if (raw == null) {
      return defaultFor(type);
    }
    if (type.isInstance(raw)) {
      return raw;
    }
    if (raw instanceof Number n) {
      if (type == int.class || type == Integer.class) {
        return n.intValue();
      }
      if (type == long.class || type == Long.class) {
        return n.longValue();
      }
      if (type == double.class || type == Double.class) {
        return n.doubleValue();
      }
      if (type == float.class || type == Float.class) {
        return n.floatValue();
      }
      if (type == short.class || type == Short.class) {
        return n.shortValue();
      }
    }
    String s = String.valueOf(raw);
    if (type == int.class || type == Integer.class) {
      return (int) Double.parseDouble(s);
    }
    if (type == long.class || type == Long.class) {
      return (long) Double.parseDouble(s);
    }
    if (type == double.class || type == Double.class) {
      return Double.parseDouble(s);
    }
    if (type == float.class || type == Float.class) {
      return Float.parseFloat(s);
    }
    if (type == boolean.class || type == Boolean.class) {
      return Boolean.parseBoolean(s);
    }
    return s;
  }

  private static Object defaultFor(Class<?> type) {
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == double.class) {
      return 0d;
    }
    if (type == float.class) {
      return 0f;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    return null;
  }
}
