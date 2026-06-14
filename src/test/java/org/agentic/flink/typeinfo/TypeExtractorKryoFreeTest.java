package org.agentic.flink.typeinfo;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentExecutionState;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.llm.ChatMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Proves the {@code @TypeInfo} wiring: Flink's {@code TypeExtractor} (which backs both
 * {@code TypeInformation.of(Class)} stream typing AND {@code new ValueStateDescriptor<>(name, Class)}
 * keyed state) resolves each annotated type to a {@link JsonTypeInfo} — NOT the Kryo {@link
 * GenericTypeInfo} fallback. This is the direct guard that these types never silently Kryo.
 */
class TypeExtractorKryoFreeTest {

  private static void assertJsonTyped(Class<?> type) {
    TypeInformation<?> ti = TypeInformation.of(type);
    assertInstanceOf(
        JsonTypeInfo.class, ti, type.getSimpleName() + " should resolve to JsonTypeInfo");
    assertFalse(
        ti instanceof GenericTypeInfo, type.getSimpleName() + " must not fall back to Kryo");
  }

  @Test
  @DisplayName("the four former Kryo types now resolve to JsonTypeInfo via @TypeInfo")
  void kryoTypesAreJsonTyped() {
    assertJsonTyped(AgentEvent.class);
    assertJsonTyped(AgentContext.class);
    assertJsonTyped(AgentExecutionState.class);
    assertJsonTyped(ChatMessage.class);
  }

  @Test
  @DisplayName("RoutingBudget resolves to JsonTypeInfo (replacing the byte[] keyed-state hack)")
  void routingBudgetIsJsonTyped() {
    assertJsonTyped(RoutingBudget.class);
  }
}
