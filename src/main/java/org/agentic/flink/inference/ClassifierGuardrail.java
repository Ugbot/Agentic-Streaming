package org.agentic.flink.inference;

import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Guardrail backed by a {@link Classifier}.
 *
 * <p>Runs the classifier against the concatenated user-message contents on {@link #beforeChat}
 * and the assistant response text on {@link #afterChat}. If the predicted label is in the
 * configured block-list, returns {@link GuardrailDecision#block}.
 *
 * <p>The {@link InferenceConnection} ships in the job graph; the live {@link InferenceClient}
 * is bound lazily on first use, like {@link InferenceToolAdapter}.
 */
public final class ClassifierGuardrail implements Guardrail {
  private static final long serialVersionUID = 1L;

  private final InferenceConnection connection;
  private final InferenceSetup setup;
  private final Set<String> blockLabels;
  private final boolean checkInput;
  private final boolean checkOutput;
  private final String name;

  private transient InferenceClient client;

  public ClassifierGuardrail(
      String name,
      InferenceConnection connection,
      InferenceSetup setup,
      Set<String> blockLabels,
      boolean checkInput,
      boolean checkOutput) {
    this.name = name == null ? "ClassifierGuardrail" : name;
    this.connection = Objects.requireNonNull(connection, "connection");
    this.setup = Objects.requireNonNull(setup, "setup");
    this.blockLabels =
        blockLabels == null ? Collections.emptySet() : Set.copyOf(blockLabels);
    this.checkInput = checkInput;
    this.checkOutput = checkOutput;
  }

  @Override
  public GuardrailDecision beforeChat(String agentId, List<ChatMessage> messages) {
    if (!checkInput || messages == null || messages.isEmpty()) {
      return GuardrailDecision.allow();
    }
    StringBuilder sb = new StringBuilder();
    for (ChatMessage m : messages) {
      if (m.getRole() == ChatRole.USER) {
        if (sb.length() > 0) sb.append('\n');
        sb.append(m.getContent());
      }
    }
    if (sb.length() == 0) return GuardrailDecision.allow();
    return decide(sb.toString());
  }

  @Override
  public GuardrailDecision afterChat(String agentId, ChatResponse response) {
    if (!checkOutput || response == null || response.getText() == null) {
      return GuardrailDecision.allow();
    }
    return decide(response.getText());
  }

  @Override
  public String name() {
    return name;
  }

  public Set<String> getBlockLabels() {
    return blockLabels;
  }

  public InferenceConnection getConnection() {
    return connection;
  }

  public InferenceSetup getSetup() {
    return setup;
  }

  private GuardrailDecision decide(String text) {
    ClassificationResult result = client().asClassifier().classify(text, setup);
    if (blockLabels.contains(result.getLabel())) {
      return GuardrailDecision.block(
          "Blocked by " + name + ": label=" + result.getLabel(),
          setup.getModelName());
    }
    return GuardrailDecision.allow();
  }

  private synchronized InferenceClient client() {
    if (client == null) {
      try {
        client = connection.bind(null);
      } catch (Exception e) {
        throw new RuntimeException(
            "ClassifierGuardrail failed to bind connection: " + e.getMessage(), e);
      }
    }
    return client;
  }
}
