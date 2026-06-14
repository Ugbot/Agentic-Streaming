# 🤖 OpenAI Integration Setup Guide

## Quick Start

### 1. Get Your API Key

Visit: https://platform.openai.com/api-keys

Create a new API key (starts with `sk-...`)

### 2. Set Environment Variable (RECOMMENDED)

```bash
# Set for current session
export OPENAI_API_KEY="sk-your-actual-key-here"

# Verify it's set
echo $OPENAI_API_KEY
```

### 3. Run the Demo

```bash
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.OpenAIFlinkAgentsDemo"
```

## Installation Methods

### Method 1: Environment Variable (RECOMMENDED)

**Temporary (current session only):**
```bash
export OPENAI_API_KEY="sk-your-key-here"
```

**Permanent (add to shell profile):**

For **bash** (`~/.bashrc` or `~/.bash_profile`):
```bash
echo 'export OPENAI_API_KEY="sk-your-key-here"' >> ~/.bashrc
source ~/.bashrc
```

For **zsh** (`~/.zshrc`):
```bash
echo 'export OPENAI_API_KEY="sk-your-key-here"' >> ~/.zshrc
source ~/.zshrc
```

For **fish** (`~/.config/fish/config.fish`):
```fish
echo 'set -x OPENAI_API_KEY "sk-your-key-here"' >> ~/.config/fish/config.fish
source ~/.config/fish/config.fish
```

### Method 2: System Property (For Testing)

```bash
mvn exec:java \
  -Dexec.mainClass="org.agentic.flink.example.OpenAIFlinkAgentsDemo" \
  -Dopenai.api.key="sk-your-key-here"
```

### Method 3: .env File (For Development)

**Create `.env` file:**
```bash
echo 'OPENAI_API_KEY=sk-your-key-here' > .env
```

**IMPORTANT: Add to `.gitignore`:**
```bash
echo '.env' >> .gitignore
```

**Load in your code:**
```java
// You'd need a library like dotenv-java for this
// Or manually read the file
```

## Security Best Practices

### ✅ DO:
- ✅ Use environment variables
- ✅ Add `.env` to `.gitignore`
- ✅ Rotate keys regularly
- ✅ Use different keys for dev/prod
- ✅ Set usage limits on OpenAI dashboard
- ✅ Monitor usage on OpenAI dashboard

### ❌ DON'T:
- ❌ Hardcode API keys in source code
- ❌ Commit API keys to git
- ❌ Share keys in Slack/email
- ❌ Use production keys in development
- ❌ Store keys in plain text files (that get committed)

## Running the Demos

### OpenAI Demo (Standalone)

```bash
# Set your key first
export OPENAI_API_KEY="sk-..."

# Run the OpenAI-specific demo
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.OpenAIFlinkAgentsDemo"
```

**What it demonstrates:**
1. Simple OpenAI chat
2. Tool integration with OpenAI
3. OpenAI + Flink Agents integration

### Interactive Demo with OpenAI

The interactive demo can also use OpenAI if you add it. See the section below.

## Using OpenAI in Your Code

### Basic Chat

```java
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;

// Get API key from environment
String apiKey = System.getenv("OPENAI_API_KEY");

// Create model
ChatLanguageModel model = OpenAiChatModel.builder()
    .apiKey(apiKey)
    .modelName("gpt-5.4-nano")  // or "gpt-5.5"
    .temperature(0.7)
    .maxTokens(500)
    .build();

// Use it
String response = model.generate("Explain Apache Flink");
```

### With Flink Agents Integration

```java
import org.agentic.flink.langchain.model.language.OpenAiLanguageModel;

// Create OpenAI model
OpenAiLanguageModel openAiModel = new OpenAiLanguageModel();
ChatLanguageModel model = openAiModel.getModel(Map.of(
    "apiKey", System.getenv("OPENAI_API_KEY"),
    "modelName", "gpt-5.4-nano"
));

// Use with your agents
// (See examples in OpenAIFlinkAgentsDemo.java)
```

## Model Options

### GPT-5.4 nano (Fast & Cheap)
```java
.modelName("gpt-5.4-nano")
.maxTokens(500)
.temperature(0.7)  // 0.0 = deterministic, 1.0 = creative
```

**Best for:**
- Quick responses
- High volume
- Cost-sensitive applications
- Testing/development

**Cost:** ~$0.001 per 1K tokens

### GPT-5.5 (Powerful & Accurate)
```java
.modelName("gpt-5.5")
.maxTokens(1000)
.temperature(0.5)
```

**Best for:**
- Complex reasoning
- High-quality responses
- Production use
- Critical applications

**Cost:** ~$0.03 per 1K tokens

### GPT-5.4 (Balanced)
```java
.modelName("gpt-5.4")
.maxTokens(2000)
.temperature(0.7)
```

**Best for:**
- Balance of speed and quality
- Longer context windows
- Most production use cases

## Troubleshooting

### Error: "API key not found"

**Solution:**
```bash
# Verify environment variable is set
echo $OPENAI_API_KEY

# If empty, set it
export OPENAI_API_KEY="sk-your-key-here"

# Try again
mvn exec:java -Dexec.mainClass="..."
```

### Error: "Incorrect API key provided"

**Causes:**
- Key is wrong/typo
- Key was revoked
- Key has expired

**Solution:**
1. Go to https://platform.openai.com/api-keys
2. Create a new key
3. Replace the old one:
   ```bash
   export OPENAI_API_KEY="sk-new-key-here"
   ```

### Error: "Rate limit exceeded"

**Solution:**
- Wait 20-60 seconds
- Reduce request frequency
- Upgrade your OpenAI plan
- Check usage: https://platform.openai.com/usage

### Error: "Insufficient quota"

**Solution:**
- Add credits to your OpenAI account
- Check billing: https://platform.openai.com/account/billing
- Set up billing if needed

### Error: "Model not found"

**Solution:**
- Check model name spelling
- Verify you have access to that model
- Try "gpt-5.4-nano" as fallback

## Cost Management

### Set Usage Limits

1. Go to https://platform.openai.com/account/limits
2. Set hard limit (e.g., $10/month)
3. Set soft limit for alerts

### Monitor Usage

```bash
# Check your usage regularly
curl https://api.openai.com/v1/usage \
  -H "Authorization: Bearer $OPENAI_API_KEY"
```

Or visit: https://platform.openai.com/usage

### Optimize Costs

**Strategies:**
- Use GPT-5.4 nano for most tasks
- Set `maxTokens` to reasonable limits
- Cache responses when possible
- Use streaming for long responses
- Implement request throttling

## Integration Patterns

### Pattern 1: Simple Q&A

```java
ChatLanguageModel model = OpenAiChatModel.builder()
    .apiKey(System.getenv("OPENAI_API_KEY"))
    .modelName("gpt-5.4-nano")
    .build();

String answer = model.generate("Question here");
```

### Pattern 2: With Context

```java
String context = "User is asking about Apache Flink...";
String question = "What are checkpoints?";

String prompt = context + "\n\nQuestion: " + question;
String answer = model.generate(prompt);
```

### Pattern 3: With Tools (Flink Agents)

```java
// Create your tools
ToolExecutor calculator = new CalculatorTool();
ToolDefinition toolDef = new ToolDefinition(...);

// Wrap for Flink Agents
Agent toolAgent = FlinkAgentsToolAdapter.wrapSingleTool(
    "calculator", calculator, toolDef
);

// Use with OpenAI
// (OpenAI can decide when to call tools)
```

### Pattern 4: With Validation

```java
// Get AI response
String response = model.generate(question);

// Validate with your framework
ValidationResult result = validator.validate(response);

if (!result.isValid()) {
    // Retry with feedback
    String feedback = result.getFeedback();
    String improved = model.generate(question + "\nFeedback: " + feedback);
}
```

## Examples

### Example 1: Customer Support Bot

```java
public class CustomerSupportAgent {
    private ChatLanguageModel model;

    public CustomerSupportAgent() {
        model = OpenAiChatModel.builder()
            .apiKey(System.getenv("OPENAI_API_KEY"))
            .modelName("gpt-5.4-nano")
            .temperature(0.5)  // More consistent responses
            .build();
    }

    public String handleInquiry(String customerMessage) {
        String systemPrompt = "You are a helpful customer support agent. " +
                             "Be concise, friendly, and professional.";

        String response = model.generate(systemPrompt + "\n\n" + customerMessage);
        return response;
    }
}
```

### Example 2: Document Analyzer

```java
public String analyzeDocument(String documentText) {
    String prompt = "Analyze this document and provide key insights:\n\n" +
                   documentText;

    ChatLanguageModel model = OpenAiChatModel.builder()
        .apiKey(System.getenv("OPENAI_API_KEY"))
        .modelName("gpt-5.5")  // Better for analysis
        .maxTokens(1000)
        .build();

    return model.generate(prompt);
}
```

## Next Steps

1. **Get API key:** https://platform.openai.com/api-keys
2. **Set environment variable:** `export OPENAI_API_KEY="sk-..."`
3. **Run demo:** `mvn exec:java -Dexec.mainClass="org.agentic.flink.example.OpenAIFlinkAgentsDemo"`
4. **Read examples:** See `OpenAIFlinkAgentsDemo.java`
5. **Build your agent:** Use patterns above

## Support

- **OpenAI Docs:** https://platform.openai.com/docs
- **LangChain4J Docs:** https://docs.langchain4j.dev/
- **Our Examples:** See `src/main/java/org/agentic/flink/example/`
- **Troubleshooting:** See this document or DEMO_GUIDE.md

---

**⚠️ SECURITY REMINDER:**
Never commit API keys to git! Always use environment variables.

**🚀 Ready to use OpenAI with Flink Agents!**
