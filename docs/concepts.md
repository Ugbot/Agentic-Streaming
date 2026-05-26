# 🎓 Core Concepts - Explained Simply

This guide explains the key concepts of Agentic Flink using simple analogies and examples.

## Table of Contents
1. [What is an Agent?](#what-is-an-agent)
2. [Events - How Agents Communicate](#events)
3. [Tools - What Agents Can Do](#tools)
4. [Workflows - The Agent's Process](#workflows)
5. [Context and Memory](#context-and-memory)
6. [Validation and Correction](#validation-and-correction)
7. [RAG - Knowledge Retrieval](#rag)
8. [Apache Flink - The Engine](#apache-flink)

---

## What is an Agent?

Think of an **agent** like a smart employee who:
- Receives tasks (emails/messages)
- Decides how to complete them
- Uses tools to do the work
- Checks their work
- Asks for help if needed

### Real-World Analogy

Imagine you're a customer service representative (the agent):

```
You receive a customer email (event)
   ↓
You read it and understand the problem
   ↓
You look up the customer's order (use database tool)
   ↓
You calculate a refund (use calculator tool)
   ↓
You send a response email (use email tool)
   ↓
Your manager reviews it (validation)
   ↓
Task complete!
```

### In Agentic Flink

```java
AgentConfig agent = new AgentConfig();
agent.setAgentId("customer-service-agent");

// Give the agent tools
agent.addTool(new DatabaseTool());
agent.addTool(new CalculatorTool());
agent.addTool(new EmailTool());

// Enable quality checks
agent.setValidationEnabled(true);
```

---

## Events - How Agents Communicate

**Events** are messages that tell agents what to do. Like emails in your inbox!

### Event Types

Think of event types as email subject lines:

| Event Type | Like an Email Saying... | Example |
|------------|-------------------------|---------|
| `FLOW_STARTED` | "New task assigned to you" | Customer sent a support ticket |
| `TOOL_CALL_REQUESTED` | "Please use this tool" | Look up order #12345 |
| `TOOL_CALL_COMPLETED` | "Tool finished, here's the result" | Order found: $99.99 |
| `VALIDATION_PASSED` | "Your work looks good!" | Response approved |
| `VALIDATION_FAILED` | "Please redo this" | Missing information |
| `FLOW_COMPLETED` | "Task finished!" | Ticket resolved |

### Creating an Event

```java
// Create a task
AgentEvent event = new AgentEvent();
event.setFlowId("ticket-12345");           // Task ID
event.setUserId("customer-001");           // Who sent it
event.setAgentId("support-agent");         // Who handles it
event.setEventType(AgentEventType.TOOL_CALL_REQUESTED);

// Add task details
event.putData("action", "lookup_order");
event.putData("orderId", "12345");
```

---

## Tools - What Agents Can Do

**Tools** are the agent's abilities. Like apps on your phone!

### Common Tool Categories

📱 **Communication Tools**
- SendEmail
- SendSMS
- PostToSlack

💾 **Data Tools**
- DatabaseQuery
- ReadFile
- WriteFile

🧮 **Processing Tools**
- Calculator
- DataValidator
- TextAnalyzer

🌐 **External Tools**
- CallAPI
- WebSearch
- FetchURL

### Creating a Tool

Let's create a simple "SendEmail" tool:

```java
public class SendEmailTool extends AbstractToolExecutor {

    @Override
    public String getToolId() {
        return "send-email";
    }

    @Override
    public String getDescription() {
        return "Sends an email to a recipient";
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> params) {
        // Get parameters
        String to = getRequiredParameter(params, "to");
        String subject = getRequiredParameter(params, "subject");
        String body = getRequiredParameter(params, "body");

        // Send the email (your email logic here)
        EmailService.send(to, subject, body);

        // Return success
        return CompletableFuture.completedFuture("Email sent!");
    }

    @Override
    public boolean validateParameters(Map<String, Object> params) {
        return params.containsKey("to") &&
               params.containsKey("subject") &&
               params.containsKey("body");
    }
}
```

### Using the Tool

```java
// Register the tool
ToolExecutorRegistry registry = new ToolExecutorRegistry();
registry.register(new SendEmailTool());

// Define it for the agent
ToolDefinition emailTool = new ToolDefinition();
emailTool.setToolId("send-email");
emailTool.setName("Email Sender");
emailTool.addInputParameter("to", "string", "Recipient email");
emailTool.addInputParameter("subject", "string", "Email subject");
emailTool.addInputParameter("body", "string", "Email body");

// Give it to your agent
config.addTool(emailTool);
```

---

## Workflows - The Agent's Process

A **workflow** is the path an agent follows to complete a task. Like a flowchart!

### Simple Workflow

```
START
  ↓
Receive Task (Event)
  ↓
Select Tool to Use
  ↓
Execute Tool
  ↓
Did it work?
  ├─ Yes → Task Complete! ✓
  └─ No  → Try Again or Ask for Help
```

### Complete Workflow with Validation

```
START
  ↓
[1] Receive Task
  ↓
[2] Choose Tool
  ↓
[3] Execute Tool
  ↓
[4] Validate Result
  ↓
Result OK?
  ├─ Yes → DONE! ✓
  │
  └─ No → Can Fix?
          ├─ Yes → [5] Auto-Correct → Back to [3]
          └─ No  → [6] Escalate to Supervisor
```

### In Code

```java
AgentConfig config = new AgentConfig();

// Allow retries
config.setMaxIterations(3);

// Enable validation
config.setValidationEnabled(true);

// Enable correction
config.setCorrectionEnabled(true);

// Enable supervisor review
config.setSupervisorReviewEnabled(true);
```

---

## Context and Memory

**Context** is what the agent remembers. Like your brain's memory!

### Types of Memory

#### 🧠 Short-Term Memory (Working Memory)

Like remembering a phone number long enough to dial it.

**Use for:**
- Current conversation
- Recent tool results
- Temporary calculations

**Example:**
```java
ShortTermMemory memory = new ShortTermMemory(50);  // Remember 50 items
memory.add(new ContextItem(
    "User asked about order #12345",
    ContextPriority.SHOULD,
    MemoryType.SHORT_TERM
));
```

#### 📚 Long-Term Memory (Permanent Memory)

Like remembering your home address forever.

**Use for:**
- User preferences
- Important facts
- Historical data

**Example:**
```java
LongTermMemory memory = new LongTermMemory();
memory.addFact(new ContextItem(
    "Customer John Doe prefers email over phone",
    ContextPriority.MUST,
    MemoryType.LONG_TERM
));
```

#### 🎯 Steering State (Rules and Policies)

Like company policies you must follow.

**Use for:**
- Business rules
- Constraints
- Guidelines

**Example:**
```java
SteeringState rules = new SteeringState();
rules.addMust("policy-01",
    "Never share customer credit card numbers",
    "must redact payment info");
rules.addShould("policy-02",
    "Respond within 24 hours",
    "should respond quickly");
```

### Memory Priority (MoSCoW)

Not all memories are equally important! We prioritize them:

| Priority | Meaning | Example | What Happens |
|----------|---------|---------|--------------|
| **MUST** | Critical, never forget | Customer password | Always kept |
| **SHOULD** | Important, keep if possible | Recent chat history | Compressed if needed |
| **COULD** | Nice to have | User's timezone | Removed if space needed |
| **WONT** | Not needed | Spam messages | Immediately deleted |

```java
// Critical information
ContextItem important = new ContextItem(
    "User's account ID: 12345",
    ContextPriority.MUST,      // Never forget this!
    MemoryType.LONG_TERM
);

// Less critical
ContextItem useful = new ContextItem(
    "User browsed FAQ page",
    ContextPriority.COULD,     // Can forget if needed
    MemoryType.SHORT_TERM
);
```

### When Memory Gets Full

Like cleaning out your closet when it's too full!

```
Memory Full! (4000/4000 tokens)
  ↓
Automatic Compaction Starts
  ↓
[Phase 1] Delete WONT items (trash)
  ↓
[Phase 2] Remove low-value COULD items
  ↓
[Phase 3] Compress SHOULD items (summarize)
  ↓
[Phase 4] Keep all MUST items (never delete!)
  ↓
[Phase 5] Archive valuable items to long-term storage
  ↓
Memory Available! (2800/4000 tokens)
```

---

## Validation and Correction

**Validation** is checking if work is correct. **Correction** is fixing mistakes.

### Why Validate?

Agents make mistakes, just like people! Validation catches them.

```
Agent: "Customer's refund is $99,999.00"
Validator: "Wait, the order was only $99.99! That's wrong!"
Agent: "Oops, let me recalculate... $99.99 refund"
Validator: "That's correct! ✓"
```

### Validation Example

```java
public class RefundValidator {

    public ValidationResult validate(Object result, Object originalData) {
        double refundAmount = (double) result;
        double orderTotal = (double) originalData;

        ValidationResult validation = new ValidationResult();

        // Check if refund is reasonable
        if (refundAmount > orderTotal) {
            validation.setValid(false);
            validation.addError("Refund amount exceeds order total!");
        } else if (refundAmount < 0) {
            validation.setValid(false);
            validation.addError("Refund amount cannot be negative!");
        } else {
            validation.setValid(true);
        }

        return validation;
    }
}
```

### Correction Flow

```
[1] Tool executes → Result: $99,999.00
  ↓
[2] Validation → FAIL: "Too high!"
  ↓
[3] Correction attempt #1 → Result: $9,999.00
  ↓
[4] Validation → FAIL: "Still too high!"
  ↓
[5] Correction attempt #2 → Result: $99.99
  ↓
[6] Validation → PASS: "Correct!" ✓
```

### Supervisor Escalation

If the agent can't fix it after multiple tries, ask for help!

```
Correction failed 3 times
  ↓
Escalate to Supervisor
  ↓
Human Reviews → Approves/Rejects
  ↓
Continue or Abort
```

---

## RAG - Knowledge Retrieval

**RAG** (Retrieval Augmented Generation) lets your AI know about YOUR data.

### The Problem

**Without RAG:**
```
User: "What's our company's return policy?"
AI: "I don't know, I wasn't trained on that."
```

**With RAG:**
```
User: "What's our company's return policy?"
AI searches your documents → Finds return policy → Reads it
AI: "Your return policy allows 30-day returns with receipt..."
```

### How RAG Works

```
STEP 1: Ingest Documents
Your Documents → Split into Chunks → Create Embeddings → Store in Database

STEP 2: Search
User Question → Create Embedding → Search Similar Chunks → Retrieve Top Matches

STEP 3: Generate Answer
Retrieved Chunks + User Question → Send to AI → Get Answer
```

### RAG Example

```java
// Step 1: Ingest a document
DocumentIngestionToolExecutor ingestion = new DocumentIngestionToolExecutor();
Map<String, Object> ingestParams = new HashMap<>();
ingestParams.put("content", "Our return policy allows 30-day returns...");
ingestParams.put("chunk_size", 500);
ingestParams.put("chunk_overlap", 50);
ingestion.execute(ingestParams);

// Step 2: Search
SemanticSearchToolExecutor search = new SemanticSearchToolExecutor();
Map<String, Object> searchParams = new HashMap<>();
searchParams.put("query", "return policy");
searchParams.put("max_results", 5);
List<String> results = (List<String>) search.execute(searchParams).get();

// Step 3: Use results to generate answer
RagToolExecutor rag = new RagToolExecutor();
Map<String, Object> ragParams = new HashMap<>();
ragParams.put("query", "What's the return policy?");
ragParams.put("context", results);
String answer = (String) rag.execute(ragParams).get();
```

### RAG Tools Included

| Tool | Purpose | When to Use |
|------|---------|-------------|
| **DocumentIngestionTool** | Store documents | When adding new documents |
| **SemanticSearchTool** | Find similar text | When looking up information |
| **RagToolExecutor** | Complete RAG pipeline | When answering questions |
| **EmbeddingToolExecutor** | Convert text to vectors | When building custom search |

---

## Apache Flink - The Engine

**Apache Flink** is like a factory assembly line that processes data.

### Why Flink?

| Feature | What It Means | Why It Matters |
|---------|---------------|----------------|
| **Exactly-Once** | Never lose or duplicate data | Reliable results |
| **Scalable** | Handle 1 or 1 million events | Grows with your needs |
| **Fault-Tolerant** | Survives server crashes | Always available |
| **Stateful** | Remembers context | Agents don't forget |

### Flink Concepts

#### Streams

Think of streams as conveyor belts carrying events:

```
Events → [Agent 1] → [Agent 2] → [Agent 3] → Results
```

#### Keying

Group related events together:

```
User A's events → Agent Instance 1
User B's events → Agent Instance 2
User C's events → Agent Instance 3
```

This ensures each user's conversation goes to the same agent (maintains context).

#### State

Agents remember things using Flink state:

```java
// Flink manages this automatically!
ValueState<AgentContext> context = getRuntimeContext().getState(
    new ValueStateDescriptor<>("agent-context", AgentContext.class)
);

// Get current context
AgentContext current = context.value();

// Update context
current.addMemory("User asked about pricing");
context.update(current);
```

#### Checkpoints

Flink saves progress regularly (like video game saves):

```
Time: 10:00 - Checkpoint 1 saved ✓
Time: 10:05 - Checkpoint 2 saved ✓
Time: 10:10 - Server crashes! ☠️
Time: 10:11 - Restart from Checkpoint 2 ✓
No data lost!
```

### You Don't Need to Know Flink!

The framework handles all the Flink complexity:
- ✅ Streaming setup - Done for you
- ✅ State management - Automatic
- ✅ Checkpointing - Configured
- ✅ Scaling - Just change parallelism

**You just write agent logic!**

---

## 🎯 Putting It All Together

Here's how all the concepts work together:

```
[1] EVENT arrives
     ↓
[2] AGENT receives it (Flink routes by key)
     ↓
[3] AGENT checks CONTEXT (What do I remember?)
     ↓
[4] AGENT selects TOOL (What should I use?)
     ↓
[5] TOOL executes (Does the work)
     ↓
[6] VALIDATOR checks result (Is it correct?)
     ↓
[7] If needed, CORRECTOR fixes issues
     ↓
[8] CONTEXT updated (Remember this!)
     ↓
[9] WORKFLOW continues or completes
     ↓
[10] Flink saves state (Checkpoint)
```

### Real-World Example: Customer Support Bot

```java
// [1-2] Customer message arrives
AgentEvent event = new AgentEvent();
event.setEventType(AgentEventType.FLOW_STARTED);
event.putData("message", "I want to return my order");

// [3] Check context - Do we know this customer?
AgentContext context = getContext(event.getUserId());
String customerName = context.getLongTermMemory().get("name");

// [4-5] Use RAG tool to look up return policy
RagToolExecutor rag = new RagToolExecutor();
String policy = rag.execute(Map.of(
    "query", "return policy",
    "userId", event.getUserId()
)).get();

// [6-7] Validate the response
if (policyContainsRequiredInfo(policy)) {
    // [8] Update context - Remember we discussed returns
    context.addMemory(new ContextItem(
        "Discussed return policy",
        ContextPriority.SHOULD,
        MemoryType.SHORT_TERM
    ));

    // [9] Send response
    sendResponse(customerName + ", " + policy);
}

// [10] Flink automatically checkpoints
```

---

## 🎓 Key Takeaways

1. **Agents** are autonomous workers that complete tasks
2. **Events** are messages that tell agents what to do
3. **Tools** are the agent's capabilities
4. **Workflows** define the process agents follow
5. **Context** is the agent's memory (short and long-term)
6. **Validation** checks if work is correct
7. **RAG** gives agents knowledge of your documents
8. **Flink** ensures reliability and scalability

---

## 📚 Next Steps

Now that you understand the concepts:

1. **Try the examples** - See these concepts in action ([EXAMPLES.md](EXAMPLES.md))
2. **Build your own** - Create a simple agent with what you learned
3. **Explore advanced topics** - Read the full framework docs ([AGENT_FRAMEWORK.md](AGENT_FRAMEWORK.md))
4. **Get help** - Check troubleshooting if you get stuck ([TROUBLESHOOTING.md](TROUBLESHOOTING.md))

**You now understand how Agentic Flink works!** 🎉
