-- Agentic Flink - PostgreSQL Schema
-- Database schema for conversation storage and context management

-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Conversations table - stores full conversation history
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flow_id VARCHAR(255) NOT NULL UNIQUE,
    user_id VARCHAR(255) NOT NULL,
    agent_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'active',
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Context items table - stores individual context items for conversations
CREATE TABLE IF NOT EXISTS context_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    priority VARCHAR(20) NOT NULL CHECK (priority IN ('MUST', 'SHOULD', 'COULD', 'WONT')),
    memory_type VARCHAR(20) NOT NULL CHECK (memory_type IN ('WORKING', 'SHORT_TERM', 'LONG_TERM', 'SYSTEM', 'STEERING')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    accessed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    access_count INT DEFAULT 1,
    relevancy_score DECIMAL(5, 4) DEFAULT 0.0,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Messages table - stores individual messages in conversations
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Tool executions table - stores tool execution history
CREATE TABLE IF NOT EXISTS tool_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    tool_name VARCHAR(255) NOT NULL,
    tool_input JSONB NOT NULL,
    tool_output JSONB,
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INT,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Validation results table - stores validation and correction attempts
CREATE TABLE IF NOT EXISTS validation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    tool_execution_id UUID REFERENCES tool_executions(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    is_valid BOOLEAN NOT NULL,
    feedback TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_conversations_flow_id ON conversations(flow_id);
CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_conversations_status ON conversations(status);
CREATE INDEX IF NOT EXISTS idx_conversations_updated_at ON conversations(updated_at);

CREATE INDEX IF NOT EXISTS idx_context_items_conversation_id ON context_items(conversation_id);
CREATE INDEX IF NOT EXISTS idx_context_items_priority ON context_items(priority);
CREATE INDEX IF NOT EXISTS idx_context_items_memory_type ON context_items(memory_type);
CREATE INDEX IF NOT EXISTS idx_context_items_accessed_at ON context_items(accessed_at);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);

CREATE INDEX IF NOT EXISTS idx_tool_executions_conversation_id ON tool_executions(conversation_id);
CREATE INDEX IF NOT EXISTS idx_tool_executions_status ON tool_executions(status);
CREATE INDEX IF NOT EXISTS idx_tool_executions_started_at ON tool_executions(started_at);

CREATE INDEX IF NOT EXISTS idx_validation_results_conversation_id ON validation_results(conversation_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_tool_execution_id ON validation_results(tool_execution_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at on conversations
CREATE TRIGGER update_conversations_updated_at BEFORE UPDATE ON conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to update accessed_at and access_count on context items
CREATE OR REPLACE FUNCTION update_context_item_access()
RETURNS TRIGGER AS $$
BEGIN
    NEW.accessed_at = CURRENT_TIMESTAMP;
    NEW.access_count = OLD.access_count + 1;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Insert sample data for testing (optional - comment out for production)
/*
INSERT INTO conversations (flow_id, user_id, agent_id, status) VALUES
    ('test-flow-001', 'user-12345', 'support-agent-01', 'active'),
    ('test-flow-002', 'user-67890', 'support-agent-01', 'active');

INSERT INTO context_items (conversation_id, content, priority, memory_type) VALUES
    ((SELECT id FROM conversations WHERE flow_id = 'test-flow-001'),
     'Customer has premium subscription', 'MUST', 'SYSTEM'),
    ((SELECT id FROM conversations WHERE flow_id = 'test-flow-001'),
     'Previous interaction was positive', 'SHOULD', 'SHORT_TERM');

INSERT INTO messages (conversation_id, role, content) VALUES
    ((SELECT id FROM conversations WHERE flow_id = 'test-flow-001'),
     'user', 'I need help with my order'),
    ((SELECT id FROM conversations WHERE flow_id = 'test-flow-001'),
     'assistant', 'I''d be happy to help you with your order. Could you provide your order number?');
*/

-- Grant permissions (adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flink_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flink_user;

-- Display schema info
\echo 'Agentic Flink database schema created successfully!'
\dt

