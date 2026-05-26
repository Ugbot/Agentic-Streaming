#!/bin/bash

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}Agentic Flink - Tiered Agent Demo${NC}"
echo -e "${CYAN}Real LLM integration with validation, execution, and supervision${NC}"
echo ""
echo -e "${YELLOW}Prerequisites: Ollama running with a model pulled (e.g. qwen2.5:3b)${NC}"
echo ""

if [ ! -d "target/classes" ]; then
    echo -e "${YELLOW}Compiling project...${NC}"
    mvn compile -q
    if [ $? -ne 0 ]; then
        echo -e "${RED}Compilation failed. Please check errors above.${NC}"
        exit 1
    fi
    echo -e "${GREEN}Compilation successful${NC}"
    echo ""
fi

echo -e "${GREEN}Starting tiered agent example...${NC}"
echo ""
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.TieredAgentExample" -q
