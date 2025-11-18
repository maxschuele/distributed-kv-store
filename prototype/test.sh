#!/bin/bash

# Test script for distributed key-value store

echo "==================================="
echo "Distributed KV Store Test Script"
echo "==================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Wait for API to be ready
wait_for_api() {
    local port=$1
    local max_attempts=10
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$port/health > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    return 1
}

echo "${BLUE}Step 1: Testing Health Endpoints${NC}"
echo "-----------------------------------"
curl -s http://localhost:9000/health | python3 -m json.tool
echo ""
sleep 1

echo "${BLUE}Step 2: Checking Cluster Status${NC}"
echo "-----------------------------------"
curl -s http://localhost:9000/cluster | python3 -m json.tool
echo ""
sleep 1

echo "${BLUE}Step 3: Writing Data to Leader${NC}"
echo "-----------------------------------"

# Write multiple key-value pairs
echo "Writing: user1 = Alice"
curl -X POST http://localhost:9000/set \
  -H "Content-Type: application/json" \
  -d '{"key": "user1", "value": "Alice"}' 2>/dev/null
echo ""

sleep 1

echo "Writing: user2 = Bob"
curl -X POST http://localhost:9000/set \
  -H "Content-Type: application/json" \
  -d '{"key": "user2", "value": "Bob"}' 2>/dev/null
echo ""

sleep 1

echo "Writing: city = Berlin"
curl -X POST http://localhost:9000/set \
  -H "Content-Type: application/json" \
  -d '{"key": "city", "value": "Berlin"}' 2>/dev/null
echo ""

sleep 2  # Wait for replication

echo ""
echo "${BLUE}Step 4: Reading from Leader (port 9000)${NC}"
echo "-----------------------------------"
echo "Reading user1:"
curl -s http://localhost:9000/get?key=user1 | python3 -m json.tool
echo ""
echo "Reading user2:"
curl -s http://localhost:9000/get?key=user2 | python3 -m json.tool
echo ""

echo "${BLUE}Step 5: Reading from Follower (port 9001) - Testing Replication${NC}"
echo "-----------------------------------"
echo "Reading user1:"
curl -s http://localhost:9001/get?key=user1 | python3 -m json.tool
echo ""
echo "Reading user2:"
curl -s http://localhost:9001/get?key=user2 | python3 -m json.tool
echo ""

echo "${BLUE}Step 6: Listing All Keys${NC}"
echo "-----------------------------------"
curl -s http://localhost:9000/keys | python3 -m json.tool
echo ""

echo "${BLUE}Step 7: Testing Write to Follower (Should Fail)${NC}"
echo "-----------------------------------"
echo "Attempting to write to follower on port 9001..."
curl -X POST http://localhost:9001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "test", "value": "should-fail"}' 2>/dev/null
echo ""
echo ""

echo "${GREEN}==================================="
echo "Test Complete!"
echo "===================================${NC}"
