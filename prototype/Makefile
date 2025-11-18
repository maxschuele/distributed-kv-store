# Makefile for Distributed Key-Value Store

.PHONY: build clean run-leader run-follower1 run-follower2 test-write test-read cluster-status help

# Build the binary
build:
	@echo "Building kvnode..."
	@go build -o kvnode cmd/node/main.go
	@echo "Build complete: ./kvnode"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f kvnode
	@echo "Clean complete"

# Run a leader node (Group 0)
run-leader: build
	@echo "Starting leader node (Group 0, Port 8000, API 9000)..."
	@./kvnode -port 8000 -group 0 -leader -api 9000

# Run first follower (Group 0)
run-follower1: build
	@echo "Starting follower 1 (Group 0, Port 8001, API 9001)..."
	@./kvnode -port 8001 -group 0 -api 9001

# Run second follower (Group 0)
run-follower2: build
	@echo "Starting follower 2 (Group 0, Port 8002, API 9002)..."
	@./kvnode -port 8002 -group 0 -api 9002

# Test write operation
test-write:
	@echo "Writing test data..."
	@curl -X POST http://localhost:9000/set \
		-H "Content-Type: application/json" \
		-d '{"key": "test", "value": "hello-world"}'
	@echo ""

# Test read operation
test-read:
	@echo "Reading from leader (9000)..."
	@curl http://localhost:9000/get?key=test
	@echo ""
	@echo "Reading from follower (9001)..."
	@curl http://localhost:9001/get?key=test
	@echo ""

# Check cluster status
cluster-status:
	@echo "Cluster status from port 9000:"
	@curl -s http://localhost:9000/cluster | python3 -m json.tool
	@echo ""

# Show help
help:
	@echo "Distributed Key-Value Store - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build the kvnode binary"
	@echo "  clean          - Remove build artifacts"
	@echo "  run-leader     - Start a leader node"
	@echo "  run-follower1  - Start first follower node"
	@echo "  run-follower2  - Start second follower node"
	@echo "  test-write     - Write test data to leader"
	@echo "  test-read      - Read from leader and follower"
	@echo "  cluster-status - Display cluster information"
	@echo "  help           - Show this help message"
	@echo ""
	@echo "Quick start:"
	@echo "  1. Terminal 1: make run-leader"
	@echo "  2. Terminal 2: make run-follower1"
	@echo "  3. Terminal 3: make run-follower2"
	@echo "  4. Terminal 4: make test-write && make test-read"
