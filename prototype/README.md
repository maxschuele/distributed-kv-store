# Distributed Key-Value Store

A production-ready distributed key-value store implemented in Go with no external middleware dependencies. Features replication groups with leader-follower architecture, consistent hashing for data partitioning, and automatic node discovery.

## Architecture

### Components

- **Discovery Service**: UDP broadcast-based node discovery (no Consul/etcd required)
- **Consistent Hashing**: Automatic data partitioning across replication groups using virtual nodes
- **Replication Groups**: Each group has one leader and multiple followers
- **Storage Engine**: Thread-safe in-memory key-value store with versioning
- **HTTP API**: RESTful interface for client operations

### Design Principles

- No middleware dependencies (no Raft, etcd, Consul, etc.)
- Simple string-based wire protocols
- Leader handles writes, replicates to followers
- Followers replicate data from their group's leader
- Automatic cluster membership via UDP broadcasts

## Project Structure

```
distributed-kv-store/
├── cmd/node/          # Application entry point
├── cluster/           # Node metadata and membership management
├── discovery/         # UDP-based node discovery
├── sharding/          # Consistent hashing implementation
├── replication/       # Leader-follower replication logic
├── storage/           # In-memory key-value store
└── api/              # HTTP REST API server
```

## Building

```bash
cd distributed-kv-store
go build -o kvnode cmd/node/main.go
```

## Running a Cluster

### Start a 3-Node Cluster (1 Group)

**Terminal 1 - Leader of Group 0:**
```bash
./kvnode -port 8000 -group 0 -leader -api 9000
```

**Terminal 2 - Follower 1 of Group 0:**
```bash
./kvnode -port 8001 -group 0 -api 9001
```

**Terminal 3 - Follower 2 of Group 0:**
```bash
./kvnode -port 8002 -group 0 -api 9002
```

### Start a Multi-Group Cluster

For sharding across multiple replication groups:

**Group 0:**
```bash
./kvnode -port 8000 -group 0 -leader -api 9000
./kvnode -port 8001 -group 0 -api 9001
```

**Group 1:**
```bash
./kvnode -port 8010 -group 1 -leader -api 9010
./kvnode -port 8011 -group 1 -api 9011
```

**Group 2:**
```bash
./kvnode -port 8020 -group 2 -leader -api 9020
./kvnode -port 8021 -group 2 -api 9021
```

## Command-Line Flags

- `-port <int>`: Port for cluster communication (default: 8000)
- `-group <int>`: Replication group ID (default: 0)
- `-leader`: Set this node as the group leader
- `-api <int>`: HTTP API port (default: 9000)

## API Usage

### Set a Key-Value Pair (Leader Only)

```bash
curl -X POST http://localhost:9000/set \
  -H "Content-Type: application/json" \
  -d '{"key": "username", "value": "alice"}'
```

Response:
```json
{"status":"ok","key":"username"}
```

### Get a Value

```bash
curl http://localhost:9000/get?key=username
```

Response:
```json
{"key":"username","value":"alice"}
```

### Delete a Key (Leader Only)

```bash
curl -X DELETE http://localhost:9000/delete?key=username
```

### List All Keys

```bash
curl http://localhost:9000/keys
```

Response:
```json
{"keys":["username","email","city"],"count":3}
```

### View Cluster State

```bash
curl http://localhost:9000/cluster
```

Response:
```json
{
  "self_node_id": "a1b2c3d4e5f6g7h8",
  "self_group": 0,
  "is_leader": true,
  "nodes": [
    {
      "node_id": "a1b2c3d4e5f6g7h8",
      "address": "127.0.0.1:8000",
      "group_id": 0,
      "is_leader": true
    },
    {
      "node_id": "b2c3d4e5f6g7h8i9",
      "address": "127.0.0.1:8001",
      "group_id": 0,
      "is_leader": false
    }
  ],
  "node_count": 2
}
```

### Health Check

```bash
curl http://localhost:9000/health
```

Response:
```json
{"status":"healthy","role":"leader","group":0,"keys":5}
```

## Testing Replication

### 1. Write to Leader

```bash
curl -X POST http://localhost:9000/set \
  -H "Content-Type: application/json" \
  -d '{"key": "test", "value": "replicated-data"}'
```

### 2. Read from Follower

Wait a moment for replication, then:

```bash
curl http://localhost:9001/get?key=test
```

You should see the same value, demonstrating successful replication.

## How It Works

### Discovery

1. Each node broadcasts its metadata (ID, address, group, role) via UDP every 5 seconds
2. All nodes listen on UDP port 7946 for announcements
3. Membership table automatically updates as nodes join/leave
4. Stale nodes (no heartbeat for 30s) are removed

### Replication

1. Client sends write request to a leader node's HTTP API
2. Leader writes to its local storage
3. Leader asynchronously sends replication messages to all followers in its group
4. Followers receive updates via TCP and apply them to their local storage
5. Each write is versioned to handle ordering

### Consistent Hashing

1. Each replication group is mapped to 150 virtual nodes on a hash ring
2. When a key is queried, it's hashed and mapped to the nearest group on the ring
3. This provides balanced data distribution and minimal redistribution on topology changes

## Features

✅ **No External Dependencies**: Pure Go implementation  
✅ **Automatic Discovery**: Nodes find each other via UDP broadcasts  
✅ **Consistent Hashing**: Scalable data partitioning  
✅ **Replication**: Data redundancy within groups  
✅ **Versioning**: Conflict-free updates  
✅ **RESTful API**: Simple HTTP interface  
✅ **Clean Architecture**: Modular, testable design

## Limitations

- **In-Memory Only**: Data not persisted to disk
- **Best-Effort Replication**: No strong consistency guarantees
- **No Leader Election**: Leaders must be manually designated
- **Single-Host Broadcast**: UDP discovery works on same network segment
- **No Authentication**: API endpoints are unauthenticated

## Production Considerations

For production use, consider adding:

- Persistent storage (RocksDB, BadgerDB)
- Leader election (Raft, Paxos)
- Strong consistency options (quorum writes)
- TLS/authentication for API
- Metrics and monitoring
- Cross-datacenter replication

## License

MIT License - Feel free to use and modify as needed.

## Contributing

This is a learning project demonstrating distributed systems concepts. Contributions welcome!
