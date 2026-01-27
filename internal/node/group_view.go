package node

import (
	"bytes"
	"distributed-kv-store/internal/logger"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// GroupView maintains knowledge of all discovered nodes in the group
type GroupView struct {
	mu    sync.RWMutex
	log   *logger.Logger
	nodes map[uuid.UUID]*NodeRecord // nodeID -> NodeRecord
}

// NodeRecord stores info about a discovered node
type NodeRecord struct {
	Info         NodeInfo
	LastSeen     time.Time
	DiscoveredAt time.Time
}

// NewGroupView creates a new group view
func NewGroupView(log *logger.Logger) *GroupView {
	return &GroupView{
		mu:    sync.RWMutex{},
		log:   log,
		nodes: make(map[uuid.UUID]*NodeRecord),
	}
}

// AddOrUpdateNode adds or updates a node in the view
func (gv *GroupView) AddOrUpdateNode(i NodeInfo) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	if record, exists := gv.nodes[i.ID]; exists {
		record.LastSeen = time.Now()
		record.Info = i
		gv.log.Info("[GroupView] Updated node: %s (last seen: now)\n", i.ID)
	} else {
		gv.nodes[i.ID] = &NodeRecord{
			Info:         i,
			LastSeen:     time.Now(),
			DiscoveredAt: time.Now(),
		}
		gv.log.Info("[GroupView] Discovered new node: %s\n", i.ID)
	}
}

// GetNodes returns all known nodes
func (gv *GroupView) GetNodes() []NodeInfo {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(gv.nodes))
	for _, record := range gv.nodes {
		nodes = append(nodes, record.Info)
	}
	return nodes
}

// GetNode returns a specific node by ID
func (gv *GroupView) GetNode(id uuid.UUID) (NodeInfo, error) {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	record, exists := gv.nodes[id]
	if !exists {
		return NodeInfo{}, fmt.Errorf("[GroupView] No entry for Node with UUID %s found", id.String())
	}
	return record.Info, nil
}

// RemoveStaleNodes removes nodes that haven't been seen in timeout duration
func (gv *GroupView) RemoveStaleNodes(timeout time.Duration) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	now := time.Now()
	removed := 0
	for nodeID, record := range gv.nodes {
		if now.Sub(record.LastSeen) > timeout {
			delete(gv.nodes, nodeID)
			removed++
			gv.log.Info("[GroupView] Removed stale node: %s (last seen: %v ago)\n", nodeID, now.Sub(record.LastSeen))
		}
	}

	if removed > 0 {
		gv.log.Info("[GroupView] Removed %d stale nodes\n", removed)
	}
}

// Size returns the number of known nodes
func (gv *GroupView) Size() int {
	gv.mu.RLock()
	defer gv.mu.RUnlock()
	return len(gv.nodes)
}

// StartHeartbeatMonitor starts a goroutine that periodically removes stale nodes
func (gv *GroupView) StartHeartbeatMonitor(timeout time.Duration, checkInterval time.Duration) {
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for range ticker.C {
			gv.RemoveStaleNodes(timeout)
		}
	}()
}

// UpdateHeartbeat updates the last-seen time without changing node info
func (gv *GroupView) UpdateHeartbeat(id uuid.UUID) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	if record, exists := gv.nodes[id]; exists {
		record.LastSeen = time.Now()
	}
}

// SortNodesByID returns a list of nodes sorted by their UUIDs
func (gv *GroupView) SortNodesByID() []NodeInfo {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	ids := make([]uuid.UUID, 0, len(gv.nodes))
	for id := range gv.nodes {
		ids = append(ids, id)
	}
	// Sort UUIDs
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})

	sortedNodes := make([]NodeInfo, 0, len(gv.nodes))
	for _, id := range ids {
		sortedNodes = append(sortedNodes, gv.nodes[id].Info)
	}
	return sortedNodes
}

// GetSuccessor returns the successor node of the given node ID
func (gv *GroupView) GetSuccessor(id uuid.UUID) (NodeInfo, error) {
	sortedNodes := gv.SortNodesByID()
	for i, node := range sortedNodes {
		if node.ID == id {
			successorIndex := (i + 1) % len(sortedNodes)
			return sortedNodes[successorIndex], nil
		}
	}
	return NodeInfo{}, fmt.Errorf("[GroupView] No Successor found for node with uuid: %s", id.String())
}
