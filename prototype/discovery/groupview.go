package discovery

import (
	"distributed-kv/cluster"
	"log"
	"sync"
	"time"
)

// GroupView maintains knowledge of all discovered nodes in the group
type GroupView struct {
	mu    sync.RWMutex
	nodes map[string]*NodeRecord // nodeID -> NodeRecord
}

// NodeRecord stores info about a discovered node
type NodeRecord struct {
	Info         cluster.NodeInfo
	LastSeen     time.Time
	DiscoveredAt time.Time
}

// NewGroupView creates a new group view
func NewGroupView() *GroupView {
	return &GroupView{
		nodes: make(map[string]*NodeRecord),
	}
}

// AddOrUpdateNode adds or updates a node in the view
func (gv *GroupView) AddOrUpdateNode(node cluster.NodeInfo) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	if record, exists := gv.nodes[node.NodeID]; exists {
		record.LastSeen = time.Now()
		record.Info = node
		log.Printf("[GroupView] Updated node: %s (last seen: now)\n", node.NodeID)
	} else {
		gv.nodes[node.NodeID] = &NodeRecord{
			Info:         node,
			LastSeen:     time.Now(),
			DiscoveredAt: time.Now(),
		}
		log.Printf("[GroupView] Discovered new node: %s\n", node.NodeID)
	}
}

// GetNodes returns all known nodes
func (gv *GroupView) GetNodes() []cluster.NodeInfo {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	nodes := make([]cluster.NodeInfo, 0, len(gv.nodes))
	for _, record := range gv.nodes {
		nodes = append(nodes, record.Info)
	}
	return nodes
}

// GetNode returns a specific node by ID
func (gv *GroupView) GetNode(nodeID string) (cluster.NodeInfo, bool) {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	record, exists := gv.nodes[nodeID]
	if !exists {
		return cluster.NodeInfo{}, false
	}
	return record.Info, true
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
			log.Printf("[GroupView] Removed stale node: %s (last seen: %v ago)\n", nodeID, now.Sub(record.LastSeen))
		}
	}
	if removed > 0 {
		log.Printf("[GroupView] Removed %d stale nodes\n", removed)
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
func (gv *GroupView) UpdateHeartbeat(nodeID string) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	if record, exists := gv.nodes[nodeID]; exists {
		record.LastSeen = time.Now()
	}
}
