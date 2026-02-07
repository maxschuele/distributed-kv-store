package cluster

import (
	"bytes"
	"distributed-kv-store/internal/logger"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type HandleNodeRemoval func(removedNode NodeInfo)

// ReplicationGroupView maintains knowledge of all discovered nodes in the group
type ReplicationGroupView struct {
	ownID uuid.UUID
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
func NewGroupView(ownID uuid.UUID, log *logger.Logger) *ReplicationGroupView {
	return &ReplicationGroupView{
		ownID: ownID,
		mu:    sync.RWMutex{},
		log:   log,
		nodes: make(map[uuid.UUID]*NodeRecord),
	}
}

// AddOrUpdateNode adds or updates a node in the view
func (gv *ReplicationGroupView) AddOrUpdateNode(i NodeInfo) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	if record, exists := gv.nodes[i.ID]; exists {
		record.LastSeen = time.Now()
		// Don't overwrite our own node's info
		if i.ID != gv.ownID {
			//record.Info = i
			record.Info.GroupID = i.GroupID
			record.Info.IsLeader = i.IsLeader
		}
	} else {
		gv.nodes[i.ID] = &NodeRecord{
			Info:         i,
			LastSeen:     time.Now(),
			DiscoveredAt: time.Now(),
		}
		gv.log.Info("[ReplicationView] Discovered new node: %s", i.ID.String())
	}
}

// GetNodes returns all known nodes
func (gv *ReplicationGroupView) GetNodes() []NodeInfo {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(gv.nodes))
	for _, record := range gv.nodes {
		nodes = append(nodes, record.Info)
	}
	return nodes
}

// GetNode returns a specific node by ID
func (gv *ReplicationGroupView) GetNode(id uuid.UUID) (NodeInfo, error) {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	record, exists := gv.nodes[id]
	if !exists {
		return NodeInfo{}, fmt.Errorf("[ReplicationView] No entry for Node with UUID %s found", id.String())
	}
	return record.Info, nil
}

// RemoveStaleNodes removes nodes that haven't been seen in timeout duration
func (gv *ReplicationGroupView) RemoveStaleNodes(timeout time.Duration, handleNodeRemoval HandleNodeRemoval) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	now := time.Now()
	for nodeID, record := range gv.nodes {
		if nodeID == gv.ownID {
			continue
		}

		if now.Sub(record.LastSeen) > timeout {
			go handleNodeRemoval(record.Info)
			delete(gv.nodes, nodeID)
			gv.log.Info("[ReplicationView] Removed stale node: %s (last seen: %v ago)", nodeID, now.Sub(record.LastSeen))
		}
	}

}

// Size returns the number of known nodes
func (gv *ReplicationGroupView) Size() int {
	gv.mu.RLock()
	defer gv.mu.RUnlock()
	return len(gv.nodes)
}

// StartHeartbeatMonitor starts a goroutine that periodically removes stale nodes
func (gv *ReplicationGroupView) StartHeartbeatMonitor(timeout time.Duration, checkInterval time.Duration, initiateElection HandleNodeRemoval) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		gv.RemoveStaleNodes(timeout, initiateElection)
	}
}

// SortNodesByID returns a list of nodes sorted by their UUIDs
func (gv *ReplicationGroupView) SortNodesByID() []NodeInfo {
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
func (gv *ReplicationGroupView) GetSuccessor(id uuid.UUID) (NodeInfo, error) {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	if len(gv.nodes) == 0 {
		return NodeInfo{}, fmt.Errorf("[ReplicationView] No Successor found for node with uuid: %s (empty group)", id.String())
	}

	// Sort UUIDs
	ids := make([]uuid.UUID, 0, len(gv.nodes))
	for nodeID := range gv.nodes {
		ids = append(ids, nodeID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})

	// Find the node and return its successor
	for i, nodeID := range ids {
		if nodeID == id {
			successorIndex := (i + 1) % len(ids)
			return gv.nodes[ids[successorIndex]].Info, nil
		}
	}

	return NodeInfo{}, fmt.Errorf("[ReplicationView] No Successor found for node with uuid: %s", id.String())
}
