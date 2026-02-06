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

type InitiateElection func()

// ViewType distinguishes between replication and cluster group views
type ViewType int

const (
	ReplicationGroupViewType ViewType = iota
	ClusterGroupViewType
)

func (vt ViewType) String() string {
	switch vt {
	case ReplicationGroupViewType:
		return "ReplicationView"
	case ClusterGroupViewType:
		return "ClusterView"
	default:
		return "UnknownView"
	}
}

// GroupView maintains knowledge of all discovered nodes in the group
type GroupView struct {
	viewType ViewType
	ownID    uuid.UUID
	mu       sync.RWMutex
	log      *logger.Logger
	nodes    map[uuid.UUID]*NodeRecord // nodeID -> NodeRecord
}

// NodeRecord stores info about a discovered node
type NodeRecord struct {
	Info         NodeInfo
	LastSeen     time.Time
	DiscoveredAt time.Time
}

// NewGroupView creates a new group view
func NewGroupView(ownID uuid.UUID, log *logger.Logger, viewType ViewType) *GroupView {
	return &GroupView{
		ownID:    ownID,
		viewType: viewType,
		mu:       sync.RWMutex{},
		log:      log,
		nodes:    make(map[uuid.UUID]*NodeRecord),
	}
}

// AddOrUpdateNode adds or updates a node in the view
func (gv *GroupView) AddOrUpdateNode(i NodeInfo) {
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
		gv.log.Info("[%s] Discovered new node: %s", gv.viewType.String(), i.ID.String())
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
		return NodeInfo{}, fmt.Errorf("[%s] No entry for Node with UUID %s found", gv.viewType.String(), id.String())
	}
	return record.Info, nil
}

// RemoveStaleNodes removes nodes that haven't been seen in timeout duration
func (gv *GroupView) RemoveStaleNodes(timeout time.Duration, initiateElection InitiateElection) {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	now := time.Now()
	removed := 0
	for nodeID, record := range gv.nodes {
		if nodeID == gv.ownID {
			continue
		}

		if now.Sub(record.LastSeen) > timeout {
			gv.log.Info("removed node %s isleader: %s", nodeID, record.Info.IsLeader)
			if record.Info.IsLeader && gv.viewType == ReplicationGroupViewType {
				// schedule election when leader is removed
				go initiateElection()
			}
			delete(gv.nodes, nodeID)
			removed++
			gv.log.Info("[%s] Removed stale node: %s (last seen: %v ago)", gv.viewType.String(), nodeID, now.Sub(record.LastSeen))
		}
	}

	if removed > 0 {
		gv.log.Info("[%s] Removed %d stale nodes", gv.viewType.String(), removed)
	}
}

// Size returns the number of known nodes
func (gv *GroupView) Size() int {
	gv.mu.RLock()
	defer gv.mu.RUnlock()
	return len(gv.nodes)
}

// StartHeartbeatMonitor starts a goroutine that periodically removes stale nodes
func (gv *GroupView) StartHeartbeatMonitor(timeout time.Duration, checkInterval time.Duration, initiateElection InitiateElection) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		gv.RemoveStaleNodes(timeout, initiateElection)
	}
}

/*
-----NOT NECESSARY------
*/
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
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	if len(gv.nodes) == 0 {
		return NodeInfo{}, fmt.Errorf("[%s] No Successor found for node with uuid: %s (empty group)", gv.viewType.String(), id.String())
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

	return NodeInfo{}, fmt.Errorf("[%s] No Successor found for node with uuid: %s", gv.viewType.String(), id.String())
}
