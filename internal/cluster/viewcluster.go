package cluster

import (
	"distributed-kv-store/internal/logger"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ClusterGroupView struct {
	mu    sync.RWMutex
	log   *logger.Logger
	nodes map[uuid.UUID]*NodeRecord // groupID -> NodeRecord
}

func NewClusterView(log *logger.Logger) *ClusterGroupView {
	return &ClusterGroupView{
		mu:    sync.RWMutex{},
		log:   log,
		nodes: make(map[uuid.UUID]*NodeRecord),
	}
}

func (cv *ClusterGroupView) AddOrUpdateNode(i NodeInfo) {
	cv.mu.Lock()
	defer cv.mu.Unlock()

	if record, exists := cv.nodes[i.GroupID]; exists {
		record.LastSeen = time.Now()
	} else {
		cv.nodes[i.GroupID] = &NodeRecord{
			Info:         i,
			LastSeen:     time.Now(),
			DiscoveredAt: time.Now(),
		}
		cv.log.Info("[ClusterView] Discovered new group: %s (node: %s)", i.GroupID.String(), i.ID.String())
	}
}

func (cv *ClusterGroupView) GetNodes() []NodeInfo {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(cv.nodes))
	for _, record := range cv.nodes {
		nodes = append(nodes, record.Info)
	}
	return nodes
}

func (cv *ClusterGroupView) GetNode(groupID uuid.UUID) (NodeInfo, bool) {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	if record, ok := cv.nodes[groupID]; ok {
		return record.Info, ok
	}

	return NodeInfo{}, false
}

func (cv *ClusterGroupView) RemoveStaleNodes(timeout time.Duration, handleNodeRemoval HandleNodeRemoval) {
	cv.mu.Lock()
	defer cv.mu.Unlock()

	now := time.Now()
	for groupID, record := range cv.nodes {

		if now.Sub(record.LastSeen) > timeout {
			go handleNodeRemoval(record.Info)
			delete(cv.nodes, groupID)
			cv.log.Info("[ClusterView] Removed stale group: %s (last seen: %v ago)", groupID, now.Sub(record.LastSeen))
		}
	}
}

func (cv *ClusterGroupView) Size() int {
	cv.mu.RLock()
	defer cv.mu.RUnlock()
	return len(cv.nodes)
}

func (cv *ClusterGroupView) StartHeartbeatMonitor(timeout time.Duration, checkInterval time.Duration, handleNodeRemoval HandleNodeRemoval) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		cv.RemoveStaleNodes(timeout, handleNodeRemoval)
	}
}
