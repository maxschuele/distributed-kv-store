package cluster

import (
	"fmt"
	"log"
	"sync"
)

type ReplicationGroup struct {
	mu           sync.RWMutex
	GroupID      int
	GroupSize    int
	GroupMembers map[string]NodeInfo // nodeID -> NodeInfo
}

func NewReplicationGroup(groupID int, groupSize int) *ReplicationGroup {
	return &ReplicationGroup{
		GroupID:      groupID,
		GroupSize:    groupSize,
		GroupMembers: make(map[string]NodeInfo),
	}
}

func (rg *ReplicationGroup) AddMember(node NodeInfo) error {
	rg.mu.Lock()
	defer rg.mu.Unlock()
	if len(rg.GroupMembers) == rg.GroupSize {
		return fmt.Errorf("[ReplicationGroup %d] Can't add member %s since group is full", rg.GroupID, node.NodeID)
	}

	if _, exists := rg.GroupMembers[node.NodeID]; exists {
		log.Printf("[ReplicationGroup %d] member: %s is already in the Replication Group", rg.GroupID, node.NodeID)
	} else {
		rg.GroupMembers[node.NodeID] = node
		log.Printf("[ReplicationGroup %d] Added member: %s", rg.GroupID, node.NodeID)
	}
	return nil
}

func (rg *ReplicationGroup) RemoveMember(node NodeInfo) {
	rg.mu.Lock()
	defer rg.mu.Unlock()

	if _, exists := rg.GroupMembers[node.NodeID]; exists {
		delete(rg.GroupMembers, node.NodeID)
		log.Printf("[ReplicationGroup %d] Removed member: %s", rg.GroupID, node.NodeID)
	}
}

func (rg *ReplicationGroup) GetMembers() []NodeInfo {
	rg.mu.RLock()
	defer rg.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(rg.GroupMembers))
	for _, member := range rg.GroupMembers {
		nodes = append(nodes, member)
	}
	return nodes
}
