package cluster

import (
	"sync"
	"time"
)

type Membership struct {
	self      NodeInfo
	mu        sync.RWMutex
	nodes     map[string]NodeInfo
	lastSeen  map[string]time.Time
	timeout   time.Duration
}

func NewMembership(self NodeInfo) *Membership {
	m := &Membership{
		self:     self,
		nodes:    make(map[string]NodeInfo),
		lastSeen: make(map[string]time.Time),
		timeout:  30 * time.Second,
	}
	m.nodes[self.NodeID] = self
	m.lastSeen[self.NodeID] = time.Now()
	return m
}

func (m *Membership) Update(node NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.NodeID] = node
	m.lastSeen[node.NodeID] = time.Now()
}

func (m *Membership) Snapshot() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, n)
	}
	return out
}

func (m *Membership) GetNode(nodeID string) (NodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[nodeID]
	return node, ok
}

func (m *Membership) GetLeader(groupID int) (NodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if node.GroupID == groupID && node.IsLeader {
			return node, true
		}
	}
	return NodeInfo{}, false
}

func (m *Membership) GetFollowers(groupID int) []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var followers []NodeInfo
	for _, node := range m.nodes {
		if node.GroupID == groupID && !node.IsLeader {
			followers = append(followers, node)
		}
	}
	return followers
}

func (m *Membership) GetGroupMembers(groupID int) []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var members []NodeInfo
	for _, node := range m.nodes {
		if node.GroupID == groupID {
			members = append(members, node)
		}
	}
	return members
}

func (m *Membership) Self() NodeInfo {
	return m.self
}

// Cleanup removes nodes that haven't been seen recently
func (m *Membership) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, lastSeen := range m.lastSeen {
		if id == m.self.NodeID {
			continue
		}
		if now.Sub(lastSeen) > m.timeout {
			delete(m.nodes, id)
			delete(m.lastSeen, id)
		}
	}
}
