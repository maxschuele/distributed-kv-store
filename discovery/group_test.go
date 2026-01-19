package discovery

import (
	"sync"
	"testing"
	"time"

	"github.com/maxschuele/distkv/cluster"
)

// TestDiscoveryBroadcastAndListen tests that a broadcasting node is discovered by listening nodes
func TestDiscoveryBroadcastAndListen(t *testing.T) {
	// Create a broadcasting node
	broadcasterNode := cluster.NodeInfo{
		NodeID:   "broadcaster-node",
		IP:       "127.0.0.1",
		Port:     9001,
		GroupID:  1,
		IsLeader: true,
	}

	// Create listener nodes with their GroupViews
	listener1Node := cluster.NodeInfo{
		NodeID:   "listener-node-1",
		IP:       "127.0.0.1",
		Port:     9002,
		GroupID:  1,
		IsLeader: false,
	}

	listener2Node := cluster.NodeInfo{
		NodeID:   "listener-node-2",
		IP:       "127.0.0.1",
		Port:     9003,
		GroupID:  1,
		IsLeader: false,
	}

	//
	viewBroadcaster := NewGroupView()

	go func() {
		Listen(ListenPort, viewBroadcaster, broadcasterNode.NodeID)
	}()

	// Create GroupViews for each listener
	view1 := NewGroupView()
	view2 := NewGroupView()

	// Start listening goroutines for both listeners
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		Listen(ListenPort, view1, listener1Node.NodeID)
	}()

	go func() {
		defer wg.Done()
		Listen(ListenPort, view2, listener2Node.NodeID)
	}()

	// Give listeners time to start
	time.Sleep(500 * time.Millisecond)

	// Broadcaster sends announcement
	msg := EncodeAnnounce(broadcasterNode)
	err := Broadcast(BroadcastIP, BroadcastPort, msg)
	if err != nil {
		t.Fatalf("Failed to broadcast announcement: %v", err)
	}

	// Wait a bit for messages to be received
	time.Sleep(1 * time.Second)

	// Check that both listeners discovered the broadcaster
	nodes1 := view1.GetNodes()
	nodes2 := view2.GetNodes()

	// Verify broadcaster is in view1
	if len(nodes1) == 0 {
		t.Errorf("Listener 1 should have discovered broadcaster, but got 0 nodes")
	} else {
		found := false
		for _, node := range nodes1 {
			if node.NodeID == broadcasterNode.NodeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Listener 1 did not discover broadcaster node")
		}
	}

	// Verify broadcaster is in view2
	if len(nodes2) == 0 {
		t.Errorf("Listener 2 should have discovered broadcaster, but got 0 nodes")
	} else {
		found := false
		for _, node := range nodes2 {
			if node.NodeID == broadcasterNode.NodeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Listener 2 did not discover broadcaster node")
		}
	}

	go BroadcastHeartbeats(listener1Node, 1*time.Second)
	go BroadcastHeartbeats(listener2Node, 1*time.Second)
	time.Sleep(10 * time.Second)
	// look at broadcaster view if he gets group view only from heartbeats
	t.Log("Broadcaster internal nodes: ", viewBroadcaster.GetNodes())
}

// TestGroupViewAddNode tests adding a node to GroupView
func TestGroupViewAddNode(t *testing.T) {
	gv := NewGroupView()

	node := cluster.NodeInfo{
		NodeID:   "test-node-1",
		IP:       "127.0.0.1",
		Port:     5000,
		GroupID:  1,
		IsLeader: false,
	}

	gv.AddOrUpdateNode(node)

	// Verify node was added
	if gv.Size() != 1 {
		t.Errorf("Expected 1 node in GroupView, got %d", gv.Size())
	}

	// Verify we can retrieve the node
	retrieved, exists := gv.GetNode(node.NodeID)
	if !exists {
		t.Errorf("Node not found in GroupView")
	}

	if retrieved.NodeID != node.NodeID || retrieved.Port != node.Port {
		t.Errorf("Retrieved node doesn't match original node")
	}
}

// TestGroupViewUpdateNode tests updating an existing node
func TestGroupViewUpdateNode(t *testing.T) {
	gv := NewGroupView()

	node1 := cluster.NodeInfo{
		NodeID:   "test-node-1",
		IP:       "127.0.0.1",
		Port:     5000,
		GroupID:  1,
		IsLeader: false,
	}

	gv.AddOrUpdateNode(node1)
	firstAddTime := time.Now()

	// Update the same node with different leader status
	time.Sleep(100 * time.Millisecond)
	node1Updated := node1
	node1Updated.IsLeader = true

	gv.AddOrUpdateNode(node1Updated)

	// Verify still only 1 node
	if gv.Size() != 1 {
		t.Errorf("Expected 1 node in GroupView after update, got %d", gv.Size())
	}

	// Verify the update took effect
	retrieved, exists := gv.GetNode(node1.NodeID)
	if !exists {
		t.Errorf("Node not found after update")
	}

	if !retrieved.IsLeader {
		t.Errorf("Node update did not take effect (IsLeader should be true)")
	}

	// Verify LastSeen was updated
	record := gv.getNodeRecord(node1.NodeID)
	if record != nil && record.LastSeen.Before(firstAddTime.Add(50*time.Millisecond)) {
		t.Errorf("LastSeen timestamp was not updated")
	}
}

// TestGroupViewRemoveStaleNodes tests that stale nodes are removed
func TestGroupViewRemoveStaleNodes(t *testing.T) {
	gv := NewGroupView()

	node1 := cluster.NodeInfo{
		NodeID:   "test-node-1",
		IP:       "127.0.0.1",
		Port:     5000,
		GroupID:  1,
		IsLeader: false,
	}

	node2 := cluster.NodeInfo{
		NodeID:   "test-node-2",
		IP:       "127.0.0.1",
		Port:     5001,
		GroupID:  1,
		IsLeader: false,
	}

	gv.AddOrUpdateNode(node1)
	time.Sleep(100 * time.Millisecond)
	gv.AddOrUpdateNode(node2)

	// Verify both nodes are present
	if gv.Size() != 2 {
		t.Errorf("Expected 2 nodes, got %d", gv.Size())
	}

	// Remove stale nodes with a timeout shorter than the age of node1
	gv.RemoveStaleNodes(50 * time.Millisecond)

	// node1 should be removed (older than 50ms), node2 should remain
	if gv.Size() != 1 {
		t.Errorf("Expected 1 node after removing stale nodes, got %d", gv.Size())
	}

	_, exists := gv.GetNode(node1.NodeID)
	if exists {
		t.Errorf("Node1 should have been removed as stale")
	}

	_, exists = gv.GetNode(node2.NodeID)
	if !exists {
		t.Errorf("Node2 should still exist")
	}
}

// TestGroupViewGetAllNodes tests retrieving all nodes
func TestGroupViewGetAllNodes(t *testing.T) {
	gv := NewGroupView()

	nodes := []cluster.NodeInfo{
		{NodeID: "node-1", IP: "127.0.0.1", Port: 5000, GroupID: 1, IsLeader: false},
		{NodeID: "node-2", IP: "127.0.0.1", Port: 5001, GroupID: 1, IsLeader: false},
		{NodeID: "node-3", IP: "127.0.0.1", Port: 5002, GroupID: 1, IsLeader: true},
	}

	for _, node := range nodes {
		gv.AddOrUpdateNode(node)
	}

	retrieved := gv.GetNodes()
	if len(retrieved) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(retrieved))
	}

	// Verify all nodes are in the result
	nodeMap := make(map[string]bool)
	for _, node := range retrieved {
		nodeMap[node.NodeID] = true
	}

	for _, originalNode := range nodes {
		if !nodeMap[originalNode.NodeID] {
			t.Errorf("Node %s not found in retrieved nodes", originalNode.NodeID)
		}
	}
}

// TestMessageEncodeDecodeAnnounce tests encoding and decoding announce messages
func TestMessageEncodeDecodeAnnounce(t *testing.T) {
	node := cluster.NodeInfo{
		NodeID:   "test-node",
		IP:       "192.168.1.100",
		Port:     5000,
		GroupID:  42,
		IsLeader: true,
	}

	encoded := EncodeAnnounce(node)
	decoded, err := DecodeAnnounce(encoded)

	if err != nil {
		t.Fatalf("Failed to decode announce message: %v", err)
	}

	if decoded.NodeID != node.NodeID {
		t.Errorf("NodeID mismatch: expected %s, got %s", node.NodeID, decoded.NodeID)
	}

	if decoded.IP != node.IP {
		t.Errorf("Host mismatch: expected %s, got %s", node.IP, decoded.IP)
	}

	if decoded.Port != node.Port {
		t.Errorf("Port mismatch: expected %d, got %d", node.Port, decoded.Port)
	}

	if decoded.GroupID != node.GroupID {
		t.Errorf("GroupID mismatch: expected %d, got %d", node.GroupID, decoded.GroupID)
	}

	if decoded.IsLeader != node.IsLeader {
		t.Errorf("IsLeader mismatch: expected %v, got %v", node.IsLeader, decoded.IsLeader)
	}
}

// TestMessageEncodeDecodeHeartbeat tests encoding and decoding heartbeat messages
func TestMessageEncodeDecodeHeartbeat(t *testing.T) {
	nodeID := "test-node-123"

	encoded := EncodeHeartbeat(nodeID)
	decoded, err := DecodeHeartbeat(encoded)

	if err != nil {
		t.Fatalf("Failed to decode heartbeat message: %v", err)
	}

	if decoded != nodeID {
		t.Errorf("NodeID mismatch: expected %s, got %s", nodeID, decoded)
	}
}

// TestUpdateHeartbeat tests updating the heartbeat timestamp
func TestUpdateHeartbeat(t *testing.T) {
	gv := NewGroupView()

	node := cluster.NodeInfo{
		NodeID:   "test-node",
		IP:       "127.0.0.1",
		Port:     5000,
		GroupID:  1,
		IsLeader: false,
	}

	gv.AddOrUpdateNode(node)
	firstUpdate := time.Now()

	time.Sleep(100 * time.Millisecond)
	gv.UpdateHeartbeat(node.NodeID)

	record := gv.getNodeRecord(node.NodeID)
	if record == nil {
		t.Fatalf("Node record not found")
	}

	if record.LastSeen.Before(firstUpdate.Add(50 * time.Millisecond)) {
		t.Errorf("Heartbeat was not updated")
	}
}

// Helper method to access internal node record (unexported)
func (gv *GroupView) getNodeRecord(nodeID string) *NodeRecord {
	gv.mu.RLock()
	defer gv.mu.RUnlock()
	return gv.nodes[nodeID]
}
