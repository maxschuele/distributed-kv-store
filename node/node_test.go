package node

import (
	"testing"
	"time"
)

// TestDiscoveryBroadcastAndListen tests that a broadcasting node is discovered by listening nodes
func TestDiscoveryBroadcastAndListen(t *testing.T) {
	node1 := NewNode("1", "192.168.178.1", 9001)
	node2 := NewNode("2", "192.168.178.2", 9002)

	go node1.Start()
	go node2.Start()

	time.Sleep(13 * time.Second)
	t.Log("Node1 Group view: ", node1.DiscoveryView.GetNodes())
	t.Log("Node2 Group view: ", node2.DiscoveryView.GetNodes())
}
