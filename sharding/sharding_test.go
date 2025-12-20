package sharding

import (
	"fmt"
	"testing"
)

// TestHasher_Workflow demonstrates the lifecycle of the Hasher.
// It prints the state of keys before and after topology changes (Adding/Removing groups).
func TestHasher_Workflow(t *testing.T) {
	// 1. Initialize
	h := NewConsistentHash()
	keys := []string{"session_1", "session_2", "session_3", "session_4", "session_5"}

	fmt.Println("=== STEP 1: Initial Setup (Groups 100, 200) ===")
	h.AddGroup(100)
	h.AddGroup(200)

	// Store initial mapping to compare later
	initialMapping := make(map[string]int)

	for _, k := range keys {
		group, err := h.KeyToGroup(k)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		initialMapping[k] = group
		fmt.Printf("Key [%s] assigned to -> Group %d\n", k, group)
	}

	fmt.Println("\n=== STEP 2: Add Group 300 (Rebalancing) ===")
	h.AddGroup(300)

	// Check where keys went
	for _, k := range keys {
		newGroup, _ := h.KeyToGroup(k)
		prevGroup := initialMapping[k]

		// Determine expectation: In consistent hashing, keys should only move
		// to the new node (300) or stay put. They shouldn't swap between 100 and 200.
		status := "STAYED"
		if newGroup != prevGroup {
			status = "MOVED "
		}

		fmt.Printf("Key [%s]: Was %d -> Now %d [%s]\n", k, prevGroup, newGroup, status)

		// Update our map for the next step
		initialMapping[k] = newGroup
	}

	fmt.Println("\n=== STEP 3: Remove Group 100 (Failover) ===")
	h.RemoveGroup(100)

	for _, k := range keys {
		newGroup, _ := h.KeyToGroup(k)
		prevGroup := initialMapping[k]

		status := "STAYED"
		expected := "Expected to stay"

		if prevGroup == 100 {
			expected = "MUST MOVE (100 is gone)"
			if newGroup == 100 {
				t.Errorf("Key %s should have moved from 100, but is still there!", k)
			}
			status = "MOVED "
		} else if newGroup != prevGroup {
			// It is possible for keys on other nodes to move due to hash shifts,
			// but in ideal consistent hashing they shouldn't.
			status = "MOVED "
		}

		fmt.Printf("Key [%s]: Was %d -> Now %d [%s] - %s\n", k, prevGroup, newGroup, status, expected)
	}
}

// TestRing_WrapAround uses a MOCK hash function to strictly control placement.
// We force nodes to be at hashes 10, 20, 30.
// We force a key to be at hash 35. It SHOULD wrap around to 10.
func TestRing_WrapAround(t *testing.T) {
	fmt.Println("\n=== STEP 4: Testing Ring Wrap-Around Logic ===")

	// Custom Mock Hash Function
	// This allows us to predict exactly where things land.
	mockHash := func(data []byte) uint32 {
		s := string(data)
		switch s {
		case "nodeA":
			return 10
		case "nodeB":
			return 20
		case "nodeC":
			return 30
		case "keyHigh":
			return 35 // Higher than 30, should wrap to 10
		case "keyMid":
			return 15 // Between 10 and 20, should go to 20
		default:
			return 0
		}
	}

	// Manually create Ring to inject mock hash
	r := NewRing(mockHash)
	r.Add("nodeA") // Hash: 10
	r.Add("nodeB") // Hash: 20
	r.Add("nodeC") // Hash: 30

	tests := []struct {
		inputKey     string
		expectedNode string
		desc         string
	}{
		{"keyMid", "nodeB", "Standard case (10 < 15 <= 20)"},
		{"keyHigh", "nodeA", "Wrap-around case (35 > 30, wraps to 10)"},
	}

	for _, tt := range tests {
		actualNode := r.Get(tt.inputKey)

		fmt.Printf("Input Key: '%s' (Hash %d)\n", tt.inputKey, mockHash([]byte(tt.inputKey)))
		fmt.Printf("  -> Ring Topology: [10:nodeA, 20:nodeB, 30:nodeC]\n")
		fmt.Printf("  -> Expected Node: %s\n", tt.expectedNode)
		fmt.Printf("  -> Actual Node:   %s\n", actualNode)

		if actualNode != tt.expectedNode {
			t.Errorf("Test %s failed: expected %s, got %s", tt.desc, tt.expectedNode, actualNode)
		} else {
			fmt.Println("  -> RESULT: PASS")
		}
		fmt.Println("------------------------------------------------")
	}
}
