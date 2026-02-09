package consistent

import (
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
)

// go test -v -count=1 -run TestVirtualNodeDistribution ./internal/consistent/
func TestVirtualNodeDistribution(t *testing.T) {
	ch := NewConsistentHash(150)

	nodes := make([]uuid.UUID, 4)
	for i := range nodes {
		nodes[i] = uuid.New()
		ch.AddNode(nodes[i])
	}

	// Print all hashes on the sorted ring
	fmt.Printf("\n=== Virtual Node Hashes (4 nodes × 150 vnodes = %d entries) ===\n\n", len(ch.ring))
	for i, hash := range ch.ring {
		nodeID := ch.hashMap[hash]
		fmt.Printf("ring[%3d]  hash=%016x  node=%s\n", i, hash, nodeID)
	}

	// Compute gaps between consecutive hashes (including wrap-around)
	n := len(ch.ring)
	gaps := make([]uint64, n)
	for i := 0; i < n-1; i++ {
		gaps[i] = ch.ring[i+1] - ch.ring[i]
	}
	// Wrap-around gap: from last hash back to first hash across uint64 boundary
	gaps[n-1] = (math.MaxUint64 - ch.ring[n-1]) + ch.ring[0] + 1

	// Stats
	var minGap, maxGap uint64
	var sumGap float64
	minGap = math.MaxUint64
	for _, g := range gaps {
		if g < minGap {
			minGap = g
		}
		if g > maxGap {
			maxGap = g
		}
		sumGap += float64(g)
	}
	meanGap := sumGap / float64(n)

	var variance float64
	for _, g := range gaps {
		diff := float64(g) - meanGap
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / float64(n))

	idealGap := float64(math.MaxUint64) / float64(n)

	// Count vnodes per physical node
	nodeCounts := make(map[uuid.UUID]int)
	for _, hash := range ch.ring {
		nodeCounts[ch.hashMap[hash]]++
	}

	fmt.Printf("\n=== Summary Statistics ===\n\n")
	fmt.Printf("Physical nodes:    %d\n", len(ch.nodes))
	fmt.Printf("Virtual nodes:     %d per node\n", ch.virtualNodes)
	fmt.Printf("Total ring entries: %d\n", n)
	fmt.Printf("\n")
	fmt.Printf("Gap analysis (between consecutive hashes on ring):\n")
	fmt.Printf("  Min gap:    %20d  (%016x)\n", minGap, minGap)
	fmt.Printf("  Max gap:    %20d  (%016x)\n", maxGap, maxGap)
	fmt.Printf("  Mean gap:   %20.0f\n", meanGap)
	fmt.Printf("  Ideal gap:  %20.0f  (MaxUint64 / %d)\n", idealGap, n)
	fmt.Printf("  Std dev:    %20.0f\n", stddev)
	fmt.Printf("  Coefficient of variation: %.2f%%\n", (stddev/meanGap)*100)
	fmt.Printf("\n")
	fmt.Printf("Virtual nodes per physical node:\n")
	for _, node := range nodes {
		fmt.Printf("  %s: %d\n", node, nodeCounts[node])
	}
}

// go test -v -count=1 -run TestKeyDistribution ./internal/consistent/
func TestKeyDistribution(t *testing.T) {
	ch := NewConsistentHash(150)

	nodes := make([]uuid.UUID, 4)
	for i := range nodes {
		nodes[i] = uuid.New()
		ch.AddNode(nodes[i])
	}

	// Test 1: Sequential keys "key0" through "key9999"
	const numKeys = 10000
	counts := make(map[uuid.UUID]int)
	for i := range numKeys {
		key := fmt.Sprintf("key%d", i)
		node, err := ch.GetNode(key)
		if err != nil {
			t.Fatalf("GetNode(%q): %v", key, err)
		}
		counts[node]++
	}

	fmt.Printf("\n=== Key Distribution: %d sequential keys (\"key0\" .. \"key9999\") ===\n\n", numKeys)
	fmt.Printf("Ideal: %d keys per node (%.1f%%)\n\n", numKeys/len(nodes), 100.0/float64(len(nodes)))
	var min, max int
	min = numKeys
	for _, node := range nodes {
		c := counts[node]
		pct := float64(c) / float64(numKeys) * 100
		fmt.Printf("  %s: %5d keys  (%.1f%%)\n", node, c, pct)
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	fmt.Printf("\n  Min: %d  Max: %d  Ratio (max/min): %.2fx\n", min, max, float64(max)/float64(min))

	// Test 2: Short/similar keys
	shortKeys := []string{
		"a", "b", "c", "d", "e", "f", "g", "h",
		"ab", "ac", "ad", "ba", "bb", "bc",
		"test", "test1", "test2", "test3", "test12", "test123", "test1234",
		"foo", "bar", "baz", "qux",
		"x", "xx", "xxx", "xxxx",
		"key", "keys", "value", "values",
	}
	shortCounts := make(map[uuid.UUID]int)
	for _, key := range shortKeys {
		node, err := ch.GetNode(key)
		if err != nil {
			t.Fatalf("GetNode(%q): %v", key, err)
		}
		shortCounts[node]++
	}

	fmt.Printf("\n=== Key Distribution: %d short/similar keys ===\n\n", len(shortKeys))
	fmt.Printf("Ideal: %.1f keys per node (%.1f%%)\n\n", float64(len(shortKeys))/float64(len(nodes)), 100.0/float64(len(nodes)))
	for _, node := range nodes {
		c := shortCounts[node]
		pct := float64(c) / float64(len(shortKeys)) * 100
		fmt.Printf("  %s: %2d keys  (%.1f%%)\n", node, c, pct)
	}

	// Show where each short key lands
	fmt.Printf("\n  Per-key assignments:\n")
	for _, key := range shortKeys {
		node, _ := ch.GetNode(key)
		fmt.Printf("    %-12s -> %s\n", key, node)
	}
}
