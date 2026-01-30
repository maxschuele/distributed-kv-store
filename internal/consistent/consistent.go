package consistent

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type Hash [16]byte

type ConsistentHash struct {
	mu           sync.RWMutex
	ring         []Hash             // sorted hash positions
	hashMap      map[Hash]uuid.UUID // hash -> node mapping
	nodes        map[uuid.UUID]bool // active nodes
	virtualNodes int
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
	return &ConsistentHash{
		ring:         []Hash{},
		hashMap:      make(map[Hash]uuid.UUID),
		nodes:        make(map[uuid.UUID]bool),
		virtualNodes: virtualNodes,
	}
}

func (ch *ConsistentHash) hash(key string) Hash {
	return md5.Sum([]byte(key))
}

func (ch *ConsistentHash) AddNode(nodeID uuid.UUID) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeID] {
		return
	}

	ch.nodes[nodeID] = true

	for i := 0; i < ch.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hash := ch.hash(virtualKey)
		ch.ring = append(ch.ring, hash)
		ch.hashMap[hash] = nodeID
	}

	sort.Slice(ch.ring, func(i, j int) bool {
		return bytes.Compare(ch.ring[i][:], ch.ring[j][:]) < 0
	})
}

func (ch *ConsistentHash) RemoveNode(nodeID uuid.UUID) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeID] {
		return // node doesn't exist
	}

	delete(ch.nodes, nodeID)

	newRing := []Hash{}
	for _, hash := range ch.ring {
		if ch.hashMap[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(ch.hashMap, hash)
		}
	}
	ch.ring = newRing
}

func (ch *ConsistentHash) GetNode(key string) (uuid.UUID, error) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return uuid.Nil, fmt.Errorf("no nodes available")
	}

	hash := ch.hash(key)

	idx := sort.Search(len(ch.ring), func(i int) bool {
		return bytes.Compare(ch.ring[i][:], hash[:]) >= 0
	})

	// Wrap around if necessary
	if idx == len(ch.ring) {
		idx = 0
	}

	return ch.hashMap[ch.ring[idx]], nil
}
