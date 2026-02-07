package consistent

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/google/uuid"
)

var (
	ErrNoNodes    = errors.New("no nodes available")
	ErrNodeExists = errors.New("node already exists")
)

type ConsistentHash struct {
	mu           sync.RWMutex
	ring         []uint64
	hashMap      map[uint64]uuid.UUID
	nodes        map[uuid.UUID]bool
	virtualNodes int
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150 // sensible default
	}
	return &ConsistentHash{
		ring:         make([]uint64, 0),
		hashMap:      make(map[uint64]uuid.UUID),
		nodes:        make(map[uuid.UUID]bool),
		virtualNodes: virtualNodes,
	}
}

func (ch *ConsistentHash) hash(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (ch *ConsistentHash) virtualNodeKey(nodeID uuid.UUID, replica int) uint64 {
	b := make([]byte, 20)
	copy(b, nodeID[:])
	binary.LittleEndian.PutUint32(b[16:], uint32(replica))
	return ch.hash(b)
}

func (ch *ConsistentHash) AddNode(nodeID uuid.UUID) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeID] {
		return
	}

	ch.nodes[nodeID] = true

	for i := 0; i < ch.virtualNodes; i++ {
		hash := ch.virtualNodeKey(nodeID, i)
		ch.ring = append(ch.ring, hash)
		ch.hashMap[hash] = nodeID
	}

	// sort.Slice(ch.ring, func(i, j int) bool {
	// 	return ch.ring[i] < ch.ring[j]
	// })

	slices.Sort(ch.ring)
}

func (ch *ConsistentHash) RemoveNode(nodeID uuid.UUID) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeID] {
		return
	}

	delete(ch.nodes, nodeID)
	newRing := make([]uint64, 0, len(ch.ring)-ch.virtualNodes)
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
		return uuid.Nil, ErrNoNodes
	}

	hash := xxhash.Sum64String(key)
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	if idx == len(ch.ring) {
		idx = 0
	}

	return ch.hashMap[ch.ring[idx]], nil
}

func (ch *ConsistentHash) NodeCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.nodes)
}
