package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
)

const (
	VirtualNodes = 150 // Number of virtual nodes per physical node
)

type Hasher struct {
	mu           sync.RWMutex
	ring         []uint32
	ringMap      map[uint32]int // hash -> groupID
	groups       map[int]bool   // set of group IDs
}

func NewConsistentHash() *Hasher {
	return &Hasher{
		ringMap: make(map[uint32]int),
		groups:  make(map[int]bool),
	}
}

// AddGroup adds a group to the consistent hash ring
func (h *Hasher) AddGroup(groupID int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.groups[groupID] {
		return // Already added
	}

	h.groups[groupID] = true

	// Add virtual nodes
	for i := 0; i < VirtualNodes; i++ {
		hash := h.hashGroupVNode(groupID, i)
		h.ring = append(h.ring, hash)
		h.ringMap[hash] = groupID
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

// RemoveGroup removes a group from the hash ring
func (h *Hasher) RemoveGroup(groupID int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.groups[groupID] {
		return
	}

	delete(h.groups, groupID)

	// Remove virtual nodes
	newRing := make([]uint32, 0, len(h.ring))
	for _, hash := range h.ring {
		if h.ringMap[hash] != groupID {
			newRing = append(newRing, hash)
		} else {
			delete(h.ringMap, hash)
		}
	}
	h.ring = newRing
}

// KeyToGroup returns the group ID responsible for the given key
func (h *Hasher) KeyToGroup(key string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return 0 // Default to group 0 if no groups
	}

	hash := h.hashKey(key)

	// Binary search for the first node >= hash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	// Wrap around if necessary
	if idx == len(h.ring) {
		idx = 0
	}

	return h.ringMap[h.ring[idx]]
}

// GetGroups returns all registered group IDs
func (h *Hasher) GetGroups() []int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	groups := make([]int, 0, len(h.groups))
	for g := range h.groups {
		groups = append(groups, g)
	}
	return groups
}

func (h *Hasher) hashKey(key string) uint32 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}

func (h *Hasher) hashGroupVNode(groupID int, vnode int) uint32 {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], uint32(groupID))
	binary.BigEndian.PutUint32(data[4:8], uint32(vnode))
	hash := sha256.Sum256(data)
	return binary.BigEndian.Uint32(hash[:4])
}
