package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type HashFn func(data []byte) uint32

type Ring struct {
	hashFn  HashFn
	keys    []uint32          // sorted ring
	hashMap map[uint32]string // hash -> node adress
	lock    sync.RWMutex
}

func NewRing(fn HashFn) *Ring {
	m := &Ring{
		hashFn:  fn,
		hashMap: make(map[uint32]string),
	}
	if m.hashFn == nil {
		m.hashFn = crc32.ChecksumIEEE
	}
	return m
}

func (r *Ring) Add(node string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	hash := r.hashFn([]byte(node))

	// Check for collision or existence
	if val, ok := r.hashMap[hash]; ok {
		if val == node {
			return nil // existence
		}
		// collision
		return fmt.Errorf("hash collision: node '%s' clashes with existing node '%s'", node, val)
	}

	// add to hashring and map
	r.keys = append(r.keys, hash)
	r.hashMap[hash] = node

	// sort hashring s.t. we can use binary search in get
	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
	return nil
}

func (r *Ring) Remove(node string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	hash := r.hashFn([]byte(node))

	// if node does not exist
	if r.hashMap[hash] != node {
		return
	}

	// remove from map
	delete(r.hashMap, hash)

	// delete value from keys
	for i, k := range r.keys {
		if k == hash {
			// delete element at index i and glue the array together
			r.keys = append(r.keys[:i], r.keys[i+1:]...)
			break
		}
	}
}

func (r *Ring) Get(key string) string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if len(r.keys) == 0 {
		return ""
	}

	hash := r.hashFn([]byte(key))

	// search for first node hash that is >= key hash
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hash
	})

	// hash is higher than all existing nodes -> we wrap around the ring to the first node
	if idx == len(r.keys) {
		idx = 0
	}

	return r.hashMap[r.keys[idx]]
}
