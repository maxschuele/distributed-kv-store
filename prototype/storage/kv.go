package storage

import (
	"sync"
	"time"
)

type KVStore struct {
	mu       sync.RWMutex
	data     map[string]string
	metadata map[string]Metadata
}

type Metadata struct {
	Version   int64
	Timestamp time.Time
}

func NewKVStore() *KVStore {
	return &KVStore{
		data:     make(map[string]string),
		metadata: make(map[string]Metadata),
	}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Increment version
	meta := s.metadata[key]
	meta.Version++
	meta.Timestamp = time.Now()

	s.data[key] = value
	s.metadata[key] = meta
}

func (s *KVStore) SetWithVersion(key, value string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	s.metadata[key] = Metadata{
		Version:   version,
		Timestamp: time.Now(),
	}
}

func (s *KVStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		delete(s.metadata, key)
		return true
	}
	return false
}

func (s *KVStore) GetMetadata(key string) (Metadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.metadata[key]
	return meta, ok
}

func (s *KVStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *KVStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *KVStore) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v
	}
	return snapshot
}
