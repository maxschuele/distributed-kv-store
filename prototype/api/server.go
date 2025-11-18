package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"distributed-kv/cluster"
	"distributed-kv/replication"
	"distributed-kv/sharding"
	"distributed-kv/storage"
)

type Server struct {
	port       int
	kv         *storage.KVStore
	hasher     *sharding.Hasher
	membership *cluster.Membership
	self       cluster.NodeInfo
	repl       interface{} // Can be *Leader or *Follower
}

func NewServer(port int, kv *storage.KVStore, hasher *sharding.Hasher, 
	membership *cluster.Membership, self cluster.NodeInfo, repl interface{}) *Server {
	return &Server{
		port:       port,
		kv:         kv,
		hasher:     hasher,
		membership: membership,
		self:       self,
		repl:       repl,
	}
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/set", s.handleSet)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/keys", s.handleKeys)
	mux.HandleFunc("/cluster", s.handleCluster)
	mux.HandleFunc("/health", s.handleHealth)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("API server starting on %s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("API server failed: %v", err)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, exists := s.kv.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"key":   key,
		"value": value,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}

	// Check if this node is the leader of its group
	if !s.self.IsLeader {
		http.Error(w, "Not a leader - writes must go to leader", http.StatusForbidden)
		return
	}

	// Handle write through leader
	if leader, ok := s.repl.(*replication.Leader); ok {
		if err := leader.HandleWrite(req.Key, req.Value); err != nil {
			http.Error(w, fmt.Sprintf("Write failed: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		s.kv.Set(req.Key, req.Value)
	}

	response := map[string]interface{}{
		"status": "ok",
		"key":    req.Key,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	if !s.self.IsLeader {
		http.Error(w, "Not a leader - deletes must go to leader", http.StatusForbidden)
		return
	}

	deleted := s.kv.Delete(key)

	response := map[string]interface{}{
		"status":  "ok",
		"deleted": deleted,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := s.kv.Keys()

	response := map[string]interface{}{
		"keys":  keys,
		"count": len(keys),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes := s.membership.Snapshot()

	nodeList := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		nodeList[i] = map[string]interface{}{
			"node_id":   node.NodeID,
			"address":   node.Address(),
			"group_id":  node.GroupID,
			"is_leader": node.IsLeader,
		}
	}

	response := map[string]interface{}{
		"self_node_id": s.self.NodeID,
		"self_group":   s.self.GroupID,
		"is_leader":    s.self.IsLeader,
		"nodes":        nodeList,
		"node_count":   len(nodes),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status": "healthy",
		"role":   func() string {
			if s.self.IsLeader {
				return "leader"
			}
			return "follower"
		}(),
		"group": s.self.GroupID,
		"keys":  s.kv.Size(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
