package replication

import (
	"fmt"
	"log"
	"net"
	"time"

	"distributed-kv/cluster"
	"distributed-kv/storage"
)

type Leader struct {
	self       cluster.NodeInfo
	membership *cluster.Membership
	kv         *storage.KVStore
	listener   net.Listener
}

func NewLeader(self cluster.NodeInfo, m *cluster.Membership, kv *storage.KVStore) *Leader {
	return &Leader{
		self:       self,
		membership: m,
		kv:         kv,
	}
}

func (l *Leader) Run() {
	addr := fmt.Sprintf(":%d", l.self.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Leader failed to listen: %v", err)
	}
	l.listener = listener

	log.Printf("Leader listening on %s for replication", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Leader accept error: %v", err)
			continue
		}
		go l.handleConnection(conn)
	}
}

func (l *Leader) handleConnection(conn net.Conn) {
	defer conn.Close()
	// Leaders don't typically receive replication messages
	// This is here for future extensibility
}

// ReplicateToFollowers sends a key-value update to all followers in the group
func (l *Leader) ReplicateToFollowers(key, value string) error {
	followers := l.membership.GetFollowers(l.self.GroupID)

	if len(followers) == 0 {
		// No followers, replication successful
		return nil
	}

	meta, _ := l.kv.GetMetadata(key)
	msg := EncodeReplicate(key, value, meta.Version)

	successCount := 0
	for _, follower := range followers {
		if err := l.sendToFollower(follower, msg); err != nil {
			log.Printf("Failed to replicate to follower %s: %v", follower.NodeID, err)
		} else {
			successCount++
		}
	}

	// Consider replication successful if at least one follower acknowledged
	// or if there are no followers
	if successCount > 0 || len(followers) == 0 {
		return nil
	}

	return fmt.Errorf("replication failed: no followers acknowledged")
}

func (l *Leader) sendToFollower(follower cluster.NodeInfo, msg string) error {
	addr := follower.Address()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write([]byte(msg + ""))
	return err
}

// HandleWrite processes a write request and replicates it
func (l *Leader) HandleWrite(key, value string) error {
	// Write to local storage
	l.kv.Set(key, value)

	// Replicate to followers (best effort)
	go l.ReplicateToFollowers(key, value)

	return nil
}
