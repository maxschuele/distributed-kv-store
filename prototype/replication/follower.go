package replication

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"distributed-kv/cluster"
	"distributed-kv/storage"
)

type Follower struct {
	self       cluster.NodeInfo
	membership *cluster.Membership
	kv         *storage.KVStore
	listener   net.Listener
}

func NewFollower(self cluster.NodeInfo, m *cluster.Membership, kv *storage.KVStore) *Follower {
	return &Follower{
		self:       self,
		membership: m,
		kv:         kv,
	}
}

func (f *Follower) Run() {
	addr := fmt.Sprintf(":%d", f.self.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Follower failed to listen: %v", err)
	}
	f.listener = listener

	log.Printf("Follower listening on %s for replication", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Follower accept error: %v", err)
			continue
		}
		go f.handleConnection(conn)
	}
}

func (f *Follower) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		f.handleMessage(msg)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Follower scan error: %v", err)
	}
}

func (f *Follower) handleMessage(msg string) {
	replMsg, err := DecodeReplicate(msg)
	if err != nil {
		log.Printf("Failed to decode replicate message: %v", err)
		return
	}

	// Apply the replicated write
	f.kv.SetWithVersion(replMsg.Key, replMsg.Value, replMsg.Version)
	log.Printf("Replicated: key=%s, version=%d", replMsg.Key, replMsg.Version)
}
