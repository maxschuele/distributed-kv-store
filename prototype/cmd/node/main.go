package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-kv/api"
	"distributed-kv/cluster"
	"distributed-kv/discovery"
	"distributed-kv/replication"
	"distributed-kv/sharding"
	"distributed-kv/storage"
)

func main() {
	var port int
	var groupID int
	var isLeader bool
	var apiPort int

	flag.IntVar(&port, "port", 8000, "port to listen on for cluster communication")
	flag.IntVar(&groupID, "group", 0, "replication group id")
	flag.BoolVar(&isLeader, "leader", false, "is this node the leader of its group")
	flag.IntVar(&apiPort, "api", 9000, "HTTP API port")
	flag.Parse()

	log.Printf("Starting node on port %d in group %d (leader=%v)", port, groupID, isLeader)
	log.Printf("API server will be available on port %d", apiPort)

	// Storage engine
	kv := storage.NewKVStore()

	// Node metadata
	info := cluster.NewLocalNode(port, groupID, isLeader)
	log.Printf("Node ID: %s", info.NodeID)

	// Membership state
	membership := cluster.NewMembership(info)

	// Sharding
	hasher := sharding.NewConsistentHash()

	// Discovery service
	disc := discovery.New(info, membership)
	go disc.Run()

	// Wait a bit for discovery to populate membership
	time.Sleep(2 * time.Second)

	// Replication - start based on role
	var repl interface{}
	if isLeader {
		leader := replication.NewLeader(info, membership, kv)
		go leader.Run()
		repl = leader
		log.Printf("Started as LEADER")
	} else {
		follower := replication.NewFollower(info, membership, kv)
		go follower.Run()
		repl = follower
		log.Printf("Started as FOLLOWER")
	}

	// HTTP API
	apiServer := api.NewServer(apiPort, kv, hasher, membership, info, repl)
	go apiServer.Start()

	log.Printf("Node fully initialized and ready")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
}
