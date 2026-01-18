package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	distkv "distributed-kv-store/internal/node"
)

func main() {
	httpAddr := flag.String("http", ":8080", "HTTP server address (e.g., :8080)")
	groupAddr := flag.String("group", ":8081", "Group communication address (e.g., :8081)")
	isLeader := flag.Bool("leader", false, "Whether this node is the leader")
	groupMembers := flag.String("members", "", "Comma-separated list of group member addresses (e.g., localhost:8081,localhost:8082)")
	leaderAddr := flag.String("leader-addr", "", "Address of the leader node (required if not leader)")

	flag.Parse()

	if !*isLeader && *leaderAddr == "" {
		fmt.Println("Error: -leader-addr is required when -leader is false")
		flag.Usage()
		os.Exit(1)
	}

	var group []string
	if *groupMembers != "" {
		group = strings.Split(*groupMembers, ",")
		for i := range group {
			group[i] = strings.TrimSpace(group[i])
		}
	}

	node := distkv.NewNode()
	err := node.Init(*httpAddr, *groupAddr, *isLeader, group, *leaderAddr)
	if err != nil {
		fmt.Printf("Failed to initialize node: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node started successfully\n")
	fmt.Printf("  HTTP Address: %s\n", *httpAddr)
	fmt.Printf("  Group Address: %s\n", *groupAddr)
	fmt.Printf("  Is Leader: %t\n", *isLeader)
	if len(group) > 0 {
		fmt.Printf("  Group Members: %v\n", group)
	}
	if *leaderAddr != "" {
		fmt.Printf("  Leader Address: %s\n", *leaderAddr)
	}

	select {}
}
