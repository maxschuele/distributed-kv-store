package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	distkv "distributed-kv-store/internal/node"
)

func main() {
	ip := flag.String("ip", "", "IP address to use")
	httpPort := flag.String("http-port", "", "HTTP server address")
	clusterPort := flag.String("cluster-port", "", "Cluster communication address")
	isLeader := flag.Bool("leader", false, "Whether this node is the leader")
	groupMembers := flag.String("members", "", "Comma-separated list of group member addresses (e.g., localhost:8081,localhost:8082)")
	leaderAddr := flag.String("leader-addr", "", "Address of the leader node (required if not leader)")
	broadcastPortRaw := flag.String("broadcast-port", "", "Broadcast port to listen/send on")

	flag.Parse()

	broadcastPort, err := strconv.Atoi(*broadcastPortRaw)

	if err != nil {
		fmt.Println("Error: -broadcast-port must be a valid integer number: ", err)
		flag.Usage()
		os.Exit(1)
	}

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

	_, err = distkv.NewNode(*ip, *httpPort, *clusterPort, *isLeader, group, *leaderAddr, broadcastPort)
	if err != nil {
		fmt.Printf("Failed to initialize node: %v\n", err)
		os.Exit(1)
	}

	select {}
}
