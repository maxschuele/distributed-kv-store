package main

import (
	"distributed-kv-store/internal/node"
	"flag"
	"fmt"
	"os"
)

func main() {
	replication := flag.Bool("replication", false, "Run node as replication node")
	ip := flag.String("ip", "", "IP address to use")
	httpPortRaw := flag.Uint("http-port", 0, "HTTP server address")
	clusterPortRaw := flag.Uint("cluster-port", 0, "Cluster communication address")
	groupPortRaw := flag.Uint("group-port", 0, "Group port")
	broadcastPortRaw := flag.Uint("broadcast-port", 9998, "Broadcast port")
	flag.Parse()

	if *ip == "" {
		exit("Error: -ip flag is required\n")
	}

	clusterPort := validatePort(*clusterPortRaw, "cluster-port")
	broadcastPort := validatePort(*broadcastPortRaw, "broadcast-port")

	if *replication {
		node.StartReplicationNode(*ip, clusterPort, broadcastPort)
	} else {
		httpPort := validatePort(*httpPortRaw, "http-port")
		groupPort := validatePort(*groupPortRaw, "group-port")
		node.StartNode(*ip, httpPort, clusterPort, groupPort, broadcastPort)
	}

	select {}
}

func exit(msg string, a ...any) {
	fmt.Fprintf(os.Stderr, msg, a...)
	os.Exit(1)
}

func validatePort(port uint, name string) uint16 {
	if port == 0 {
		exit("Error -%s is required\n", name)
	}

	if port > 65535 {
		exit("Error: invalid -%s value: %d exceeds uint16 max (65535)", name, port)
	}

	return uint16(port)
}
