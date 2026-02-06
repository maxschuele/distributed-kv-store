package main

import (
	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/logger"
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
	logLevelStr := flag.String("log-level", "INFO", "Log level (DEBUG, INFO, WARN, ERROR)")
	flag.Parse()

	if *ip == "" {
		exit("Error: -ip flag is required\n")
	}

	logLevel, err := logger.ParseLevel(*logLevelStr)
	if err != nil {
		exit("Error: %v\n", err)
	}

	clusterPort := validatePort(*clusterPortRaw, "cluster-port")

	if *replication {
		cluster.StartReplicationNode(*ip, clusterPort, logLevel)
	} else {
		httpPort := validatePort(*httpPortRaw, "http-port")
		groupPort := validatePort(*groupPortRaw, "group-port")
		cluster.StartNode(*ip, clusterPort, httpPort, groupPort, logLevel)
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
