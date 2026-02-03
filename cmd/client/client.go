package main

import (
	"bufio"
	"distributed-kv-store/internal/node"
	"flag"
	"fmt"
	"os"
)

func main() {
	broadcastPortRaw := flag.Uint("broadcast-port", 9998, "Broadcast port")

	broadcastPort := validatePort(*broadcastPortRaw, "broadcast-port")
	client := node.StartNewClient(broadcastPort)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Echo (Ctrl+C to exit):")

	for scanner.Scan() {
		client.ProcessInput(scanner.Text())
	}
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
