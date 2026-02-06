package main

import (
	"bufio"
	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/logger"
	"flag"
	"fmt"
	"os"
)

func main() {
	broadcastPortRaw := flag.Uint("broadcast-port", 9998, "Broadcast port")
	logFilePath := flag.String("log-file", "logs/client.log", "Path to the log file. If not provided, logs to stdout.")
	logLevelStr := flag.String("log-level", "INFO", "Log level (DEBUG, INFO, WARN, ERROR)")
	flag.Parse()

	if *logFilePath == "" {
		exit("no log file path provided")
	}

	logLevel, err := logger.ParseLevel(*logLevelStr)
	if err != nil {
		exit("Error: %v\n", err)
	}

	broadcastPort := validatePort(*broadcastPortRaw, "broadcast-port")

	client, err := cluster.StartNewClient(broadcastPort, *logFilePath, logLevel)
	if err != nil {
		exit("Failed to start client: %v", err)
	}

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
