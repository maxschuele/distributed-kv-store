package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// Multicast group address and port
	multicastAddr := "239.0.0.1:9999"

	// Resolve the multicast address
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		os.Exit(1)
	}

	// Listen on the multicast address
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Listening for multicast messages on %s\n", multicastAddr)

	// Set buffer size
	conn.SetReadBuffer(1024)

	// Receive messages
	buffer := make([]byte, 1024)
	for {
		n, src, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			continue
		}

		fmt.Printf("Received from %s: %s\n", src, string(buffer[:n]))
	}
}
