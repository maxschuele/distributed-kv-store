package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	// Multicast group address and port
	multicastAddr := "239.0.0.1:9999"

	// Resolve the multicast address
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	// Create UDP connection
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("Error creating connection:", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Sending multicast messages to %s\n", multicastAddr)

	// Send messages periodically
	counter := 0
	for {
		message := fmt.Sprintf("Multicast message #%d at %s", counter, time.Now().Format("15:04:05"))

		_, err := conn.Write([]byte(message))
		if err != nil {
			fmt.Println("Error sending message:", err)
			return
		}

		fmt.Printf("Sent: %s\n", message)
		counter++
		time.Sleep(2 * time.Second)
	}
}
