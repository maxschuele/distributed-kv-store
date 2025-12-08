/*
Plan:
New Nodes only knowledge on startup is broadcast address.
Node sends broadcast announce including its ID and address.

Existing members are in listening loop.
Recieve announce, add to membership list if new.
Respond with own unicast announce to new node.

New node collects responses, builds membership list.

(Should leader take care of membership? Or all nodes maintain full list?)
Inidividual membership management requires additional logic to maintain consistency. (Heartbeats?)

To build:
Broadcast sender function
Broadcast listener function
Each node runs both in separate goroutines.
*/

package discovery

import (
	"fmt"
	"log"
	"net"
	"syscall"
)

const (
	BroadcastIP   = "192.168.178.255"
	BroadcastPort = 5972
	ListenPort    = 5972
)

// Broadcast sends an announcement message to the broadcast address
func Broadcast(ip string, port int, announcement string) error {
	// Resolve the broadcast address
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return fmt.Errorf("failed to resolve broadcast address: %w", err)
	}

	// Create a UDP connection for broadcasting
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to dial UDP: %w", err)
	}
	defer conn.Close()

	//Enable SO_BROADCAST option
	file, err := conn.File()
	if err != nil {
		return fmt.Errorf("failed to get socket file: %w", err)
	}
	defer file.Close()

	//Set SO_BROADCAST on socket
	err = syscall.SetsockoptInt(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	if err != nil {
		return fmt.Errorf("failed to set SO_BROADCAST: %w", err)
	}

	// Send the announcement
	_, err = conn.Write([]byte(announcement))
	if err != nil {
		return fmt.Errorf("failed to send announcement: %w", err)
	}

	return nil
}

// Listen listens for incoming broadcast announcements
func Listen(listenPort int) error {
	// Resolve the listen address
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if err != nil {
		return fmt.Errorf("failed to resolve listen address: %w", err)
	}

	// Create a UDP connection for listening
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP: %w", err)
	}
	defer conn.Close()

	log.Printf("Listening for broadcasts on port %d\n", listenPort)

	// Receive announcements in a loop
	buf := make([]byte, MaxDatagramSize)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading UDP: %v\n", err)
			continue
		}

		announcement := string(buf[:n])
		log.Printf("Received announcement from %s: %s\n", remoteAddr.IP.String(), announcement)
	}
}
