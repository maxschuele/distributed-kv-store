package discovery

import (
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/maxschuele/distkv/cluster"
)

const (
	BroadcastIP     = "192.168.178.255"
	BroadcastPort   = 5972
	ListenPort      = 5972
	MaxDatagramSize = 8192
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
func Listen(listenPort int, view *GroupView, selfNodeID string) error {
	// Create a socket with SO_REUSEADDR and SO_REUSEPORT set before binding
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}
	defer unix.Close(fd)

	// Enable SO_REUSEADDR and SO_REUSEPORT
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return fmt.Errorf("failed to set SO_REUSEPORT: %w", err)
	}

	// Bind to the address
	addr := unix.SockaddrInet4{Port: listenPort}
	if err := unix.Bind(fd, &addr); err != nil {
		return fmt.Errorf("failed to bind socket: %w", err)
	}

	// Convert to net.UDPConn
	file := os.NewFile(uintptr(fd), "")
	defer file.Close()

	conn, err := net.FilePacketConn(file)
	if err != nil {
		return fmt.Errorf("failed to create connection from file: %w", err)
	}
	defer conn.Close()

	udpConn := conn.(*net.UDPConn)

	log.Printf("[Discovery] Listening for broadcasts on port %d\n", listenPort)

	// Receive announcements in a loop
	buf := make([]byte, MaxDatagramSize)
	for {
		n, remoteAddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("[Discovery] Error reading UDP: %v\n", err)
			continue
		}

		announcement := string(buf[:n])

		if node, err := DecodeAnnounce(announcement); err == nil {
			// Ignore our own announcements
			if node.NodeID == selfNodeID {
				continue
			}

			// Update GroupView with this node
			view.AddOrUpdateNode(node)
			log.Printf("[Discovery] Received announcement from %s at %s\n", node.NodeID, remoteAddr.IP.String())
		} else {
			log.Printf("[Discovery] Failed to decode message from %s: %v\n", remoteAddr.IP.String(), err)
		}
	}
}

// BroadcastHeartbeats periodically broadcasts node announcements (acts as heartbeat)
func BroadcastHeartbeats(nodeInfo cluster.NodeInfo, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		msg := EncodeAnnounce(nodeInfo)
		if err := Broadcast(BroadcastIP, BroadcastPort, msg); err != nil {
			log.Printf("[Discovery] Failed to broadcast announcement: %v\n", err)
		} else {
			log.Printf("[Discovery] Broadcasted announcement\n")
		}
	}
}
