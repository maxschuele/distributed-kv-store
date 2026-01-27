package broadcast

import (
	"distributed-kv-store/internal/logger"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

type BroadcastHandlerFunc func(data []byte, remoteAddr *net.UDPAddr)

const (
	MaxDatagramSize = 8192
)

// Broadcast sends an announcement message to the broadcast address
func Send(port int, pkt []byte) error {
	broadcastAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", port))
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, broadcastAddr)
	if err != nil {
		fmt.Println("Error creating connection:", err)
		return err
	}
	defer conn.Close()

	_, err = conn.Write(pkt)
	if err != nil {
		return err
	}

	return nil
}

// Listen listens for incoming broadcast announcements
func Listen(listenPort int, log *logger.Logger, handler BroadcastHandlerFunc) error {

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

	buf := make([]byte, MaxDatagramSize)
	for {
		n, remoteAddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			log.Error("failed to read UDP broadcast message: %v", err)
			continue
		}

		handler(buf[:n], remoteAddr)
	}
}
