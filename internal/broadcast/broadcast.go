package broadcast

import (
	"context"
	"distributed-kv-store/internal/logger"
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

type BroadcastHandlerFunc func(data []byte, remoteAddr *net.UDPAddr)

const (
	MaxDatagramSize = 8192
	IPv4Broadcast   = "255.255.255.255"
)

// Broadcast sends an announcement message to the broadcast address
func Send(port uint16, pkt []byte) error {
	broadcastAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IPv4Broadcast, port))
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
func Listen(listenPort uint16, log *logger.Logger, handler BroadcastHandlerFunc) error {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				if opErr != nil {
					return
				}
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	pc, err := lc.ListenPacket(context.Background(), "udp4", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		return fmt.Errorf("failed to set up conn on broadcast port: %w", err)
	}

	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return fmt.Errorf("unexpected packet conn type %T", pc)
	}

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
