package discovery

import (
	"log"
	"net"
	"time"

	"distributed-kv/cluster"
)

const (
	BroadcastAddr    = "255.255.255.255:7946"
	MaxDatagramSize  = 8192
	AnnounceInterval = 5 * time.Second
	CleanupInterval  = 10 * time.Second
)

type Discovery struct {
	self       cluster.NodeInfo
	membership *cluster.Membership
	conn       *net.UDPConn
}

func New(self cluster.NodeInfo, m *cluster.Membership) *Discovery {
	return &Discovery{
		self:       self,
		membership: m,
	}
}

func (d *Discovery) Run() {
	// Setup UDP socket
	addr, err := net.ResolveUDPAddr("udp4", ":7946")
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	d.conn = conn
	defer conn.Close()

	log.Printf("Discovery service listening on :7946")

	// Start background goroutines
	go d.announceLoop()
	go d.cleanupLoop()

	// Listen for incoming messages
	d.listenLoop()
}

func (d *Discovery) announceLoop() {
	ticker := time.NewTicker(AnnounceInterval)
	defer ticker.Stop()

	// Send initial announcement immediately
	d.broadcast()

	for range ticker.C {
		d.broadcast()
	}
}

func (d *Discovery) broadcast() {
	msg := EncodeAnnounce(d.self)

	addr, err := net.ResolveUDPAddr("udp4", BroadcastAddr)
	if err != nil {
		log.Printf("Failed to resolve broadcast address: %v", err)
		return
	}

	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("Failed to dial broadcast address: %v", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	if err != nil {
		log.Printf("Failed to broadcast: %v", err)
	}
}

func (d *Discovery) listenLoop() {
	buf := make([]byte, MaxDatagramSize)

	for {
		n, remoteAddr, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading UDP: %v", err)
			continue
		}

		msg := string(buf[:n])
		d.handleMessage(msg, remoteAddr)
	}
}

func (d *Discovery) handleMessage(msg string, remoteAddr *net.UDPAddr) {
	node, err := DecodeAnnounce(msg)
	if err != nil {
		// Not an announce message, ignore
		return
	}

	// Don't process our own announcements
	if node.NodeID == d.self.NodeID {
		return
	}

	// Update membership
	d.membership.Update(node)
	log.Printf("Discovered node: %s", node.String())
}

func (d *Discovery) cleanupLoop() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		d.membership.Cleanup()
	}
}
