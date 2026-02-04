package multicast

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// VectorClock represents a vector clock for tracking causal ordering
type VectorClock map[uuid.UUID]uint32

// VectorClockMessage represents a multicast message with piggybacked acknowledgements
type VectorClockMessage struct {
	SenderID    uuid.UUID   // Unique identifier of the sender
	Sp          uint32      // Sequence number from sender
	Payload     []byte      // Application content
	VectorClock VectorClock // Piggybacked acknowledgements <q, Rq>
	Timestamp   time.Time   // Timestamp for message ordering
}

// NegativeAckMessage represents a response message requesting missing messages
type NegativeAckMessage struct {
	SenderID    uuid.UUID   // NAK sender (requester)
	RecipientID uuid.UUID   // Target recipient
	R           uint32      // Received in message by p
	RVector     VectorClock // Stored by p for each participant
	Timestamp   time.Time   // When the NAK was sent
}

// ReliableOrderedMulticast manages reliable ordered multicast with causal ordering
type ReliableOrderedMulticast struct {
	mu sync.RWMutex

	// Node identification
	nodeID       uuid.UUID
	groupSize    int
	participants map[uuid.UUID]bool

	// Sequence tracking
	Sp uint32               // Number of messages this node has sent
	Rq map[uuid.UUID]uint32 // Latest sequence number delivered from each participant

	// Message queues
	holdback map[uuid.UUID][]*VectorClockMessage // Out-of-order messages
	delivery chan *VectorClockMessage            // Ready for application

	// Sent messages (for retransmission on NAK)
	sentMessages map[uuid.UUID][]*VectorClockMessage

	// Network connections
	groupListenConn *net.UDPConn // Connection for listening to group messages
	unicastConn     *net.UDPConn // Connection for unicast communications
	addrMap         map[uuid.UUID]*net.UDPAddr
	addrToUUID      map[string]uuid.UUID

	// Logger
	logger *log.Logger

	// Drop simulation (for testing)
	dropSeq  map[uuid.UUID]uint32
	dropUsed map[uuid.UUID]bool

	// Peer unicast ports (to route NAKs correctly)
	peerUnicastPort map[uuid.UUID]uint16

	// State tracking
	isRunning bool
	stopChan  chan struct{}
}

// NewReliableOrderedMulticast creates a new instance of reliable ordered multicast
func NewReliableOrderedMulticast(nodeID uuid.UUID, groupSize int, participants []uuid.UUID, logger *log.Logger) *ReliableOrderedMulticast {
	rom := &ReliableOrderedMulticast{
		nodeID:          nodeID,
		groupSize:       groupSize,
		participants:    make(map[uuid.UUID]bool),
		Sp:              0,
		Rq:              make(map[uuid.UUID]uint32),
		holdback:        make(map[uuid.UUID][]*VectorClockMessage),
		delivery:        make(chan *VectorClockMessage, 100),
		sentMessages:    make(map[uuid.UUID][]*VectorClockMessage),
		addrMap:         make(map[uuid.UUID]*net.UDPAddr),
		addrToUUID:      make(map[string]uuid.UUID),
		logger:          logger,
		isRunning:       false,
		stopChan:        make(chan struct{}),
		dropSeq:         make(map[uuid.UUID]uint32),
		dropUsed:        make(map[uuid.UUID]bool),
		peerUnicastPort: make(map[uuid.UUID]uint16),
	}

	// Initialize participants and Rq vector
	for _, participant := range participants {
		rom.participants[participant] = true
		rom.Rq[participant] = 0
		rom.holdback[participant] = make([]*VectorClockMessage, 0)
		rom.sentMessages[participant] = make([]*VectorClockMessage, 0)
	}

	return rom
}

// SetDropSequence configures this ROM instance to drop one message from a sender
// before processing (useful to trigger retransmission in demos).
func (rom *ReliableOrderedMulticast) SetDropSequence(senderID uuid.UUID, seq uint32) {
	rom.mu.Lock()
	defer rom.mu.Unlock()
	rom.dropSeq[senderID] = seq
	rom.dropUsed[senderID] = false
}

// SetPeerUnicastPort sets the unicast port for a peer to route NAKs/retransmits.
func (rom *ReliableOrderedMulticast) SetPeerUnicastPort(peerID uuid.UUID, port uint16) {
	rom.mu.Lock()
	defer rom.mu.Unlock()
	rom.peerUnicastPort[peerID] = port
}

// Start initializes the reliable ordered multicast
func (rom *ReliableOrderedMulticast) Start() {
	rom.mu.Lock()
	if rom.isRunning {
		rom.mu.Unlock()
		return
	}
	rom.isRunning = true
	rom.Sp = 0
	// Initialize Rq to 0 for all participants
	for participant := range rom.participants {
		rom.Rq[participant] = 0
	}
	rom.mu.Unlock()
	rom.logger.Printf("[ROM] Reliable ordered multicast started for node %s", rom.nodeID.String())
}

// Stop terminates the reliable ordered multicast
func (rom *ReliableOrderedMulticast) Stop() {
	rom.mu.Lock()
	if !rom.isRunning {
		rom.mu.Unlock()
		return
	}
	rom.isRunning = false

	// Close network connections
	if rom.groupListenConn != nil {
		rom.groupListenConn.Close()
		rom.groupListenConn = nil
	}
	if rom.unicastConn != nil {
		rom.unicastConn.Close()
		rom.unicastConn = nil
	}

	rom.mu.Unlock()
	close(rom.stopChan)
	rom.logger.Printf("[ROM] Reliable ordered multicast stopped for node %s", rom.nodeID.String())
}

// InitializeGroupListener sets up a listening socket for group messages with SO_REUSEADDR and SO_REUSEPORT
func (rom *ReliableOrderedMulticast) InitializeGroupListener(listenPort int) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	// Create a socket with SO_REUSEADDR and SO_REUSEPORT set before binding
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}

	// Enable SO_REUSEADDR
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	// Enable SO_REUSEPORT - allows multiple instances on the same port
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEPORT: %w", err)
	}

	// Bind to the address
	addr := unix.SockaddrInet4{Port: listenPort}
	if err := unix.Bind(fd, &addr); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to bind socket: %w", err)
	}

	// Convert to net.UDPConn
	file := os.NewFile(uintptr(fd), "")
	defer file.Close()

	conn, err := net.FilePacketConn(file)
	if err != nil {
		return fmt.Errorf("failed to create connection from file: %w", err)
	}

	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("unexpected conn type %T", conn)
	}

	rom.groupListenConn = udpConn
	rom.logger.Printf("[ROM] Initialized group listener on port %d with SO_REUSEADDR and SO_REUSEPORT", listenPort)

	return nil
}

// InitializeUnicastListener sets up a listening socket for unicast messages with SO_REUSEADDR and SO_REUSEPORT
func (rom *ReliableOrderedMulticast) InitializeUnicastListener(listenPort int) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	// Create a socket with SO_REUSEADDR and SO_REUSEPORT set before binding
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}

	// Enable SO_REUSEADDR
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	// Enable SO_REUSEPORT - allows multiple instances on the same port
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEPORT: %w", err)
	}

	// Bind to the address
	addr := unix.SockaddrInet4{Port: listenPort}
	if err := unix.Bind(fd, &addr); err != nil {
		unix.Close(fd)
		return fmt.Errorf("failed to bind socket: %w", err)
	}

	// Convert to net.UDPConn
	file := os.NewFile(uintptr(fd), "")
	defer file.Close()

	conn, err := net.FilePacketConn(file)
	if err != nil {
		return fmt.Errorf("failed to create connection from file: %w", err)
	}

	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("unexpected conn type %T", conn)
	}

	rom.unicastConn = udpConn
	rom.logger.Printf("[ROM] Initialized unicast listener on port %d with SO_REUSEADDR and SO_REUSEPORT", listenPort)

	// start unicast read loop to handle incoming messages (NAKs and retransmissions)
	go func(conn *net.UDPConn) {
		if conn == nil {
			return
		}
		buffer := make([]byte, 8192)
		for {
			select {
			case <-rom.stopChan:
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				rom.mu.RLock()
				running := rom.isRunning
				rom.mu.RUnlock()
				if !running {
					return
				}
				rom.logger.Printf("[ROM] unicast read error: %v", err)
				continue
			}
			if n < 1 {
				continue
			}

			switch buffer[0] {
			case 0x01:
				// VectorClockMessage (retransmitted message)
				msg, err := UnmarshalVectorClockMessage(buffer[:n])
				if err != nil {
					rom.logger.Printf("[ROM] failed to unmarshal VectorClockMessage from %s: %v", remoteAddr.String(), err)
					continue
				}
				if err := rom.ReceiveMulticast(msg, remoteAddr); err != nil {
					rom.logger.Printf("[ROM] failed to process retransmitted message from %s: %v", remoteAddr.String(), err)
				}

			case 0x02:
				// NegativeAckMessage
				nak, err := UnmarshalNegativeAckMessage(buffer[:n])
				if err != nil {
					rom.logger.Printf("[ROM] failed to unmarshal NegativeAckMessage from %s: %v", remoteAddr.String(), err)
					continue
				}

				// If this NAK targets us, attempt to retransmit missing messages to requester
				if nak.RecipientID == rom.nodeID {
					requesterID := nak.SenderID
					addr := remoteAddr
					rom.mu.Lock()
					if port, ok := rom.peerUnicastPort[requesterID]; ok {
						addr = &net.UDPAddr{IP: remoteAddr.IP, Port: int(port)}
					}
					rom.addrMap[requesterID] = addr
					rom.mu.Unlock()

					if err := rom.RetransmitMissingMessages(requesterID, nak.R, nak.RVector); err != nil {
						rom.logger.Printf("[ROM] RetransmitMissingMessages failed for requester %s: %v", requesterID.String(), err)
					} else {
						rom.logger.Printf("[ROM] RetransmitMissingMessages invoked for requester %s (addr=%s) R=%d", requesterID.String(), addr.String(), nak.R)
					}
				}
			default:
				rom.logger.Printf("[ROM] unknown unicast message type %02x from %s", buffer[0], remoteAddr.String())
			}
		}
	}(udpConn)

	return nil
}

// SendMulticast sends a multicast message to all group participants
func (rom *ReliableOrderedMulticast) SendMulticast(payload []byte) (*VectorClockMessage, error) {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return nil, fmt.Errorf("reliable ordered multicast is not running")
	}

	// Increment sequence number
	rom.Sp++

	// Create a copy of current vector clock for piggybacked acknowledgements
	vectorClock := make(VectorClock)
	for participant, seqNum := range rom.Rq {
		vectorClock[participant] = seqNum
	}

	// Create the message
	msg := &VectorClockMessage{
		SenderID:    rom.nodeID,
		Sp:          rom.Sp,
		Payload:     payload,
		VectorClock: vectorClock,
		Timestamp:   time.Now(),
	}

	// Store message for potential retransmission
	rom.sentMessages[rom.nodeID] = append(rom.sentMessages[rom.nodeID], msg)

	rom.logger.Printf("[ROM] Node %s sending message %d with vector clock %v",
		rom.nodeID.String(), rom.Sp, vectorClock)

	return msg, nil
}

// helper: can we deliver msg right now?
func (rom *ReliableOrderedMulticast) canDeliver(msg *VectorClockMessage) bool {
	sender := msg.SenderID
	// FIFO check
	if msg.Sp != rom.Rq[sender]+1 {
		return false
	}
	// Causality check: for every participant q != sender, local Rq[q] >= msg.VectorClock[q]
	for q, v := range msg.VectorClock {
		if q == sender {
			continue
		}
		local, ok := rom.Rq[q]
		if !ok {
			// if we don't know q yet, treat as not ready
			return false
		}
		if local < v {
			return false
		}
	}
	return true
}

func (rom *ReliableOrderedMulticast) ReceiveMulticast(msg *VectorClockMessage, remoteAddr *net.UDPAddr) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	senderID := msg.SenderID

	// record address <-> uuid mapping for unicast responses
	if remoteAddr != nil {
		addr := remoteAddr
		if port, ok := rom.peerUnicastPort[msg.SenderID]; ok {
			addr = &net.UDPAddr{IP: remoteAddr.IP, Port: int(port)}
		}
		rom.addrMap[msg.SenderID] = addr
		rom.addrToUUID[remoteAddr.String()] = msg.SenderID
	}

	// If deliverable now (FIFO + causal), deliver and update Rq
	if rom.canDeliver(msg) {
		rom.delivery <- msg
		rom.Rq[senderID]++

		// After delivering, try to drain holdback (scan all queues)
		rom.deliverFromHoldbackUnlocked()
	} else {
		// Not deliverable yet — append to holdback for sender
		rom.holdback[senderID] = append(rom.holdback[senderID], msg)
	}

	// Process piggybacked acks and send any generated NAKs back to the sender.
	naks := rom.processPiggybackedAcks(senderID, msg.VectorClock)
	if len(naks) > 0 {
		// release lock while sending network I/O to avoid holding lock for I/O
		rom.mu.Unlock()
		rom.sendNAKsToSender(senderID, naks, remoteAddr)
		rom.mu.Lock()
	}

	return nil
}

// AddParticipant adds a new participant to the group and extends vector clocks
func (rom *ReliableOrderedMulticast) AddParticipant(participantID uuid.UUID) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	// Check if participant already exists
	if rom.participants[participantID] {
		return fmt.Errorf("participant %s already exists", participantID.String())
	}

	// Add to participants map
	rom.participants[participantID] = true

	// Extend Rq with new participant starting at 0
	rom.Rq[participantID] = 0

	// Initialize empty holdback queue
	rom.holdback[participantID] = make([]*VectorClockMessage, 0)

	// Initialize empty sent messages queue
	rom.sentMessages[participantID] = make([]*VectorClockMessage, 0)

	rom.logger.Printf("[ROM] Added new participant %s to group. Vector clock now: %v",
		participantID.String(), rom.Rq)

	return nil
}

// RemoveParticipant removes a participant from the group (when detected as dead)
func (rom *ReliableOrderedMulticast) RemoveParticipant(participantID uuid.UUID) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	// Check if participant exists
	if !rom.participants[participantID] {
		return fmt.Errorf("participant %s does not exist", participantID.String())
	}

	// Mark as inactive (keep history but don't wait for messages)
	rom.participants[participantID] = false

	rom.logger.Printf("[ROM] Marked participant %s as inactive. Vector clock preserved: %v",
		participantID.String(), rom.Rq)

	return nil
}

// GetParticipants returns list of active participants
func (rom *ReliableOrderedMulticast) GetParticipants() []uuid.UUID {
	rom.mu.RLock()
	defer rom.mu.RUnlock()

	var activeParticipants []uuid.UUID
	for id, active := range rom.participants {
		if active {
			activeParticipants = append(activeParticipants, id)
		}
	}
	return activeParticipants
}

// deliverFromHoldbackUnlocked scans all holdback queues and delivers any now-deliverable messages.
// Assumes rom.mu is already locked by caller.
func (rom *ReliableOrderedMulticast) deliverFromHoldbackUnlocked() {
	progress := true
	for progress {
		progress = false
		for sender, q := range rom.holdback {
			newQ := q[:0]
			for _, m := range q {
				if rom.canDeliver(m) {
					rom.delivery <- m
					rom.Rq[sender]++
					progress = true
				} else {
					newQ = append(newQ, m)
				}
			}
			rom.holdback[sender] = newQ
		}
	}
}

// processPiggybackedAcks handles piggybacked acknowledgements and returns NAKs if needed
func (rom *ReliableOrderedMulticast) processPiggybackedAcks(senderID uuid.UUID, piggyback VectorClock) []*NegativeAckMessage {
	var naks []*NegativeAckMessage

	for participant, R := range piggyback {
		localRq, exists := rom.Rq[participant]
		if !exists {
			continue
		}
		if R > localRq {
			nak := &NegativeAckMessage{
				SenderID:    rom.nodeID,
				RecipientID: senderID,
				R:           R,
				RVector:     rom.copyVectorClock(rom.Rq),
				Timestamp:   time.Now(),
			}
			naks = append(naks, nak)
		}
	}
	return naks
}

// sendNAKsToSender marshals and sends NAKs to the sender's unicast address (or fallback address).
func (rom *ReliableOrderedMulticast) sendNAKsToSender(senderID uuid.UUID, naks []*NegativeAckMessage, fallback *net.UDPAddr) {
	rom.mu.RLock()
	running := rom.isRunning
	addr := rom.addrMap[senderID]
	rom.mu.RUnlock()
	if !running {
		return
	}
	if addr == nil {
		addr = fallback
	}
	if addr == nil {
		rom.logger.Printf("[ROM] no address for sender %s; cannot send NAK", senderID.String())
		return
	}

	for _, nak := range naks {
		data, err := MarshalNegativeAckMessage(nak)
		if err != nil {
			rom.logger.Printf("[ROM] failed to marshal NAK: %v", err)
			continue
		}

		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			rom.logger.Printf("[ROM] failed to dial %s to send NAK: %v", addr.String(), err)
			continue
		}

		_, err = conn.Write(data)
		conn.Close()
		if err != nil {
			rom.logger.Printf("[ROM] failed to send NAK to %s: %v", addr.String(), err)
			continue
		}
		rom.logger.Printf("[ROM] Sent NAK to %s (requested R=%d)", addr.String(), nak.R)
	}
}

// GetDeliveryChannel returns the channel where delivered messages are sent
func (rom *ReliableOrderedMulticast) GetDeliveryChannel() <-chan *VectorClockMessage {
	return rom.delivery
}

// RetransmitMissingMessages sends missing messages to a requesting participant.
// It collects messages to send while holding the lock, then performs network I/O without holding the lock.
func (rom *ReliableOrderedMulticast) RetransmitMissingMessages(requesterID uuid.UUID, R uint32, rVector VectorClock) error {
	// collect messages to send
	var toSend []*VectorClockMessage
	var targetAddr *net.UDPAddr

	rom.mu.Lock()
	if !rom.isRunning {
		rom.mu.Unlock()
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	// resolve requester address
	addr, ok := rom.addrMap[requesterID]
	if !ok || addr == nil {
		rom.mu.Unlock()
		return fmt.Errorf("unknown address for requester %s", requesterID.String())
	}
	targetAddr = addr

	// We can only retransmit messages we actually sent (rom.nodeID).
	// Use our send sequence (Sp) as the upper bound, not Rq.
	requesterRq := uint32(0)
	if rv, ok := rVector[rom.nodeID]; ok {
		requesterRq = rv
	}
	ourMax := rom.Sp
	sentMsgs := rom.sentMessages[rom.nodeID]

	if requesterRq < ourMax {
		for _, msg := range sentMsgs {
			if msg.Sp > requesterRq && msg.Sp <= ourMax {
				toSend = append(toSend, msg)
			}
		}
	}
	rom.mu.Unlock()

	// nothing to send
	if len(toSend) == 0 {
		return nil
	}

	// marshal and send each message via unicast to the requester
	for _, msg := range toSend {
		data, err := MarshalVectorClockMessage(msg)
		if err != nil {
			rom.logger.Printf("[ROM] failed to marshal retransmit message (sender=%s sp=%d): %v", msg.SenderID.String(), msg.Sp, err)
			continue
		}

		// prefer using existing unicastConn to write back
		if rom.unicastConn != nil {
			if _, err := rom.unicastConn.WriteToUDP(data, targetAddr); err != nil {
				rom.logger.Printf("[ROM] failed to send retransmit to %s via unicastConn: %v", targetAddr.String(), err)
				// fallback to DialUDP below
			} else {
				rom.logger.Printf("[ROM] retransmitted message %d from %s to %s", msg.Sp, msg.SenderID.String(), targetAddr.String())
				continue
			}
		}

		// fallback: dial + write
		conn, err := net.DialUDP("udp", nil, targetAddr)
		if err != nil {
			rom.logger.Printf("[ROM] failed to dial %s to retransmit: %v", targetAddr.String(), err)
			continue
		}
		if _, err := conn.Write(data); err != nil {
			rom.logger.Printf("[ROM] failed to write retransmit to %s: %v", targetAddr.String(), err)
		} else {
			rom.logger.Printf("[ROM] retransmitted message %d from %s to %s", msg.Sp, msg.SenderID.String(), targetAddr.String())
		}
		conn.Close()
	}

	return nil
}

// GetVectorClock returns a copy of the current vector clock (Rq)
func (rom *ReliableOrderedMulticast) GetVectorClock() VectorClock {
	rom.mu.RLock()
	defer rom.mu.RUnlock()
	return rom.copyVectorClock(rom.Rq)
}

// GetSendSequenceNumber returns the current send sequence number
func (rom *ReliableOrderedMulticast) GetSendSequenceNumber() uint32 {
	rom.mu.RLock()
	defer rom.mu.RUnlock()
	return rom.Sp
}

// copyVectorClock creates a copy of a vector clock
func (rom *ReliableOrderedMulticast) copyVectorClock(vc VectorClock) VectorClock {
	copy := make(VectorClock)
	for k, v := range vc {
		copy[k] = v
	}
	return copy
}

// ==================== Message Serialization ====================

// MarshalVectorClockMessage serializes a VectorClockMessage into bytes
func MarshalVectorClockMessage(msg *VectorClockMessage) ([]byte, error) {
	var buf bytes.Buffer

	// Write message type (1 byte) - 0x01 for VectorClockMessage
	if err := binary.Write(&buf, binary.BigEndian, uint8(0x01)); err != nil {
		return nil, err
	}

	// Write sender ID (16 bytes for UUID)
	senderBytes, err := msg.SenderID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(senderBytes)

	// Write sequence number
	if err := binary.Write(&buf, binary.BigEndian, msg.Sp); err != nil {
		return nil, err
	}

	// Write payload length and content
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(msg.Payload))); err != nil {
		return nil, err
	}
	buf.Write(msg.Payload)

	// Write vector clock
	if err := marshalVectorClock(&buf, msg.VectorClock); err != nil {
		return nil, err
	}

	// Write timestamp
	if err := binary.Write(&buf, binary.BigEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalVectorClockMessage deserializes a VectorClockMessage from bytes
func UnmarshalVectorClockMessage(data []byte) (*VectorClockMessage, error) {
	buf := bytes.NewReader(data)

	// Read and verify message type
	var msgType uint8
	if err := binary.Read(buf, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}
	if msgType != 0x01 {
		return nil, fmt.Errorf("invalid message type: %d", msgType)
	}

	// Read sender ID (16 bytes for UUID)
	senderBytes := make([]byte, 16)
	if _, err := buf.Read(senderBytes); err != nil {
		return nil, err
	}
	senderID, err := uuid.FromBytes(senderBytes)
	if err != nil {
		return nil, err
	}

	// Read sequence number
	var Sp uint32
	if err := binary.Read(buf, binary.BigEndian, &Sp); err != nil {
		return nil, err
	}

	// Read payload
	var payloadLen uint32
	if err := binary.Read(buf, binary.BigEndian, &payloadLen); err != nil {
		return nil, err
	}
	payload := make([]byte, payloadLen)
	if _, err := buf.Read(payload); err != nil {
		return nil, err
	}

	// Read vector clock
	vectorClock, err := unmarshalVectorClock(buf)
	if err != nil {
		return nil, err
	}

	// Read timestamp
	var unixNano int64
	if err := binary.Read(buf, binary.BigEndian, &unixNano); err != nil {
		return nil, err
	}
	timestamp := time.Unix(0, unixNano)

	return &VectorClockMessage{
		SenderID:    senderID,
		Sp:          Sp,
		Payload:     payload,
		VectorClock: vectorClock,
		Timestamp:   timestamp,
	}, nil
}

// MarshalNegativeAckMessage serializes a NegativeAckMessage into bytes
func MarshalNegativeAckMessage(msg *NegativeAckMessage) ([]byte, error) {
	var buf bytes.Buffer

	// Write message type (1 byte) - 0x02 for NegativeAckMessage
	if err := binary.Write(&buf, binary.BigEndian, uint8(0x02)); err != nil {
		return nil, err
	}

	// Write sender ID (16 bytes for UUID)
	senderBytes, err := msg.SenderID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(senderBytes)

	// Write recipient ID (16 bytes for UUID)
	recipientBytes, err := msg.RecipientID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(recipientBytes)

	// Write R
	if err := binary.Write(&buf, binary.BigEndian, msg.R); err != nil {
		return nil, err
	}

	// Write R vector clock
	if err := marshalVectorClock(&buf, msg.RVector); err != nil {
		return nil, err
	}

	// Write timestamp
	if err := binary.Write(&buf, binary.BigEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalNegativeAckMessage deserializes a NegativeAckMessage from bytes
func UnmarshalNegativeAckMessage(data []byte) (*NegativeAckMessage, error) {
	buf := bytes.NewReader(data)

	// Read and verify message type
	var msgType uint8
	if err := binary.Read(buf, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}
	if msgType != 0x02 {
		return nil, fmt.Errorf("invalid message type: %d", msgType)
	}

	// Read sender ID (16 bytes for UUID)
	senderBytes := make([]byte, 16)
	if _, err := buf.Read(senderBytes); err != nil {
		return nil, err
	}
	senderID, err := uuid.FromBytes(senderBytes)
	if err != nil {
		return nil, err
	}

	// Read recipient ID (16 bytes for UUID)
	recipientBytes := make([]byte, 16)
	if _, err := buf.Read(recipientBytes); err != nil {
		return nil, err
	}
	recipientID, err := uuid.FromBytes(recipientBytes)
	if err != nil {
		return nil, err
	}

	// Read R
	var R uint32
	if err := binary.Read(buf, binary.BigEndian, &R); err != nil {
		return nil, err
	}

	// Read R vector clock
	RVector, err := unmarshalVectorClock(buf)
	if err != nil {
		return nil, err
	}

	// Read timestamp
	var unixNano int64
	if err := binary.Read(buf, binary.BigEndian, &unixNano); err != nil {
		return nil, err
	}
	timestamp := time.Unix(0, unixNano)

	return &NegativeAckMessage{
		SenderID:    senderID,
		RecipientID: recipientID,
		R:           R,
		RVector:     RVector,
		Timestamp:   timestamp,
	}, nil
}

// marshalVectorClock serializes a vector clock with UUID keys
func marshalVectorClock(buf *bytes.Buffer, vc VectorClock) error {
	// Write number of entries
	if err := binary.Write(buf, binary.BigEndian, uint16(len(vc))); err != nil {
		return err
	}

	// Write each entry
	for participantID, seqNum := range vc {
		participantBytes, err := participantID.MarshalBinary()
		if err != nil {
			return err
		}
		buf.Write(participantBytes)

		if err := binary.Write(buf, binary.BigEndian, seqNum); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalVectorClock deserializes a vector clock with UUID keys
func unmarshalVectorClock(buf *bytes.Reader) (VectorClock, error) {
	vc := make(VectorClock)

	// Read number of entries
	var numEntries uint16
	if err := binary.Read(buf, binary.BigEndian, &numEntries); err != nil {
		return nil, err
	}

	// Read each entry
	for i := 0; i < int(numEntries); i++ {
		participantBytes := make([]byte, 16)
		if _, err := buf.Read(participantBytes); err != nil {
			return nil, err
		}
		participantID, err := uuid.FromBytes(participantBytes)
		if err != nil {
			return nil, err
		}

		var seqNum uint32
		if err := binary.Read(buf, binary.BigEndian, &seqNum); err != nil {
			return nil, err
		}

		vc[participantID] = seqNum
	}

	return vc, nil
}

// ==================== Multicast Group Operations ====================

// SendToGroup broadcasts a serialized VectorClockMessage to all group members via UDP multicast
func (rom *ReliableOrderedMulticast) SendToGroup(port int, msg *VectorClockMessage) error {
	rom.mu.RLock()
	if !rom.isRunning {
		rom.mu.RUnlock()
		return fmt.Errorf("reliable ordered multicast is not running")
	}
	rom.mu.RUnlock()

	// Serialize the message
	data, err := MarshalVectorClockMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Send to multicast group (224.0.0.1 is standard multicast address)
	addr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("224.0.0.1"),
	}

	conn, err := net.DialUDP("udp", nil, &addr)
	if err != nil {
		return fmt.Errorf("failed to dial multicast address: %w", err)
	}
	defer conn.Close()

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send multicast message: %w", err)
	}

	rom.logger.Printf("[ROM] Sent message %d to multicast group on port %d", msg.Sp, port)
	return nil
}

// ListenToGroup starts listening for serialized VectorClockMessages on a UDP multicast group
// and automatically deserializes and processes them via ReceiveMulticast
func (rom *ReliableOrderedMulticast) ListenToGroup(listenPort int) error {
	rom.mu.Lock()
	if rom.groupListenConn == nil {
		rom.mu.Unlock()
		return fmt.Errorf("group listener not initialized; call InitializeGroupListener first")
	}
	conn := rom.groupListenConn
	rom.mu.Unlock()

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Listen in a goroutine
	go func() {
		buffer := make([]byte, 4096)
		for {
			select {
			case <-rom.stopChan:
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				rom.mu.RLock()
				running := rom.isRunning
				rom.mu.RUnlock()
				if !running {
					return
				}
				rom.logger.Printf("[ROM] Error reading from group: %v", err)
				continue
			}

			// Deserialize the message
			msg, err := UnmarshalVectorClockMessage(buffer[:n])
			if err != nil {
				rom.logger.Printf("[ROM] Failed to unmarshal message from %s: %v", remoteAddr.String(), err)
				continue
			}

			// Optional: drop one message to simulate loss
			rom.mu.Lock()
			dropSeq, ok := rom.dropSeq[msg.SenderID]
			used := rom.dropUsed[msg.SenderID]
			if ok && !used && msg.Sp == dropSeq {
				rom.dropUsed[msg.SenderID] = true
				rom.mu.Unlock()
				rom.logger.Printf("[ROM] Dropped message %d from %s (simulated loss)", msg.Sp, msg.SenderID.String())
				continue
			}
			rom.mu.Unlock()

			// Process the message through the ROM protocol
			if err := rom.ReceiveMulticast(msg, remoteAddr); err != nil {
				rom.logger.Printf("[ROM] Failed to process message from %s: %v", remoteAddr.String(), err)
				continue
			}

			rom.logger.Printf("[ROM] Received and processed message %d from %s on port %d", msg.Sp, msg.SenderID.String(), listenPort)
		}
	}()

	return nil
}
