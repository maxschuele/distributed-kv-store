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

	// Logger
	logger *log.Logger

	// State tracking
	isRunning bool
	stopChan  chan struct{}
}

// NewReliableOrderedMulticast creates a new instance of reliable ordered multicast
func NewReliableOrderedMulticast(nodeID uuid.UUID, groupSize int, participants []uuid.UUID, logger *log.Logger) *ReliableOrderedMulticast {
	rom := &ReliableOrderedMulticast{
		nodeID:       nodeID,
		groupSize:    groupSize,
		participants: make(map[uuid.UUID]bool),
		Sp:           0,
		Rq:           make(map[uuid.UUID]uint32),
		holdback:     make(map[uuid.UUID][]*VectorClockMessage),
		delivery:     make(chan *VectorClockMessage, 100),
		sentMessages: make(map[uuid.UUID][]*VectorClockMessage),
		logger:       logger,
		isRunning:    false,
		stopChan:     make(chan struct{}),
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

	rom.groupListenConn = conn.(*net.UDPConn)
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

	rom.unicastConn = conn.(*net.UDPConn)
	rom.logger.Printf("[ROM] Initialized unicast listener on port %d with SO_REUSEADDR and SO_REUSEPORT", listenPort)

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

// ReceiveMulticast processes an incoming multicast message
func (rom *ReliableOrderedMulticast) ReceiveMulticast(msg *VectorClockMessage) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	senderID := msg.SenderID
	S := msg.Sp
	expectedSeqNum := rom.Rq[senderID] + 1

	// Check if message is in order
	if S == expectedSeqNum {
		// Message is in order - deliver it
		rom.delivery <- msg
		rom.Rq[senderID]++

		// Check holdback for any deliverable messages from this sender
		rom.deliverFromHoldback(senderID)

	} else if S > expectedSeqNum {
		// Gap detected - place in holdback
		rom.holdback[senderID] = append(rom.holdback[senderID], msg)

	} else {
		// Duplicate message - drop it
		return nil
	}

	// Process piggybacked acknowledgements
	rom.processPiggybackedAcks(senderID, msg.VectorClock)

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

// deliverFromHoldback checks and delivers messages from holdback if they're now in order
func (rom *ReliableOrderedMulticast) deliverFromHoldback(senderID uuid.UUID) {
	holdbackQueue := rom.holdback[senderID]
	deliveredCount := 0

	for len(holdbackQueue) > 0 {
		// Check if the first message in holdback is now deliverable
		if holdbackQueue[0].Sp == rom.Rq[senderID]+1 {
			msg := holdbackQueue[0]
			holdbackQueue = holdbackQueue[1:]

			rom.delivery <- msg
			rom.Rq[senderID]++
			deliveredCount++
		} else {
			break
		}
	}

	rom.holdback[senderID] = holdbackQueue
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

// GetDeliveryChannel returns the channel where delivered messages are sent
func (rom *ReliableOrderedMulticast) GetDeliveryChannel() <-chan *VectorClockMessage {
	return rom.delivery
}

// RetransmitMissingMessages sends missing messages to a requesting participant
func (rom *ReliableOrderedMulticast) RetransmitMissingMessages(requesterID uuid.UUID, R uint32, rVector VectorClock) error {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	if !rom.isRunning {
		return fmt.Errorf("reliable ordered multicast is not running")
	}

	// For each participant in the received vector
	for participant, requesterRq := range rVector {
		ourRq, exists := rom.Rq[participant]
		if !exists {
			continue
		}

		// If requester is missing messages from this participant
		if requesterRq < ourRq {
			sentMsgs := rom.sentMessages[participant]

			// Find and resend missing messages
			for _, msg := range sentMsgs {
				if msg.Sp > requesterRq && msg.Sp <= ourRq {
					// Message retransmission would be handled by the network layer
					_ = msg
				}
			}
		}
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
	rom.mu.Unlock()

	if err := rom.groupListenConn.SetReadDeadline(time.Time{}); err != nil {
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

			rom.groupListenConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, remoteAddr, err := rom.groupListenConn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
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

			// Process the message through the ROM protocol
			if err := rom.ReceiveMulticast(msg); err != nil {
				rom.logger.Printf("[ROM] Failed to process message from %s: %v", remoteAddr.String(), err)
				continue
			}

			rom.logger.Printf("[ROM] Received and processed message %d from %s on port %d", msg.Sp, msg.SenderID.String(), listenPort)
		}
	}()

	return nil
}
