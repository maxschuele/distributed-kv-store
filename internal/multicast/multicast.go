package multicast

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const multicastIP = "224.0.0.1"

// FIFOMessage represents a FIFO multicast data message.
type FIFOMessage struct {
	SenderID  uuid.UUID
	Seq       uint32
	Payload   []byte
	Timestamp time.Time
}

// NAKMessage requests a missing FIFO sequence number (R).
type NAKMessage struct {
	RequesterID uuid.UUID
	MissingSeq  uint32
	Timestamp   time.Time
}

// LastAckMessage indicates the receiver is done and has received up to LastSeq.
type LastAckMessage struct {
	RequesterID uuid.UUID
	LastSeq     uint32
	Timestamp   time.Time
}

// ReliableFIFOMulticast provides reliable FIFO-ordered multicast (single-sender).
type ReliableFIFOMulticast struct {
	mu sync.RWMutex

	// identity
	nodeID       uuid.UUID
	leaderID     uuid.UUID
	participants map[uuid.UUID]bool

	// sequence tracking (single sender)
	Sp uint32 // send sequence (leader only)
	Rq uint32 // last delivered (receiver only)

	// queues
	holdback []*FIFOMessage
	delivery chan *FIFOMessage

	// retransmit storage (leader)
	sentMessages map[uint32]*FIFOMessage

	// addressing (unicast for NAK/LASTACK/retransmit)
	unicastConn *net.UDPConn
	addrMap     map[uuid.UUID]*net.UDPAddr

	// multicast data-plane
	multicastAddr     *net.UDPAddr
	multicastRecvConn *net.UDPConn
	multicastSendConn *net.UDPConn

	// state
	isRunning bool
	stopChan  chan struct{}

	// logging
	logger *log.Logger

	// last-ack tracking (leader)
	lastAck map[uuid.UUID]uint32
}

// NewReliableFIFOMulticast creates a FIFO-reliable multicast instance.
// leaderID is the only sender in the group.
func NewReliableFIFOMulticast(nodeID, leaderID uuid.UUID, participants []uuid.UUID, logger *log.Logger) *ReliableFIFOMulticast {
	rom := &ReliableFIFOMulticast{
		nodeID:       nodeID,
		leaderID:     leaderID,
		participants: make(map[uuid.UUID]bool),
		holdback:     make([]*FIFOMessage, 0),
		delivery:     make(chan *FIFOMessage, 128),
		sentMessages: make(map[uint32]*FIFOMessage),
		addrMap:      make(map[uuid.UUID]*net.UDPAddr),
		stopChan:     make(chan struct{}),
		logger:       logger,
		lastAck:      make(map[uuid.UUID]uint32),
	}
	for _, id := range participants {
		rom.participants[id] = true
	}
	return rom
}

// NewReliableFIFOMulticastWithLeaderAddr is like NewReliableFIFOMulticast but
// also seeds the leader's unicast address (useful for receivers).
func NewReliableFIFOMulticastWithLeaderAddr(
	nodeID, leaderID uuid.UUID,
	participants []uuid.UUID,
	leaderAddr *net.UDPAddr,
	logger *log.Logger,
) *ReliableFIFOMulticast {
	rom := NewReliableFIFOMulticast(nodeID, leaderID, participants, logger)
	if leaderAddr != nil {
		rom.addrMap[leaderID] = leaderAddr
	}
	return rom
}

// Start marks the instance running.
func (rom *ReliableFIFOMulticast) Start() {
	rom.mu.Lock()
	if rom.isRunning {
		rom.mu.Unlock()
		return
	}
	rom.isRunning = true
	rom.Sp = 0
	rom.Rq = 0
	rom.mu.Unlock()

	if rom.logger != nil {
		rom.logger.Printf("[FIFO] started for node %s (leader=%s)", rom.nodeID.String(), rom.leaderID.String())
	}
}

// Stop terminates connections.
func (rom *ReliableFIFOMulticast) Stop() {
	rom.mu.Lock()
	if !rom.isRunning {
		rom.mu.Unlock()
		return
	}
	rom.isRunning = false

	if rom.unicastConn != nil {
		_ = rom.unicastConn.Close()
		rom.unicastConn = nil
	}
	if rom.multicastRecvConn != nil {
		_ = rom.multicastRecvConn.Close()
		rom.multicastRecvConn = nil
	}
	if rom.multicastSendConn != nil {
		_ = rom.multicastSendConn.Close()
		rom.multicastSendConn = nil
	}

	rom.mu.Unlock()
	close(rom.stopChan)

	if rom.logger != nil {
		rom.logger.Printf("[FIFO] stopped for node %s", rom.nodeID.String())
	}
}

// InitializeMulticastListener binds to the multicast port, joins the group,
// and starts receiving FIFO messages (type tag 0x01).
func (rom *ReliableFIFOMulticast) InitializeMulticastListener(groupIP string, port int) error {
	ip := net.ParseIP(groupIP)
	if ip == nil || ip.To4() == nil {
		return fmt.Errorf("invalid IPv4 multicast address: %q", groupIP)
	}
	if !isLocalMulticast(ip) {
		return fmt.Errorf("multicast address must be in 224.0.0.0/24, got %s", ip.String())
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid port: %d", port)
	}

	maddr := &net.UDPAddr{IP: ip, Port: port}

	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var innerErr error
			if err := c.Control(func(fd uintptr) {
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return innerErr
		},
	}

	pc, err := lc.ListenPacket(context.Background(), "udp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	conn, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return fmt.Errorf("unexpected packet conn type %T", pc)
	}

	rc, err := conn.SyscallConn()
	if err != nil {
		_ = conn.Close()
		return err
	}
	var joinErr error
	if err := rc.Control(func(fd uintptr) {
		multi := ip.To4()
		mreq := &unix.IPMreqn{
			Multiaddr: [4]byte{multi[0], multi[1], multi[2], multi[3]},
		}
		joinErr = unix.SetsockoptIPMreqn(int(fd), unix.IPPROTO_IP, unix.IP_ADD_MEMBERSHIP, mreq)
	}); err != nil {
		_ = conn.Close()
		return err
	}
	if joinErr != nil {
		_ = conn.Close()
		return joinErr
	}

	// separate multicast sender
	sendConn, err := net.DialUDP("udp4", nil, maddr)
	if err != nil {
		_ = conn.Close()
		return err
	}

	rom.mu.Lock()
	rom.multicastAddr = maddr
	rom.multicastRecvConn = conn
	rom.multicastSendConn = sendConn
	rom.mu.Unlock()

	// receive loop for multicast FIFO data messages
	go func(c *net.UDPConn) {
		buf := make([]byte, 64*1024)
		for {
			select {
			case <-rom.stopChan:
				return
			default:
			}

			_ = c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			n, _, err := c.ReadFromUDP(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
			if n == 0 {
				continue
			}

			if buf[0] != 0x01 {
				continue
			}
			msg, err := UnmarshalFIFOMessage(buf[:n])
			if err != nil {
				continue
			}
			_ = rom.ReceiveMulticast(msg)
		}
	}(conn)

	return nil
}

// InitializeMulticastListenerOnPort hardcodes the multicast group to 224.0.0.1
// and uses groupPort as the UDP port.
func (rom *ReliableFIFOMulticast) InitializeMulticastListenerOnPort(groupPort int) error {
	return rom.InitializeMulticastListener(multicastIP, groupPort)
}

// InitializeUnicastListener starts a UDP listener for NAKs, LASTACKs, and retransmissions.
func (rom *ReliableFIFOMulticast) InitializeUnicastListener(listenPort int) error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	rom.mu.Lock()
	rom.unicastConn = conn
	rom.mu.Unlock()

	go func(c *net.UDPConn) {
		buf := make([]byte, 64*1024)
		for {
			select {
			case <-rom.stopChan:
				return
			default:
			}

			_ = c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			n, remote, err := c.ReadFromUDP(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
			if n == 0 {
				continue
			}

			switch buf[0] {
			case 0x01:
				// retransmitted FIFO message
				msg, err := UnmarshalFIFOMessage(buf[:n])
				if err != nil {
					continue
				}
				_ = rom.ReceiveMulticast(msg)

			case 0x02:
				// NAK
				nak, err := UnmarshalNAKMessage(buf[:n])
				if err != nil {
					continue
				}

				rom.mu.Lock()
				rom.addrMap[nak.RequesterID] = remote
				rom.mu.Unlock()

				_ = rom.RetransmitMissingMessage(nak.RequesterID, nak.MissingSeq)

			case 0x03:
				// LASTACK
				la, err := UnmarshalLastAckMessage(buf[:n])
				if err != nil {
					continue
				}
				rom.mu.Lock()
				rom.addrMap[la.RequesterID] = remote
				rom.lastAck[la.RequesterID] = la.LastSeq
				rom.mu.Unlock()
			}
		}
	}(conn)

	return nil
}

// SendMulticast creates a FIFO message, increments Sp, stores it for retransmission,
// and sends it via IP multicast. Only the leader may call this.
func (rom *ReliableFIFOMulticast) SendMulticast(payload []byte) (*FIFOMessage, error) {
	rom.mu.Lock()
	if !rom.isRunning {
		rom.mu.Unlock()
		return nil, fmt.Errorf("fifo multicast not running")
	}
	if rom.nodeID != rom.leaderID {
		rom.mu.Unlock()
		return nil, fmt.Errorf("only leader may send")
	}
	if rom.multicastSendConn == nil {
		rom.mu.Unlock()
		return nil, fmt.Errorf("multicast not initialized")
	}

	rom.Sp++
	msg := &FIFOMessage{
		SenderID:  rom.nodeID,
		Seq:       rom.Sp,
		Payload:   payload,
		Timestamp: time.Now(),
	}
	rom.sentMessages[msg.Seq] = msg
	mc := rom.multicastSendConn
	rom.mu.Unlock()

	data, err := MarshalFIFOMessage(msg)
	if err != nil {
		return nil, err
	}
	if _, err := mc.Write(data); err != nil {
		return nil, err
	}
	return msg, nil
}

// ReceiveMulticast handles FIFO ordering, buffering, and NAKs.
func (rom *ReliableFIFOMulticast) ReceiveMulticast(msg *FIFOMessage) error {
	rom.mu.Lock()
	if !rom.isRunning {
		rom.mu.Unlock()
		return fmt.Errorf("fifo multicast not running")
	}

	// only accept from leader (single sender)
	if msg.SenderID != rom.leaderID {
		rom.mu.Unlock()
		return nil
	}

	// drop duplicates
	if msg.Seq <= rom.Rq {
		rom.mu.Unlock()
		return nil
	}

	expected := rom.Rq + 1
	if msg.Seq > expected {
		rom.addToHoldbackUnlocked(msg)
		nak := &NAKMessage{
			RequesterID: rom.nodeID,
			MissingSeq:  expected,
			Timestamp:   time.Now(),
		}
		rom.mu.Unlock()
		rom.sendNAK(nak)
		return nil
	}

	var deliverList []*FIFOMessage
	if msg.Seq == expected {
		deliverList = append(deliverList, msg)
		rom.Rq++
		deliverList = append(deliverList, rom.deliverFromHoldbackUnlocked()...)
	} else {
		rom.addToHoldbackUnlocked(msg)
	}

	rom.mu.Unlock()

	for _, m := range deliverList {
		rom.delivery <- m
	}

	return nil
}

// SetPeerAddr registers a unicast address for a participant/leader.
// Needed so receivers can send NAK/LASTACK to the leader.
func (rom *ReliableFIFOMulticast) SetPeerAddr(id uuid.UUID, addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	rom.mu.Lock()
	rom.addrMap[id] = addr
	rom.mu.Unlock()
}

// RetransmitMissingMessage sends a single missing message to requester over unicast.
func (rom *ReliableFIFOMulticast) RetransmitMissingMessage(requesterID uuid.UUID, missingSeq uint32) error {
	rom.mu.RLock()
	if !rom.isRunning {
		rom.mu.RUnlock()
		return fmt.Errorf("fifo multicast not running")
	}
	if rom.nodeID != rom.leaderID {
		rom.mu.RUnlock()
		return fmt.Errorf("only leader retransmits")
	}
	addr := rom.addrMap[requesterID]
	if addr == nil {
		rom.mu.RUnlock()
		return fmt.Errorf("unknown requester address")
	}

	msg := rom.sentMessages[missingSeq]
	uconn := rom.unicastConn
	rom.mu.RUnlock()

	if msg == nil {
		return fmt.Errorf("missing seq %d not found", missingSeq)
	}
	data, err := MarshalFIFOMessage(msg)
	if err != nil {
		return err
	}
	if uconn != nil {
		_, _ = uconn.WriteToUDP(data, addr)
		return nil
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	_, _ = conn.Write(data)
	_ = conn.Close()
	return nil
}

// SendLastAck sends LASTACK to the leader, indicating receiver is done.
func (rom *ReliableFIFOMulticast) SendLastAck() error {
	rom.mu.RLock()
	if !rom.isRunning {
		rom.mu.RUnlock()
		return fmt.Errorf("fifo multicast not running")
	}
	if rom.nodeID == rom.leaderID {
		rom.mu.RUnlock()
		return fmt.Errorf("leader does not send last-ack to itself")
	}
	addr := rom.addrMap[rom.leaderID]
	lastSeq := rom.Rq
	uconn := rom.unicastConn
	rom.mu.RUnlock()

	if addr == nil {
		return fmt.Errorf("unknown leader address")
	}

	msg := &LastAckMessage{
		RequesterID: rom.nodeID,
		LastSeq:     lastSeq,
		Timestamp:   time.Now(),
	}
	data, err := MarshalLastAckMessage(msg)
	if err != nil {
		return err
	}

	if uconn != nil {
		_, _ = uconn.WriteToUDP(data, addr)
		return nil
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	_, _ = conn.Write(data)
	_ = conn.Close()
	return nil
}

// WaitForLastAcks blocks until all non-leader participants have sent a LASTACK
// with LastSeq >= current Sp.
func (rom *ReliableFIFOMulticast) WaitForLastAcks(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if rom.allLastAcksReceived() {
				return nil
			}
		}
	}
}

func (rom *ReliableFIFOMulticast) allLastAcksReceived() bool {
	rom.mu.RLock()
	defer rom.mu.RUnlock()

	if rom.nodeID != rom.leaderID {
		return false
	}

	target := rom.Sp
	for p := range rom.participants {
		if p == rom.leaderID {
			continue
		}
		if rom.lastAck[p] < target {
			return false
		}
	}
	return true
}

// GetDeliveryChannel returns the channel of delivered messages.
func (rom *ReliableFIFOMulticast) GetDeliveryChannel() <-chan *FIFOMessage {
	return rom.delivery
}

func (rom *ReliableFIFOMulticast) addToHoldbackUnlocked(msg *FIFOMessage) {
	for _, existing := range rom.holdback {
		if existing.Seq == msg.Seq {
			return
		}
	}
	rom.holdback = append(rom.holdback, msg)
	for i := len(rom.holdback) - 1; i > 0 && rom.holdback[i].Seq < rom.holdback[i-1].Seq; i-- {
		rom.holdback[i], rom.holdback[i-1] = rom.holdback[i-1], rom.holdback[i]
	}
}

func (rom *ReliableFIFOMulticast) deliverFromHoldbackUnlocked() []*FIFOMessage {
	var delivered []*FIFOMessage
	for {
		if len(rom.holdback) == 0 {
			return delivered
		}
		if rom.holdback[0].Seq != rom.Rq+1 {
			return delivered
		}
		delivered = append(delivered, rom.holdback[0])
		rom.Rq++
		rom.holdback = rom.holdback[1:]
	}
}

func (rom *ReliableFIFOMulticast) sendNAK(nak *NAKMessage) {
	rom.mu.RLock()
	addr := rom.addrMap[rom.leaderID]
	uconn := rom.unicastConn
	rom.mu.RUnlock()

	if addr == nil {
		return
	}
	data, err := MarshalNAKMessage(nak)
	if err != nil {
		return
	}
	if uconn != nil {
		_, _ = uconn.WriteToUDP(data, addr)
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	_, _ = conn.Write(data)
	_ = conn.Close()
}

func isLocalMulticast(ip net.IP) bool {
	ip4 := ip.To4()
	return ip4 != nil && ip4[0] == 224 && ip4[1] == 0 && ip4[2] == 0
}

// MarshalFIFOMessage serializes a FIFOMessage. First byte is a type tag (0x01).
func MarshalFIFOMessage(msg *FIFOMessage) ([]byte, error) {
	buf := &bytes.Buffer{}
	_ = buf.WriteByte(0x01)
	if _, err := buf.Write(msg.SenderID[:]); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Seq); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(msg.Payload))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(msg.Payload); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalFIFOMessage deserializes a FIFOMessage.
func UnmarshalFIFOMessage(data []byte) (*FIFOMessage, error) {
	buf := bytes.NewReader(data)
	var tag byte
	if err := binary.Read(buf, binary.BigEndian, &tag); err != nil {
		return nil, err
	}
	if tag != 0x01 {
		return nil, fmt.Errorf("unexpected message type: %d", tag)
	}
	var idBytes [16]byte
	if _, err := buf.Read(idBytes[:]); err != nil {
		return nil, err
	}
	var seq uint32
	if err := binary.Read(buf, binary.BigEndian, &seq); err != nil {
		return nil, err
	}
	var payloadLen uint32
	if err := binary.Read(buf, binary.BigEndian, &payloadLen); err != nil {
		return nil, err
	}
	payload := make([]byte, payloadLen)
	if _, err := buf.Read(payload); err != nil {
		return nil, err
	}
	var ts int64
	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return nil, err
	}

	return &FIFOMessage{
		SenderID:  uuid.UUID(idBytes),
		Seq:       seq,
		Payload:   payload,
		Timestamp: time.Unix(0, ts),
	}, nil
}

// MarshalNAKMessage serializes a NAKMessage. First byte is a type tag (0x02).
func MarshalNAKMessage(msg *NAKMessage) ([]byte, error) {
	buf := &bytes.Buffer{}
	_ = buf.WriteByte(0x02)
	if _, err := buf.Write(msg.RequesterID[:]); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.MissingSeq); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalNAKMessage deserializes a NAKMessage.
func UnmarshalNAKMessage(data []byte) (*NAKMessage, error) {
	buf := bytes.NewReader(data)
	var tag byte
	if err := binary.Read(buf, binary.BigEndian, &tag); err != nil {
		return nil, err
	}
	if tag != 0x02 {
		return nil, fmt.Errorf("unexpected message type: %d", tag)
	}
	var reqBytes [16]byte
	if _, err := buf.Read(reqBytes[:]); err != nil {
		return nil, err
	}
	var missing uint32
	if err := binary.Read(buf, binary.BigEndian, &missing); err != nil {
		return nil, err
	}
	var ts int64
	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return nil, err
	}

	return &NAKMessage{
		RequesterID: uuid.UUID(reqBytes),
		MissingSeq:  missing,
		Timestamp:   time.Unix(0, ts),
	}, nil
}

// MarshalLastAckMessage serializes a LastAckMessage. First byte is a type tag (0x03).
func MarshalLastAckMessage(msg *LastAckMessage) ([]byte, error) {
	buf := &bytes.Buffer{}
	_ = buf.WriteByte(0x03)
	if _, err := buf.Write(msg.RequesterID[:]); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.LastSeq); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalLastAckMessage deserializes a LastAckMessage.
func UnmarshalLastAckMessage(data []byte) (*LastAckMessage, error) {
	buf := bytes.NewReader(data)
	var tag byte
	if err := binary.Read(buf, binary.BigEndian, &tag); err != nil {
		return nil, err
	}
	if tag != 0x03 {
		return nil, fmt.Errorf("unexpected message type: %d", tag)
	}
	var reqBytes [16]byte
	if _, err := buf.Read(reqBytes[:]); err != nil {
		return nil, err
	}
	var last uint32
	if err := binary.Read(buf, binary.BigEndian, &last); err != nil {
		return nil, err
	}
	var ts int64
	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return nil, err
	}

	return &LastAckMessage{
		RequesterID: uuid.UUID(reqBytes),
		LastSeq:     last,
		Timestamp:   time.Unix(0, ts),
	}, nil
}

func (rom *ReliableFIFOMulticast) debugf(tag string, format string, args ...interface{}) {
	if rom.logger == nil {
		return
	}
	rom.logger.Printf("%s "+format, append([]interface{}{tag}, args...)...)
}
