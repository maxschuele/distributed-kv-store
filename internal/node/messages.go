package node

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/uuid"
)

type MessageType byte

type Message interface {
	SizeBytes() int
	Type() MessageType
	Unmarshal(buf []byte) error
	Marshal(buf []byte) error
}

const (
	MessageTypeHeartbeat     MessageType = 0x01
	MessageTypeClusterJoin   MessageType = 0x02
	MessageTypeNodeInfo      MessageType = 0x03
	MessageTypeWriteRequest  MessageType = 0x04
	MessageTypeDeleteRequest MessageType = 0x05
	MessageTypeElection      MessageType = 0x06
	MessageTypeGroupJoin     MessageType = 0x07
	MessageTypeGroupInfo     MessageType = 0x08
)

const MessageHeaderMagic uint32 = 0x44534B56 // "DSKV" in ASCII

func isClusterMessage(buf []byte) bool {
	if len(buf) < 4 {
		return false
	}

	return binary.BigEndian.Uint32(buf[0:4]) == MessageHeaderMagic
}

type MessageHeader struct {
	Magic uint32
	Type  MessageType
	ID    uuid.UUID
}

func (h *MessageHeader) SizeBytes() int { return 21 } // 4 (magic) + 1 (type) + 16 (UUID)

func (h *MessageHeader) Unmarshal(buf []byte) error {
	if len(buf) < h.SizeBytes() {
		return NewBufferSizeError(h.SizeBytes(), len(buf))
	}

	h.Magic = binary.BigEndian.Uint32(buf[0:4])
	if h.Magic != MessageHeaderMagic {
		return fmt.Errorf("invalid magic number")
	}

	h.Type = MessageType(buf[4])

	id, err := uuid.FromBytes(buf[5:h.SizeBytes()])
	if err != nil {
		return fmt.Errorf("failed to parse UUID: %w", err)
	}
	h.ID = id

	return nil
}

func (h *MessageHeader) Marshal(buf []byte) error {
	if len(buf) < h.SizeBytes() {
		return NewBufferSizeError(h.SizeBytes(), len(buf))
	}

	binary.BigEndian.PutUint32(buf[0:4], MessageHeaderMagic)
	buf[4] = byte(h.Type)
	copy(buf[5:h.SizeBytes()], h.ID[:])

	return nil
}

type MessageGroupJoin struct {
	Host [4]byte
	Port uint16
}

func (m *MessageGroupJoin) SizeBytes() int {
	return 6 // 4 (IP) + 2 (Port)
}

func (m *MessageGroupJoin) Marshal(buf []byte) error {
	if len(buf) < m.SizeBytes() {
		return NewBufferSizeError(m.SizeBytes(), len(buf))
	}
	copy(buf[0:4], m.Host[:])
	binary.BigEndian.PutUint16(buf[4:6], m.Port)
	return nil
}

func (m *MessageGroupJoin) Unmarshal(buf []byte) error {
	if len(buf) < m.SizeBytes() {
		return NewBufferSizeError(m.SizeBytes(), len(buf))
	}
	copy(m.Host[:], buf[0:4])
	m.Port = binary.BigEndian.Uint16(buf[4:6])
	return nil
}

// TODO: use NodeInfo instead, add GroupPort to NodeInfo
type MessageGroupInfo struct {
	GroupID    uuid.UUID
	GroupSize  uint32
	LeaderHost [4]byte
	LeaderPort uint16
	GroupPort  uint16
}

func (m *MessageGroupInfo) SizeBytes() int    { return 16 + 4 + 4 + 2 + 2 }
func (m *MessageGroupInfo) Type() MessageType { return MessageTypeGroupInfo }

func (m *MessageGroupInfo) Marshal(buf []byte) error {
	if len(buf) < m.SizeBytes() {
		return NewBufferSizeError(m.SizeBytes(), len(buf))
	}
	copy(buf[0:16], m.GroupID[:])
	binary.BigEndian.PutUint32(buf[16:20], m.GroupSize)
	copy(buf[20:24], m.LeaderHost[:])
	binary.BigEndian.PutUint16(buf[24:26], m.LeaderPort)
	binary.BigEndian.PutUint16(buf[26:28], m.GroupPort)
	return nil
}

func (m *MessageGroupInfo) Unmarshal(buf []byte) error {
	if len(buf) < m.SizeBytes() {
		return NewBufferSizeError(m.SizeBytes(), len(buf))
	}
	copy(m.GroupID[:], buf[0:16])
	m.GroupSize = binary.BigEndian.Uint32(buf[16:20])
	copy(m.LeaderHost[:], buf[20:24])
	m.LeaderPort = binary.BigEndian.Uint16(buf[24:26])
	m.GroupPort = binary.BigEndian.Uint16(buf[26:28])
	return nil
}

type TcpMessage interface {
	Type() MessageType
	Marshal() []byte
}

type NodeInfoMessage struct {
	Info NodeInfo
}

func (m *NodeInfoMessage) Type() MessageType {
	return MessageTypeNodeInfo
}

func (m *NodeInfoMessage) Marshal() []byte {
	// Message format:
	// [1 byte: message type]
	// [16 bytes: ID UUID]
	// [16 bytes: GroupID UUID]
	// [4 bytes: Host]
	// [2 bytes: Port]
	// [2 bytes: HttpPort]
	// [1 byte: IsLeader]
	// [1 byte: Participant]

	totalSize := 43
	buf := make([]byte, totalSize)
	offset := 0
	buf[offset] = byte(MessageTypeNodeInfo)
	offset += 1
	copy(buf[offset:offset+16], m.Info.ID[:])
	offset += 16
	copy(buf[offset:offset+16], m.Info.GroupID[:])
	offset += 16
	copy(buf[offset:offset+4], m.Info.Host[:])
	offset += 4
	binary.BigEndian.PutUint16(buf[offset:offset+2], m.Info.Port)
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:offset+2], m.Info.HttpPort)
	offset += 2

	if m.Info.IsLeader {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset += 1

	if m.Info.Participant {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	return buf
}

type ElectionMessage struct {
	CandidateID uuid.UUID
	IsLeader    bool
}

func (e *ElectionMessage) Type() MessageType {
	return MessageTypeElection
}

func (e *ElectionMessage) Marshal() []byte {
	// Message Format:
	// [1 byte: Type]
	// [16 bytes: UUID]
	// [1 byte: IsLeader]
	buf := make([]byte, 18)
	buf[0] = byte(MessageTypeElection)
	copy(buf[1:17], e.CandidateID[:])
	if e.IsLeader {
		buf[17] = 1
	} else {
		buf[17] = 0
	}
	return buf
}

type MessageHeartbeat struct {
	Info NodeInfo
}

func (h *MessageHeartbeat) SizeBytes() int    { return 43 }
func (h *MessageHeartbeat) Type() MessageType { return MessageTypeHeartbeat }

func (h *MessageHeartbeat) Marshal(buf []byte) error {
	if len(buf) < h.SizeBytes() {
		return NewBufferSizeError(h.SizeBytes(), len(buf))
	}

	offset := 0
	buf[offset] = byte(MessageTypeHeartbeat)
	offset++
	offset += copy(buf[offset:offset+16], h.Info.ID[:])
	offset += copy(buf[offset:offset+16], h.Info.GroupID[:])
	offset += copy(buf[offset:offset+4], h.Info.Host[:])
	binary.BigEndian.PutUint16(buf[offset:offset+2], h.Info.Port)
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:offset+2], h.Info.HttpPort)
	offset += 2
	buf[offset] = boolToByte(h.Info.IsLeader)
	offset++
	buf[offset] = boolToByte(h.Info.Participant)

	return nil
}

func (h *MessageHeartbeat) Unmarshal(buf []byte) error {
	if len(buf) < h.SizeBytes() {
		return NewBufferSizeError(h.SizeBytes(), len(buf))
	}
	offset := 0
	offset++ // skip message type byte
	copy(h.Info.ID[:], buf[offset:offset+16])
	offset += 16
	copy(h.Info.GroupID[:], buf[offset:offset+16])
	offset += 16
	copy(h.Info.Host[:], buf[offset:offset+4])
	offset += 4
	h.Info.Port = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	h.Info.HttpPort = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	h.Info.IsLeader = byteToBool(buf[offset])
	offset++
	h.Info.Participant = byteToBool(buf[offset])
	return nil
}

type ClusterJoinMessage struct {
	GroupID   uuid.UUID
	Host      [4]byte
	Port      uint16
	GroupPort uint16
}

func (h *ClusterJoinMessage) Type() MessageType {
	return MessageTypeClusterJoin
}

func (h *ClusterJoinMessage) Marshal() []byte {
	// Message format:
	// [1 byte: message type]
	// [16 bytes: GroupID]
	// [4 bytes: Host]
	// [2 bytes: Port]
	// [2 bytes: GroupPort]

	totalSize := 25 // 25 bytes (was 29)
	buf := make([]byte, totalSize)
	offset := 0
	buf[offset] = byte(MessageTypeClusterJoin)
	offset += 1
	copy(buf[offset:offset+16], h.GroupID[:])
	offset += 16
	copy(buf[offset:offset+4], h.Host[:])
	offset += 4
	binary.BigEndian.PutUint16(buf[offset:offset+2], h.Port)
	offset += 2
	binary.BigEndian.PutUint16(buf[offset:offset+2], h.GroupPort)

	return buf
}

type WriteRequestMessage struct {
	Key   []byte
	Value []byte
}

func (w *WriteRequestMessage) Type() MessageType {
	return MessageTypeWriteRequest
}

func (w *WriteRequestMessage) Marshal() []byte {
	// Message format:
	// [1 byte: message type]
	// [4 bytes: key length]
	// [n bytes: key data]
	// [4 bytes: value length]
	// [m bytes: value data]

	keyLen := len(w.Key)
	valueLen := len(w.Value)

	totalSize := 1 + 4 + keyLen + 4 + valueLen
	buf := make([]byte, totalSize)

	buf[0] = byte(MessageTypeWriteRequest)

	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	copy(buf[5:5+keyLen], w.Key)

	binary.BigEndian.PutUint32(buf[5+keyLen:9+keyLen], uint32(valueLen))
	copy(buf[9+keyLen:], w.Value)

	return buf
}

type DeleteRequestMessage struct {
	Key []byte
}

func (d *DeleteRequestMessage) Type() MessageType {
	return MessageTypeDeleteRequest
}

func (d *DeleteRequestMessage) Marshal() []byte {
	// Message format:
	// [1 byte: message type]
	// [4 bytes: key length]
	// [n bytes: key data]

	keyLen := len(d.Key)

	totalSize := 1 + 4 + keyLen
	buf := make([]byte, totalSize)

	buf[0] = byte(MessageTypeDeleteRequest)

	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	copy(buf[5:5+keyLen], d.Key)

	return buf
}

func Unmarshal(r io.Reader) (TcpMessage, error) {
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}
	msgType := MessageType(typeBuf[0])
	switch msgType {
	case MessageTypeWriteRequest:
		keyLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, keyLenBuf); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, fmt.Errorf("failed to read key data: %w", err)
		}
		valueLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, valueLenBuf); err != nil {
			return nil, fmt.Errorf("failed to read value length: %w", err)
		}
		valueLen := binary.BigEndian.Uint32(valueLenBuf)
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("failed to read value data: %w", err)
		}
		return &WriteRequestMessage{
			Key:   key,
			Value: value,
		}, nil
	case MessageTypeDeleteRequest:
		keyLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, keyLenBuf); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, fmt.Errorf("Failed to read value length: %w", err)
		}
		return &DeleteRequestMessage{
			Key: key,
		}, nil
	case MessageTypeNodeInfo:
		buf := make([]byte, 42) // 16+16+4+2+2+1+1
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("failed to read node info data: %w", err)
		}
		msg := &NodeInfoMessage{}
		offset := 0
		copy(msg.Info.ID[:], buf[offset:offset+16])
		offset += 16
		copy(msg.Info.GroupID[:], buf[offset:offset+16])
		offset += 16
		copy(msg.Info.Host[:], buf[offset:offset+4])
		offset += 4
		msg.Info.Port = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2
		msg.Info.HttpPort = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2
		msg.Info.IsLeader = buf[offset] != 0
		offset += 1
		msg.Info.Participant = buf[offset] != 0
		return msg, nil
	case MessageTypeElection:
		buf := make([]byte, 17) // 16 + 1
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("failed to read node info data: %w", err)
		}
		id, err := uuid.FromBytes(buf[0:16])
		if err != nil {
			return nil, fmt.Errorf("invalid uuid: %w", err)
		}
		return &ElectionMessage{
			CandidateID: id,
			IsLeader:    buf[16] == 1,
		}, nil
	case MessageTypeClusterJoin:
		buf := make([]byte, 24) // 16+4+2+2 (was 28)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("failed to read node info data: %w", err)
		}
		msg := &ClusterJoinMessage{}
		offset := 0
		copy(msg.GroupID[:], buf[offset:offset+16])
		offset += 16
		copy(msg.Host[:], buf[offset:offset+4])
		offset += 4
		msg.Port = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2
		msg.GroupPort = binary.BigEndian.Uint16(buf[offset : offset+2])
		return msg, nil
	default:
		return nil, fmt.Errorf("unknown message type: 0x%02x", msgType)
	}
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func byteToBool(b byte) bool {
	return b != 0
}

type BufferSizeError struct {
	Expected int
	Got      int
}

func (e *BufferSizeError) Error() string {
	return fmt.Sprintf("buffer too small: expected at least %d bytes, got %d", e.Expected, e.Got)
}

func NewBufferSizeError(expected, got int) *BufferSizeError {
	return &BufferSizeError{Expected: expected, Got: got}
}
