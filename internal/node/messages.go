package node

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/uuid"
)

type MessageType byte

const (
	MessageTypeHeartbeat     MessageType = 0x01
	MessageTypeClusterJoin   MessageType = 0x02
	MessageTypeNodeInfo      MessageType = 0x03
	MessageTypeWriteRequest  MessageType = 0x04
	MessageTypeDeleteRequest MessageType = 0x05
	MessageTypeElection      MessageType = 0x06
)

type MessageHeader struct {
	Type BroadcastMessageType
	ID   uuid.UUID
}

func (h *MessageHeader) SizeBytes() uint { return 17 }

func (h *MessageHeader) Unmarshal(buf []byte) error {
	if len(buf) < 17 {
		return NewBufferSizeError(17, len(buf))
	}

	h.Type = BroadcastMessageType(buf[0])
	id, err := uuid.FromBytes(buf[1:17])
	if err != nil {
		return fmt.Errorf("failed to parse UUID: %w", err)
	}
	h.ID = id

	return nil
}

func (h *MessageHeader) Marshal(buf []byte) error {
	if len(buf) < 17 {
		return NewBufferSizeError(17, len(buf))
	}

	buf[0] = byte(h.Type)
	copy(buf[1:17], h.ID[:])

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
	// [4 bytes: Port]
	// [1 byte: IsLeader]
	// [1 byte: Participant]

	totalSize := 1 + 16 + 16 + 4 + 4 + 1 + 1 // 43 bytes
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

	binary.BigEndian.PutUint32(buf[offset:offset+4], m.Info.Port)
	offset += 4

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

type HeartbeatMessage struct{}

func (h *HeartbeatMessage) Type() MessageType {
	return MessageTypeHeartbeat
}

func (h *HeartbeatMessage) Marshal() []byte {
	return []byte{byte(MessageTypeHeartbeat)}
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
	case MessageTypeHeartbeat:
		return &HeartbeatMessage{}, nil

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
		buf := make([]byte, 42) // 16+16+4+4+1+1
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

		msg.Info.Port = binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4

		msg.Info.IsLeader = buf[offset] != 0
		offset += 1

		msg.Info.Participant = buf[offset] != 0

		return msg, nil
	case MessageTypeElection:
		buf := make([]byte, 18) // 1 + 16 + 1
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

	default:
		return nil, fmt.Errorf("unknown message type: 0x%02x", msgType)
	}
}
