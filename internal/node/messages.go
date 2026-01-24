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
	MessageTypeWriteRequest  MessageType = 0x02
	MessageTypeDeleteRequest MessageType = 0x03
	MessageTypeBroadcast     MessageType = 0x04
)

type Message interface {
	Type() MessageType
	Marshal() []byte
}

type BroadcastMessage struct {
	Id uuid.UUID
}

func (m *BroadcastMessage) Type() MessageType {
	return MessageTypeBroadcast
}

func UnmarshalBroadcastMessage(r io.Reader) (BroadcastMessage, error) {
	idBuf := make([]byte, 16)
	if _, err := io.ReadFull(r, idBuf); err != nil {
		return BroadcastMessage{}, fmt.Errorf("failed to read broadcast ID: %w", err)
	}
	var id uuid.UUID
	copy(id[:], idBuf)
	return BroadcastMessage{
		Id: id,
	}, nil
}

func (m *BroadcastMessage) Marshal() []byte {
	buf := make([]byte, 17) // 1 byte type + 16 bytes UUID
	buf[0] = byte(MessageTypeBroadcast)
	copy(buf[1:], m.Id[:])
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

func Unmarshal(r io.Reader) (Message, error) {
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
	case MessageTypeBroadcast:
		idBuf := make([]byte, 16)
		if _, err := io.ReadFull(r, idBuf); err != nil {
			return nil, fmt.Errorf("failed to read broadcast ID: %w", err)
		}
		var id uuid.UUID
		copy(id[:], idBuf)
		return &BroadcastMessage{
			Id: id,
		}, nil
	default:
		return nil, fmt.Errorf("unknown message type: 0x%02x", msgType)
	}
}
