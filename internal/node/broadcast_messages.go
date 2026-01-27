package node

import (
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

type BroadcastMessageType byte

const (
	BroadcastMessageTypeHeartbeat BroadcastMessageType = 0x01
	BroadcastMessageTypeJoin      BroadcastMessageType = 0x02
)

type BroadcastMessage interface {
	Type() BroadcastMessageType
	Marshal([]byte) error
	Unmarshal([]byte) error
	SizeBytes() uint
}

type BroadcastHeader struct {
	Type BroadcastMessageType
	ID   uuid.UUID
}

func (h *BroadcastHeader) SizeBytes() uint { return 17 }

func (h *BroadcastHeader) Unmarshal(buf []byte) error {
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

func (h *BroadcastHeader) Marshal(buf []byte) error {
	if len(buf) < 17 {
		return NewBufferSizeError(17, len(buf))
	}

	buf[0] = byte(h.Type)
	copy(buf[1:17], h.ID[:])

	return nil
}

type BroadcastMessageJoin struct {
	Host [4]byte
	Port uint32
}

func (m *BroadcastMessageJoin) Type() BroadcastMessageType { return BroadcastMessageTypeJoin }
func (m *BroadcastMessageJoin) SizeBytes() uint            { return 8 }

func (m *BroadcastMessageJoin) Unmarshal(buf []byte) error {
	if len(buf) < 8 {
		return NewBufferSizeError(8, len(buf))
	}

	copy(m.Host[:], buf[0:4])
	m.Port = binary.BigEndian.Uint32(buf[4:8])

	return nil
}

func (m *BroadcastMessageJoin) Marshal(buf []byte) error {
	if len(buf) < 8 {
		return NewBufferSizeError(8, len(buf))
	}

	copy(buf[0:4], m.Host[:])
	binary.BigEndian.PutUint32(buf[4:8], m.Port)

	return nil
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
