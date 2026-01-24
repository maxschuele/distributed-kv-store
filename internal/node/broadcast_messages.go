package node

import (
	"fmt"

	"github.com/google/uuid"
)

type BroadcastMessageType byte

const (
	BroadcastMessageTypeHeartbeat BroadcastMessageType = 0x01
)

var BroadcastHeaderSize = 17

type BroadcastHeader struct {
	Type BroadcastMessageType
	ID   uuid.UUID
}

func (h *BroadcastHeader) Unmarshal(buf []byte) error {
	if len(buf) < 17 {
		return fmt.Errorf("buffer too short: expected at least 17 bytes, got %d", len(buf))
	}

	h.Type = BroadcastMessageType(buf[0])
	id, err := uuid.FromBytes(buf[2:18])
	if err != nil {
		return fmt.Errorf("failed to parse UUID: %w", err)
	}
	h.ID = id

	return nil
}

type BroadcastMessageHeartbeat struct{}

func UnmarshalBroadcastMessageHeartbeat(buf []byte) {}

type BroadcastMessageAnnounce struct{}

func UnmarshalBroadcastMessageAnnounce(buf []byte) {}
