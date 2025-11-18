package replication

import (
	"fmt"
	"strconv"
	"strings"
)

// Message types for replication
const (
	MsgReplicate = "REPLICATE"
	MsgReplicateAck = "REPLICATE_ACK"
)

type ReplicateMsg struct {
	Key     string
	Value   string
	Version int64
}

func EncodeReplicate(key, value string, version int64) string {
	return fmt.Sprintf("%s|%s|%s|%d", MsgReplicate, key, value, version)
}

func DecodeReplicate(msg string) (ReplicateMsg, error) {
	parts := strings.Split(msg, "|")
	if len(parts) != 4 || parts[0] != MsgReplicate {
		return ReplicateMsg{}, fmt.Errorf("invalid replicate message")
	}

	version, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return ReplicateMsg{}, err
	}

	return ReplicateMsg{
		Key:     parts[1],
		Value:   parts[2],
		Version: version,
	}, nil
}

func EncodeReplicateAck(key string) string {
	return fmt.Sprintf("%s|%s", MsgReplicateAck, key)
}

func DecodeReplicateAck(msg string) (string, error) {
	parts := strings.Split(msg, "|")
	if len(parts) != 2 || parts[0] != MsgReplicateAck {
		return "", fmt.Errorf("invalid replicate ack message")
	}
	return parts[1], nil
}
