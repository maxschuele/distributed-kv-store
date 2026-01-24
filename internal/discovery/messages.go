package discovery

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/maxschuele/distkv/cluster"
)

// Simple string-based wire format
// Example: ANNOUNCE|nodeID|host|port|groupID|isLeader
const (
	MsgAnnounce        = "ANNOUNCE"
	MsgDiscoverRequest = "DISCOVER_REQUEST"
	MsgDiscoverResp    = "DISCOVER_RESPONSE"
	MsgHeartbeat       = "HEARTBEAT"
)

func EncodeHeartbeat(nodeID string) string {
	return fmt.Sprintf("%s|%s", MsgHeartbeat, nodeID)
}

func DecodeHeartbeat(msg string) (string, error) {
	parts := strings.Split(msg, "|")
	if len(parts) < 2 || parts[0] != MsgHeartbeat {
		return "", fmt.Errorf("invalid heartbeat message")
	}
	return parts[1], nil
}

func EncodeAnnounce(node cluster.NodeInfo) string {
	return fmt.Sprintf("%s|%s|%s|%d|%d|%t",
		MsgAnnounce, node.NodeID, node.Host, node.Port, node.GroupID, node.IsLeader)
}

func DecodeAnnounce(msg string) (cluster.NodeInfo, error) {
	parts := strings.Split(msg, "|")
	if len(parts) != 6 || parts[0] != MsgAnnounce {
		return cluster.NodeInfo{}, fmt.Errorf("invalid announce message")
	}

	port, err := strconv.Atoi(parts[3])
	if err != nil {
		return cluster.NodeInfo{}, err
	}

	groupID, err := strconv.Atoi(parts[4])
	if err != nil {
		return cluster.NodeInfo{}, err
	}

	isLeader, err := strconv.ParseBool(parts[5])
	if err != nil {
		return cluster.NodeInfo{}, err
	}

	return cluster.NodeInfo{
		NodeID:   parts[1],
		Host:     parts[2],
		Port:     port,
		GroupID:  groupID,
		IsLeader: isLeader,
	}, nil
}

func EncodeDiscoverRequest(node cluster.NodeInfo) string {
	return fmt.Sprintf("%s|%s", MsgDiscoverRequest, node.NodeID)
}

func DecodeDiscoverRequest(msg string) (string, error) {
	parts := strings.Split(msg, "|")
	if len(parts) != 2 || parts[0] != MsgDiscoverRequest {
		return "", fmt.Errorf("invalid discover request")
	}
	return parts[1], nil
}
