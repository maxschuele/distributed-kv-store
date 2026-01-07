package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type NodeInfo struct {
	NodeID   string
	Host     string
	Port     int
	GroupID  int
	IsLeader bool
}

func NewLocalNode(port int, groupID int, isLeader bool) NodeInfo {
	return NodeInfo{
		NodeID:   generateNodeID(),
		Host:     "127.0.0.1",
		Port:     port,
		GroupID:  groupID,
		IsLeader: isLeader,
	}
}

func generateNodeID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

func (n NodeInfo) Address() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

func (n NodeInfo) String() string {
	role := "follower"
	if n.IsLeader {
		role = "leader"
	}
	return fmt.Sprintf("Node[%s, group=%d, %s, %s]", n.NodeID[:8], n.GroupID, role, n.Address())
}
