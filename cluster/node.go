package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type NodeInfo struct {
	NodeID   string
	IP       string
	Port     int
	GroupID  int
	IsLeader bool
}

func NewLocalNode(ip string, port int, groupID int) NodeInfo {
	return NodeInfo{
		NodeID:   generateNodeID(),
		IP:       ip,
		Port:     port,
		GroupID:  groupID,
		IsLeader: false,
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
	return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

func (n NodeInfo) String() string {
	role := "follower"
	if n.IsLeader {
		role = "leader"
	}
	return fmt.Sprintf("Node[%s, group=%d, %s, %s]", n.NodeID, n.GroupID, role, n.Address())
}
