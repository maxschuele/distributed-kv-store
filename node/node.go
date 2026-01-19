package node

import (
	"fmt"
	"log"
	"time"

	"github.com/maxschuele/distkv/cluster"
	"github.com/maxschuele/distkv/discovery"
)

type Node struct {
	Info             cluster.NodeInfo
	DiscoveryView    *discovery.GroupView
	ReplicationGroup *cluster.ReplicationGroup
}

func NewNode(id string, ip string, port int) *Node {
	return &Node{
		Info: cluster.NodeInfo{
			NodeID:   id,
			IP:       ip,
			Port:     port,
			GroupID:  -1, // initially -1 since it has no group
			IsLeader: false,
		},
		DiscoveryView: discovery.NewGroupView(),
	}
}

func (n *Node) Start() {
	log.Printf("Starting node %s on %s:%d", n.Info.NodeID, n.Info.IP, n.Info.Port)

	// Start Discovery Listener and passively build group view
	go func() {
		err := discovery.Listen(discovery.ListenPort, n.DiscoveryView, n.Info.NodeID)
		if err != nil {
			log.Fatalf("Discovery listener failed: %v", err)
		}
	}()

	// Start Discovery Broadcast
	go func() {
		// current format DISCOVER_REQUEST|nodeID|host|port (needs to be changed)
		// announcement := fmt.Sprintf("%s|%s|%s|%d", discovery.MsgDiscoverRequest, n.Info.NodeID, n.Info.IP, n.Info.Port)
		announcement := fmt.Sprintf("%s|%s|%s|%d|%d|%t",
			discovery.MsgAnnounce, n.Info.NodeID, n.Info.IP, n.Info.Port, n.Info.GroupID, n.Info.IsLeader)
		err := discovery.Broadcast(discovery.BroadcastIP, discovery.BroadcastPort, announcement)
		if err != nil {
			log.Fatalf("Discovery listener failed: %v", err)
		}
	}()

	// Start sending hearbeats
	go func() {
		discovery.BroadcastHeartbeats(n.Info, 5*time.Second)
	}()

}
