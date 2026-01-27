package election

import (
	"fmt"

	"bytes"
	"distributed-kv-store/internal/node"

	"github.com/google/uuid"
)

type ElectionMessage struct {
	Mid      uuid.UUID
	IsLeader bool
}

func InitiateElection(nodeInfo *node.NodeInfo) {
	fmt.Printf("[Election] Node %s: Initiating Election\n", nodeInfo.ID)
	nodeInfo.Participant = true
	msg := ElectionMessage{Mid: nodeInfo.ID, IsLeader: nodeInfo.IsLeader}
	fmt.Printf("[Election] Node %s: Sending Election Message: Candidate=%s \n", nodeInfo.ID, msg.Mid)
	forwardElectionMessage(msg)
}

func HandleElectionMessage(msg ElectionMessage, nodeInfo *node.NodeInfo) {

	if msg.IsLeader == true {
		nodeInfo.Participant = false
		// node updates the uuid which is now leader (TODO)
		fmt.Printf("[Election] Node %s: LEADER ANNOUNCEMENT received. New Leader is: %s\n", nodeInfo.ID, msg.Mid)
		forwardElectionMessage(msg)
	}

	if CompareUUID(nodeInfo.ID, msg.Mid) < 0 && !nodeInfo.Participant {
		// Received ID is lower
		fmt.Printf("[Election] Node %s: My ID is HIGHER. Forwarding new message.\n", nodeInfo.ID)
		msg.Mid = nodeInfo.ID
		nodeInfo.Participant = true
		forwardElectionMessage(msg)
	} else if CompareUUID(nodeInfo.ID, msg.Mid) > 0 {
		// Received ID is higher, forward the message
		fmt.Printf("[Election] Node %s: My ID is LOWER. Forwarding existing message.\n", nodeInfo.ID)
		nodeInfo.Participant = true
		forwardElectionMessage(msg)
	} else if CompareUUID(nodeInfo.ID, msg.Mid) == 0 {
		// IDs are equal, this node becomes the leader
		fmt.Printf("[Election] Node %s: IDs MATCH. I have won the election!\n", nodeInfo.ID)
		nodeInfo.IsLeader = true
		msg.IsLeader = true
		nodeInfo.Participant = false
		fmt.Printf("[Election] Node %s: sending Leader Announcement.\n", nodeInfo.ID)
		forwardElectionMessage(msg)
	}
}

/*
	 returns:
		-1: uuid1 < uuid2
		 0: uuid1 = uuid2
		 1: uuid1 > uuid2
*/
func CompareUUID(uuid1 uuid.UUID, uuid2 uuid.UUID) int {
	return bytes.Compare(uuid1[:], uuid2[:])
}

func forwardElectionMessage(msg ElectionMessage) {
	// TODO send to successor in ring
	// successorID via groupview (TODO sort groupview by uuid and implement function to get successor)
}
