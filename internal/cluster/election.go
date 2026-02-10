package cluster

import (
	"bytes"
	"distributed-kv-store/internal/netutil"
	"time"

	"github.com/google/uuid"
)

func (n *Node) initiateElection() {
	n.rw.Lock()

	// if already participating the node does not need to initiate again
	if n.info.Participant {
		n.rw.Unlock()
		n.log.Debug("[Election] Already participating don't initate election")
		return
	}

	// if one node in the group is already a leader it does not need to start a new election
	if n.hasExistingLeader() {
		n.rw.Unlock()
		n.log.Debug("[Election] Leader already exists, skipping initiation")
		return
	}

	// if only node in group, self-elect as leader immediately
	if n.replicationView.Size() <= 1 {
		n.info.IsLeader = true
		n.info.Participant = false
		n.rw.Unlock()
		n.log.Info("[Election] Only node in group, self-electing as leader")
		n.startLeaderActivities()
		return
	}

	n.info.Participant = true
	n.rw.Unlock()

	// if after a timeout leader is not elected restart election
	go n.restartElectionAfterTimeOut()
	n.log.Info("[Election] Initiating election")

	n.forwardElectionMessage(&ElectionMessage{
		CandidateID: n.info.ID,
		IsLeader:    false,
	})
}

func (n *Node) handleElectionMessage(msg *ElectionMessage) {
	n.rw.Lock()
	//defer n.rw.Unlock()

	// Leader Announcement
	if msg.IsLeader {
		n.log.Info("[Election] New Leader Announced: %s", msg.CandidateID)
		n.info.Participant = false

		if !n.info.IsLeader {
			// Find the leader's address in GroupView
			leaderNode, err := n.replicationView.GetNode(msg.CandidateID)

			if err != nil {
				// if new leader died after announcement restart election
				n.log.Warn("[Election] Received message for dead/unknown leader %s. Dropping and restarting.", msg.CandidateID)
				n.info.Participant = false
				n.rw.Unlock()
				n.initiateElection()
				return
			}
			n.leaderAddr = netutil.FormatAddress(leaderNode.Host, leaderNode.Port)
			n.rw.Unlock()
			n.forwardElectionMessage(msg)
		} else {
			n.leaderAddr = netutil.FormatAddress(n.info.Host, n.info.Port)
			n.rw.Unlock()
		}
		return
	}

	// Case 2: Voting Phase
	cmp := bytes.Compare(n.info.ID[:], msg.CandidateID[:])

	if msg.CandidateID != n.info.ID {
		// check first if this node still lives
		_, err := n.replicationView.GetNode(msg.CandidateID)
		// if no, initiate new election
		if err != nil {
			n.log.Warn("[Election] Received message for dead/unknown candidate %s. Dropping and restarting.", msg.CandidateID)
			n.info.Participant = false
			n.rw.Unlock()
			n.initiateElection()
			return
		}
	}

	if cmp > 0 {
		// My ID is higher than the candidate's, send my ID.
		if !n.info.Participant {
			n.log.Info("[Election] Received lower ID (%s). Starting my own election.", msg.CandidateID)
			n.info.Participant = true
			n.rw.Unlock()
			go n.forwardElectionMessage(&ElectionMessage{CandidateID: n.info.ID, IsLeader: false})
			// if after a timeout leader is not elected restart election
			go n.restartElectionAfterTimeOut()
		} else {
			n.rw.Unlock()
		}
	} else if cmp < 0 {
		// My ID is lower. I accept this candidate and forward the message.
		n.log.Info("[Election] Received higher ID (%s). Forwarding.", msg.CandidateID)
		n.info.Participant = true
		n.rw.Unlock()
		go n.forwardElectionMessage(msg)
		// if after a timeout leader is not elected restart election
		go n.restartElectionAfterTimeOut()
	} else {
		// IDs match. The message circled back to me. I won!
		n.log.Info("[Election] I have won the election!")
		n.info.IsLeader = true
		n.info.Participant = false

		// Send Leader Announcement
		announceMsg := &ElectionMessage{
			CandidateID: n.info.ID,
			IsLeader:    true,
		}
		n.rw.Unlock()
		go n.forwardElectionMessage(announceMsg)

		// start activites as new leader
		n.startLeaderActivities()
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

func (n *Node) forwardElectionMessage(msg *ElectionMessage) {
	data := msg.Marshal()
	for {
		// determine successor node within group view
		successorNode, err := n.replicationView.GetSuccessor(n.info.ID)
		// TODO, what to do if there is no successor found
		if err != nil {
			n.log.Error(err.Error())
			break
		}

		recipient := netutil.FormatAddress(successorNode.Host, successorNode.Port)
		n.log.Info("[Election] sucessor node is %s, sending tcp msg to %s", successorNode.ID.String(), recipient)

		errTCP := n.sendTcpMessage(recipient, data)
		if errTCP != nil {
			// try resending
			n.log.Error("[Election] Failed to send message to successor %s: %v", recipient, err)
			time.Sleep(HeartbeatTimeout)
		} else {
			break
		}

	}
}

func (n *Node) hasExistingLeader() bool {
	for _, node := range n.replicationView.GetNodes() {
		if node.IsLeader {
			return true
		}
	}
	return false
}

func (n *Node) restartElectionAfterTimeOut() {
	for {
		// if election took to long restart it
		time.Sleep(2 * HeartbeatTimeout)
		n.rw.Lock()
		if n.info.Participant {
			n.info.Participant = false
			n.rw.Unlock()
			n.initiateElection()
			return
		}
		n.rw.Unlock()
	}

}
