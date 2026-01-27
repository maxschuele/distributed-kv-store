package node

import (
	"bytes"

	"github.com/google/uuid"
)

func (n *Node) InitiateElection() {
	n.rw.Lock()
	n.info.Participant = true
	n.rw.Unlock()

	n.log.Info("[Election] Initiating election")

	msg := &ElectionMessage{
		CandidateID: n.info.ID,
		IsLeader:    false,
	}
	n.forwardElectionMessage(msg)
}

func (n *Node) handleElectionMessage(msg *ElectionMessage) {
	n.rw.Lock()
	defer n.rw.Unlock()

	// Case 1: Leader Announcement
	if msg.IsLeader {
		n.log.Info("[Election] New Leader Announced: %s", msg.CandidateID)

		n.info.Participant = false

		if !n.isLeader {
			// Find the leader's address in GroupView
			leaderNode, err := n.groupView.GetNode(msg.CandidateID)

			if err != nil {
				//TODO
			}

			n.leaderAddr = formatAddress(leaderNode.Host, leaderNode.Port)

			go n.forwardElectionMessage(msg)
		} else {
			n.leaderAddr = formatAddress(n.info.Host, n.info.Port)
		}
		return
	}

	// Case 2: Voting Phase
	cmp := bytes.Compare(n.info.ID[:], msg.CandidateID[:])

	if cmp > 0 {
		// My ID is higher than the candidate's, send my ID.
		if !n.info.Participant {
			n.log.Info("[Election] Received lower ID (%s). Starting my own election.", msg.CandidateID)
			n.info.Participant = true
			go n.forwardElectionMessage(&ElectionMessage{CandidateID: n.info.ID, IsLeader: false})
		}
	} else if cmp < 0 {
		// My ID is lower. I accept this candidate and forward the message.
		n.log.Info("[Election] Received higher ID (%s). Forwarding.", msg.CandidateID)
		n.info.Participant = true
		go n.forwardElectionMessage(msg)
	} else {
		// IDs match. The message circled back to me. I won!
		n.log.Info("[Election] I have won the election!")
		n.isLeader = true
		n.info.Participant = false

		// Send Leader Announcement
		announceMsg := &ElectionMessage{
			CandidateID: n.info.ID,
			IsLeader:    true,
		}
		go n.forwardElectionMessage(announceMsg)
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
	// determine successor node within group view
	successorNode, err := n.groupView.GetSuccessor(n.info.ID)

	// TODO, what to do if there is no successor found
	if err != nil {
		n.log.Error(err.Error())
	}

	data := msg.Marshal()
	recipient := formatAddress(successorNode.Host, successorNode.Port)
	if err := n.notifyPeer(recipient, data); err != nil {
		n.log.Error("[Election] Failed to send message to successor %s: %v", recipient, err)
		// TODO: think if we resend or send to next
	}
}
