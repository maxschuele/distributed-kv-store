package cluster

type GroupInfo struct {
	GroupID int
	Leader  string   // NodeID of leader
	Members []string // NodeIDs
}

func (g *GroupInfo) IsMember(nodeID string) bool {
	for _, id := range g.Members {
		if id == nodeID {
			return true
		}
	}
	return false
}
