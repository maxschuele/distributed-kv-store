package node

import (
	"distributed-kv-store/internal/broadcast"
	"distributed-kv-store/internal/httpserver"
	"distributed-kv-store/internal/logger"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	HeartbeatInterval  = 5 * time.Second
	HeartbeatTimeout   = 15 * time.Second
	GroupSizeThreshold = 3
)

type NodeInfo struct {
	ID          uuid.UUID
	GroupID     uuid.UUID
	Host        [4]byte
	Port        uint32
	IsLeader    bool
	Participant bool
}

type Node struct {
	info            NodeInfo
	log             *logger.Logger
	httpServer      *httpserver.Server
	groupView       *GroupView
	store           map[string]string
	rw              sync.RWMutex
	clusterListener net.Listener
	isLeader        bool
	group           map[string]bool
	leaderAddr      string
	broadcastPort   int
	groupPort       uint32
}

func NewNode(ip string, httpPort string, clusterPort string, isLeader bool, group []string, leaderAddr string, broadcastPort int, groupPort uint32) (*Node, error) {
	log := logger.New(logger.DEBUG)

	httpAddr, err := parseTcp4Addr(fmt.Sprintf("%s:%s", ip, httpPort))

	log.Info("[Node] HTTP address: %s", formatAddress(httpAddr.Host, httpAddr.Port))

	if err != nil {
		return nil, fmt.Errorf("invalid HTTP address: %w", err)
	}
	clusterTcpAddr, err := parseTcp4Addr(fmt.Sprintf("%s:%s", ip, clusterPort))
	if err != nil {
		return nil, fmt.Errorf("invalid cluster address: %w", err)
	}

	n := &Node{
		info: NodeInfo{
			ID:       uuid.New(),
			GroupID:  uuid.Nil,
			Host:     clusterTcpAddr.Host,
			Port:     clusterTcpAddr.Port,
			IsLeader: false,
		},
		log:           log,
		httpServer:    httpserver.New(),
		groupView:     NewGroupView(log),
		store:         make(map[string]string),
		rw:            sync.RWMutex{},
		group:         make(map[string]bool),
		isLeader:      isLeader,
		leaderAddr:    leaderAddr,
		broadcastPort: broadcastPort,
		groupPort:     groupPort,
	}

	// Start heartbeat monitor
	go n.groupView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval)

	for _, member := range group {
		n.group[member] = true
	}

	// Setup HTTP server
	if err := n.httpServer.Bind(httpAddr.Str); err != nil {
		return nil, err
	}
	n.httpServer.RegisterHandler(httpserver.GET, "/kv", n.handleGetKey)
	n.httpServer.RegisterHandler(httpserver.PUT, "/kv", n.handlePutKey)
	n.httpServer.RegisterHandler(httpserver.DELETE, "/kv", n.handleDeleteKey)

	go func() {
		for {
			if err := n.httpServer.Listen(); err != nil {
				n.log.Error("[Node] Error in HTTP Listen: %v", err)
			}
		}
	}()

	// Setup group listener
	n.clusterListener, err = net.Listen("tcp4", clusterTcpAddr.Str)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			conn, err := n.clusterListener.Accept()
			if err != nil {
				n.log.Error("[Node] Error accepting connection: %v", err)
			}
			go n.handleGroupMessage(conn)
		}
	}()

	// Setup broadcast listener
	// go func() {
	// 	if err := broadcast.Listen(broadcastPort, n.log, n.handleBroadcastMessage); err != nil {
	// 		fmt.Println("Error listening to broadcast: ", err)
	// 	}
	// }()

	go n.initiateJoinProcess()

	return n, nil
}

func (n *Node) initiateJoinProcess() {
	choices := []int{1, 2, 3, 4, 5}
	retries := choices[rand.Intn(len(choices))]

	n.log.Info("[Node] Group join: Retry %d times", retries)
	msg := &BroadcastMessageJoin{
		Host: n.info.Host,
		Port: n.info.Port,
	}

	// Send cluster join message
	n.sendBroadcastMessage(msg)

	for i := 0; i < retries; i++ {
		time.Sleep(1 * time.Second) // possible debug

		// check if node already joined a group
		n.rw.RLock()
		joined := n.info.GroupID != uuid.Nil
		n.rw.RUnlock()

		if joined {
			return
		}

		time.Sleep(4 * time.Second)
		n.sendBroadcastMessage(msg)
	}

	// if no group found start a new group/cluster
	n.startNewCluster()
}

func (n *Node) startNewCluster() {
	n.rw.Lock()
	defer n.rw.Unlock()

	n.info.GroupID = uuid.New()
	n.info.IsLeader = true
	n.isLeader = true
	n.leaderAddr = formatAddress(n.info.Host, n.info.Port)

	n.log.Info("[Node] Became leader of new group %s", n.info.GroupID.String())
	n.startListenAndSendHeartbeats()
}

func (n *Node) handleGroupMessage(conn net.Conn) {
	defer conn.Close()

	msg, err := Unmarshal(conn)
	if err != nil {
		// TODO
		n.log.Error("[Node] Failed to unmarshal group message: %v", err)
		return
	}

	switch m := msg.(type) {
	case *HeartbeatMessage:
		n.handleHeartbeat(m)

	case *WriteRequestMessage:
		n.log.Info("[Node] Received group write key message")
		k := string(m.Key)
		v := string(m.Value)
		n.rw.Lock()
		n.store[k] = v
		n.rw.Unlock()

		if n.isLeader {
			n.log.Info("[Node] Sending key %s with value %s to members", k, v)
			n.writeNotifyMembers(k, v)
		}

	case *DeleteRequestMessage:
		k := string(m.Key)
		n.log.Info("[Node] Received group delete for key %s", k)
		n.rw.Lock()
		delete(n.store, k)
		n.rw.Unlock()

		if n.isLeader {
			n.log.Info("[Node] Sending delete for key %s to members", k)
			n.deleteNotifyMembers(k)
		}

	case *NodeInfoMessage:
		n.log.Info("[Node] Received node info message")
		if n.isLeader {
			// if m.isLeader {
			// 	n.groupView.AddOrUpdateNode(m.Info)
			// 	return
			// }

			if n.getClusterSize() < GroupSizeThreshold {
				n.groupView.AddOrUpdateNode(m.Info)
				n.sendClusterInviteMessage(m.Info)
			}
		}

	case *ElectionMessage:
		n.handleElectionMessage(m)

	case *ClusterJoinMessage:
		n.handleClusterJoin(m)
	}

}

func (n *Node) handleGetKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
	key, ok := r.Params["key"]
	if !ok || key == "" {
		w.Status(400, "Bad Request")
		w.Write([]byte("key parameter is required"))
		return
	}

	n.rw.RLock()
	defer n.rw.RUnlock()

	val, ok := n.store[key]
	if !ok {
		w.Status(404, "key not found")
		w.Write([]byte("key not found"))
		return
	}

	w.Write([]byte(val))
}

func (n *Node) handlePutKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
	key, ok := r.Params["key"]
	if !ok || key == "" {
		w.Status(400, "Bad Request")
		w.Write([]byte("key parameter is required"))
		return
	}

	value := r.Body

	if n.isLeader {
		n.rw.Lock()
		defer n.rw.Unlock()
		n.store[key] = value
		n.writeNotifyMembers(key, value)
	} else {
		n.writeNotifyLeader(key, value)
	}

	w.Status(200, "OK")
	w.Write([]byte("OK"))
}

func (n *Node) handleDeleteKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
	key, ok := r.Params["key"]
	if !ok || key == "" {
		w.Status(400, "Bad Request")
		w.Write([]byte("key parameter is required"))
		return
	}

	n.rw.Lock()
	defer n.rw.Unlock()

	_, ok = n.store[key]
	if !ok {
		w.Status(404, "Not Found")
		w.Write([]byte("key not found"))
		return
	}

	if n.isLeader {
		delete(n.store, key)
		n.deleteNotifyMembers(key)
	} else {
		n.deleteNotifyLeader(key)
	}

	w.Status(200, "OK")
	w.Write([]byte("OK"))
}

func (n *Node) writeNotifyLeader(key string, value string) error {
	w := WriteRequestMessage{
		Key:   []byte(key),
		Value: []byte(value),
	}
	n.notifyPeer(n.leaderAddr, w.Marshal())
	return nil
}

func (n *Node) writeNotifyMembers(key string, value string) {
	w := WriteRequestMessage{
		Key:   []byte(key),
		Value: []byte(value),
	}
	n.notifyMembers(w.Marshal())
}

func (n *Node) deleteNotifyLeader(key string) error {
	d := DeleteRequestMessage{
		Key: []byte(key),
	}
	return n.notifyPeer(n.leaderAddr, d.Marshal())
}

func (n *Node) deleteNotifyMembers(key string) error {
	d := DeleteRequestMessage{
		Key: []byte(key),
	}
	n.notifyMembers(d.Marshal())
	return nil
}

func (n *Node) notifyPeer(addr string, data []byte) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		n.log.Error("[Node] Connection to peer failed: %v", err)
		return err
	}
	defer conn.Close()

	// TODO: set a timeout, handle partial write
	_, err = conn.Write(data)
	if err != nil {
		n.log.Error("[Node] Write to peer failed: %v", err)
		return err
	}
	return nil
}

func (n *Node) notifyMembers(data []byte) {
	for addr := range n.group {
		if err := n.notifyPeer(addr, data); err != nil {
			// TODO: handle error appropriately
		}
	}
}

func (n *Node) handleBroadcastMessage(buf []byte, remoteAddr *net.UDPAddr) {
	header := &BroadcastHeader{}
	if err := header.Unmarshal(buf); err != nil {
		n.log.Error("[Node] Failed to unmarshal header: %v", err)
		return
	}

	if header.ID == n.info.ID {
		return
	}

	payload := buf[header.SizeBytes():]

	switch header.Type {
	case BroadcastMessageTypeJoin:
		msg := BroadcastMessageJoin{}
		if err := msg.Unmarshal(payload); err != nil {
			n.log.Error("[Node] Failed to unmarshal payload for join message: %v", err)
			return
		}

		m := NodeInfoMessage{
			Info: n.info,
		}
		b := m.Marshal()

		addr := formatAddress(msg.Host, msg.Port)
		n.log.Info("[Node] Notifying peer at %s", addr)
		n.notifyPeer(addr, b)
	}
}

func (n *Node) sendTcpMsg(addr string, buf []byte) {
	//TODO ?
}

func (n *Node) sendBroadcastMessage(m BroadcastMessage) error {
	h := BroadcastHeader{
		Type: m.Type(),
		ID:   n.info.ID,
	}

	buf := make([]byte, h.SizeBytes()+m.SizeBytes())

	if err := h.Marshal(buf[0:h.SizeBytes()]); err != nil {
		return err
	}

	if err := m.Marshal(buf[h.SizeBytes():]); err != nil {
		return err
	}

	if err := broadcast.Send(n.broadcastPort, buf); err != nil {
		return err
	}

	return nil
}

func (n *Node) StartHeartbeat() {
	for {
		hb := &HeartbeatMessage{Info: n.info}
		data := hb.Marshal()

		if n.isLeader {
			if err := broadcast.Send(n.broadcastPort, data); err != nil {
				n.log.Error("[Heartbeat] broadcast send failed: %v", err)
			}
		}
		if err := broadcast.Send(int(n.groupPort), data); err != nil {
			n.log.Error("[Heartbeat] group send failed: %v", err)
		}

		time.Sleep(HeartbeatInterval)
	}
}

func (n *Node) handleHeartbeat(m *HeartbeatMessage) {
	n.groupView.AddOrUpdateNode(m.Info)
}

func (n *Node) handleClusterJoin(m *ClusterJoinMessage) {
	n.rw.Lock()
	defer n.rw.Unlock()

	// node joins group
	n.leaderAddr = formatAddress(m.Host, m.Port)
	n.info.GroupID = m.GroupID
	n.groupPort = m.GroupPort
	n.log.Info("[Node] Joining Group with ID %s", n.info.GroupID.String())
	n.startListenAndSendHeartbeats()

}

func (n *Node) startListenAndSendHeartbeats() {
	go broadcast.Listen(int(n.groupPort), n.log, n.handleBroadcastMessage)
	go n.StartHeartbeat()
}

// sendClusterInviteMessage sends a ClusterJoinMessage to invite a node to join this cluster
func (n *Node) sendClusterInviteMessage(info NodeInfo) {
	msg := &ClusterJoinMessage{
		GroupID:   n.info.GroupID,
		Host:      n.info.Host,
		Port:      n.info.Port,
		GroupPort: n.groupPort,
	}

	addr := formatAddress(info.Host, info.Port)
	if err := n.notifyPeer(addr, msg.Marshal()); err != nil {
		n.log.Error("[Node] Failed to send cluster invite to %s: %v", addr, err)
	} else {
		n.log.Info("[Node] Sent cluster invite to %s for group %s", addr, n.info.GroupID.String())
	}
}

// GetClusterSize returns the number of nodes in the group view with the same GroupID
func (n *Node) getClusterSize() int {
	count := 0
	gv := n.groupView
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	for _, node := range n.groupView.nodes {
		if node.Info.GroupID == n.info.GroupID {
			count++
		}
	}

	return count
}
