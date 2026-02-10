package cluster

import (
	"bytes"
	"distributed-kv-store/internal/broadcast"
	"distributed-kv-store/internal/consistent"
	"distributed-kv-store/internal/httpclient"
	"distributed-kv-store/internal/httpserver"
	"distributed-kv-store/internal/logger"
	"distributed-kv-store/internal/multicast"
	"distributed-kv-store/internal/netutil"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	HeartbeatInterval        = 2 * time.Second
	HeartbeatTimeout         = 8 * time.Second
	RebalanceInterval        = 10 * time.Second
	broadcastPort     uint16 = 9898
)

type NodeInfo struct {
	ID          uuid.UUID
	GroupID     uuid.UUID
	Host        [4]byte
	Port        uint16
	HttpPort    uint16
	IsLeader    bool
	Participant bool
}

type Node struct {
	info            NodeInfo
	log             *logger.Logger
	httpServer      *httpserver.Server
	clusterView     *ClusterGroupView
	replicationView *ReplicationGroupView
	clusterListener *net.TCPListener
	consistent      *consistent.ConsistentHash
	storage         *Storage
	rw              sync.RWMutex
	leaderAddr      string
	broadcastPort   uint16
	groupPort       uint16
	iface           *net.Interface

	mcast     *multicast.ReliableFIFOMulticast
	mcastStop chan struct{}
}

type nodeConfig struct {
	ip        string
	groupPort uint16
	isLeader  bool
	logLevel  logger.Level
}

func StartNode(ip string, groupPort uint16, logLevel logger.Level) {
	n, err := newBaseNode(nodeConfig{
		ip:        ip,
		groupPort: groupPort,
		isLeader:  true,
		logLevel:  logLevel,
	})
	if err != nil {
		logger.New(logger.DEBUG).Fatal("%v", err)
	}

	n.info.GroupID = uuid.New()

	n.log.Info("starting node groupID: %s ID: %s", n.info.GroupID.String(), n.info.ID.String())
	n.startActivities()
	n.startLeaderActivities()
}

func StartReplicationNode(ip string, logLevel logger.Level) {
	n, err := newBaseNode(nodeConfig{
		ip:       ip,
		logLevel: logLevel,
	})
	if err != nil {
		logger.New(logger.DEBUG).Fatal("%v", err)
	}

	n.log.Info("starting replication node %s", n.info.ID.String())

	for !n.replicationGroupJoin() {
	}

	n.startActivities()
	n.initMulticast()
}

func newBaseNode(cfg nodeConfig) (*Node, error) {
	log := logger.New(cfg.logLevel)

	iface, ip, err := netutil.FindInterfaceByIP(cfg.ip)
	if err != nil {
		return nil, fmt.Errorf("invalid ip: %w", err)
	}

	info := NodeInfo{
		ID:       uuid.New(),
		Host:     ip,
		IsLeader: cfg.isLeader,
	}
	if cfg.isLeader {
		info.GroupID = uuid.New()
	}

	n := &Node{
		info:            info,
		log:             log,
		clusterView:     NewClusterView(log),
		replicationView: NewGroupView(info.ID, log),
		consistent:      consistent.NewConsistentHash(150),
		storage:         NewStorage(),
		broadcastPort:   broadcastPort,
		groupPort:       cfg.groupPort,
		iface:           iface,
	}

	clusterTcpAddr, err := netutil.ParseTcp4Addr(ip, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster tcp address: %w", err)
	}

	n.clusterListener, err = net.ListenTCP("tcp4", clusterTcpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to set up cluster listener: %w", err)
	}

	n.info.Port = uint16(n.clusterListener.Addr().(*net.TCPAddr).Port)
	// add self to replication view (needed for election), has to be done after getting the clusterListener port
	n.replicationView.AddOrUpdateNode(n.info)

	n.log.Info("started cluster listener on port %d", n.info.Port)

	return n, nil
}

func (n *Node) startActivities() {
	go n.listenClusterMessages()

	go func() {
		if err := broadcast.Listen(n.groupPort, n.log, n.handleReplicationBroadcastMessage); err != nil {
			n.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go n.sendHeartbeats(n.groupPort)
	go n.replicationView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, n.handleReplicationGroupNodeRemoval)
}

func (n *Node) startLeaderActivities() {
	n.log.Info("Starting leader activities")

	n.consistent.AddNode(n.info.GroupID)

	go func() {
		if err := broadcast.Listen(broadcastPort, n.log, n.handleBroadcastMessage); err != nil {
			n.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go n.sendHeartbeats(broadcastPort)
	go n.clusterView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, n.handleClusterGroupNodeRemoval)

	go n.startPeriodicRebalance()

	n.httpServer = httpserver.New()
	go n.startHttpServer()

	n.initMulticast()
}

func (n *Node) handleReplicationGroupNodeRemoval(removedNode NodeInfo) {
	if removedNode.IsLeader {
		n.initiateElection()
	}
	// Rebuild multicast with the updated participant set.
	n.initMulticast()
}

func (n *Node) handleClusterGroupNodeRemoval(removedNode NodeInfo) {
	n.log.Info("Removing node %s from hashing ring", removedNode.GroupID.String())
	n.consistent.RemoveNode(removedNode.GroupID)
}

func (n *Node) listenClusterMessages() {
	for {
		conn, err := n.clusterListener.Accept()
		if err != nil {
			n.log.Error("Error accepting cluster tcp connection: %v", err)
		}
		go n.handleClusterMessage(conn)

	}
}

func (n *Node) handleClusterMessage(conn net.Conn) {
	defer conn.Close()
	msg, err := Unmarshal(conn)
	if err != nil {
		n.log.Error("[Node] Failed to unmarshal cluster messages: %v", err)
		return
	}

	switch m := msg.(type) {
	case *ElectionMessage:
		n.handleElectionMessage(m)
	}
}

func (n *Node) sendHeartbeats(port uint16) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	header := MessageHeader{
		Type: MessageTypeHeartbeat,
		ID:   n.info.ID,
	}
	msg := MessageHeartbeat{
		Info: n.info,
	}
	for {
		n.rw.RLock()
		msg.Info = n.info
		n.rw.RUnlock()

		buf := make([]byte, header.SizeBytes()+msg.SizeBytes())
		if err := header.Marshal(buf); err != nil {
			n.log.Fatal("failed to marshal heartbeat header: %v", err)
		}
		if err := msg.Marshal(buf[header.SizeBytes():]); err != nil {
			n.log.Fatal("failed to marshal heartbeat message: %v", err)
		}

		if err := broadcast.Send(port, buf); err != nil {
			n.log.Error("failed to broadcast heartbeat message: %v", err)
		}
		<-ticker.C
	}
}

func (n *Node) sendTcpMessage(addr string, data []byte) error {
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

func (n *Node) handleBroadcastMessage(buf []byte, remoteAddr *net.UDPAddr) {
	if !isClusterMessage(buf) {
		n.log.Debug("got non node message")
		return
	}

	header := &MessageHeader{}
	if err := header.Unmarshal(buf); err != nil {
		n.log.Error("[Node] Failed to unmarshal header: %v", err)
		return
	}

	if header.ID == n.info.ID {
		return
	}

	payload := buf[header.SizeBytes():]

	switch header.Type {
	case MessageTypeHeartbeat:
		msg := MessageHeartbeat{}
		if err := msg.Unmarshal(payload); err != nil {
			n.log.Error("Failed to unmarshal cluster heartbeat: %v", err)
			return
		}

		_, exists := n.clusterView.GetNode(msg.Info.GroupID)
		n.clusterView.AddOrUpdateNode(msg.Info)
		if !exists {
			n.consistent.AddNode(msg.Info.GroupID)
			n.rebalanceKeys()
		}

	case MessageTypeGroupJoin:
		if !n.info.IsLeader {
			return
		}

		msg := MessageGroupJoin{}
		if err := msg.Unmarshal(payload); err != nil {
			n.log.Error("Failed to unmarshal group join message: %v", err)
			return
		}

		n.sendMsg(netutil.FormatAddress(msg.Host, msg.Port), &MessageGroupInfo{
			GroupID:    n.info.GroupID,
			GroupSize:  uint32(n.replicationView.Size()), // TODO: fix cast
			LeaderHost: n.info.Host,
			LeaderPort: n.info.Port,
			GroupPort:  n.groupPort,
			HttpPort:   n.info.HttpPort,
		})
	}
}

func (n *Node) handleReplicationBroadcastMessage(buf []byte, remoteAddr *net.UDPAddr) {
	if !isClusterMessage(buf) {
		n.log.Debug("got non node message")
		return
	}

	header := &MessageHeader{}
	if err := header.Unmarshal(buf); err != nil {
		n.log.Error("[Node] Failed to unmarshal header: %v", err)
		return
	}

	if header.ID == n.info.ID {
		return
	}

	payload := buf[header.SizeBytes():]

	switch header.Type {
	case MessageTypeHeartbeat:
		msg := MessageHeartbeat{}
		if err := msg.Unmarshal(payload); err != nil {
			n.log.Error("Failed to unmarshal cluster heartbeat: %v", err)
			return
		}

		_, err := n.replicationView.GetNode(msg.Info.ID)
		isNew := err != nil
		n.replicationView.AddOrUpdateNode(msg.Info)

		// A new node joined the replication group — rebuild multicast.
		if isNew {
			n.initMulticast()
		}
	}
}

func (n *Node) rebalanceKeys() {
	n.storage.mu.Lock()
	defer n.storage.mu.Unlock()

	client := httpclient.NewClient()

	for key, value := range n.storage.data {
		groupId, err := n.consistent.GetNode(key)
		if err != nil {
			n.log.Error("failed to get node for key %q: %v", key, err)
			continue
		}

		if groupId == n.info.GroupID {
			continue
		}

		nodeInfo, ok := n.clusterView.GetNode(groupId)
		if !ok {
			n.log.Error("failed to find node for group %s: %v", groupId, err)
			continue
		}

		addr := netutil.FormatAddress(nodeInfo.Host, nodeInfo.HttpPort)
		resp, err := client.Put(addr, "/kv?key="+key, value)
		if err != nil {
			n.log.Error("failed to send key %q to %s: %v", key, addr, err)
			continue
		}

		if resp.StatusCode != 200 {
			n.log.Error("failed to send key %q to %s: status %d %s", key, addr, resp.StatusCode, resp.StatusText)
			continue
		}

		delete(n.storage.data, key)
		n.log.Info("rebalanced key %q to group %s", key, groupId)
	}
}

func (n *Node) startPeriodicRebalance() {
	ticker := time.NewTicker(RebalanceInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.rebalanceKeys()
	}
}

func (n *Node) sendMsg(addr string, m Message) error {
	h := MessageHeader{
		ID:   n.info.ID,
		Type: m.Type(),
	}

	buf := make([]byte, h.SizeBytes()+m.SizeBytes())

	if err := h.Marshal(buf); err != nil {
		return err
	}

	if err := m.Marshal(buf[h.SizeBytes():]); err != nil {
		return err
	}

	if err := n.sendTcpMessage(addr, buf); err != nil {
		return err
	}

	return nil
}

func (n *Node) replicationGroupJoin() bool {
	responses := make(map[uuid.UUID]MessageGroupInfo)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	deadline := time.Now().Add(2 * time.Second)
	n.clusterListener.SetDeadline(deadline)
	defer n.clusterListener.SetDeadline(time.Time{})

	// Start accepting connections before broadcasting
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for time.Now().Before(deadline) {
			conn, err := n.clusterListener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					return
				}
				n.log.Error("failed to accept connection: %v", err)
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				header, msg, ok := n.readGroupInfo(c)
				if ok {
					mu.Lock()
					responses[header.ID] = msg
					mu.Unlock()
				}
			}(conn)
		}
	}()

	// Broadcast join request
	h := MessageHeader{ID: n.info.ID, Type: MessageTypeGroupJoin}
	m := MessageGroupJoin{Host: n.info.Host, Port: n.info.Port}
	buf := make([]byte, h.SizeBytes()+m.SizeBytes())
	if err := h.Marshal(buf); err != nil {
		n.log.Error("failed to marshal broadcast header: %v", err)
		return false
	}
	if err := m.Marshal(buf[h.SizeBytes():]); err != nil {
		n.log.Error("failed to marshal group join message: %v", err)
		return false
	}
	if err := broadcast.Send(broadcastPort, buf); err != nil {
		n.log.Error("failed to send group join broadcast: %v", err)
		return false
	}
	n.log.Info("sent group join broadcast message")

	<-acceptDone
	wg.Wait()

	if len(responses) == 0 {
		return false
	}

	// Select the group with the fewest members
	var selectedID uuid.UUID
	minMembers := math.MaxInt
	for id, info := range responses {
		if int(info.GroupSize) < minMembers {
			minMembers = int(info.GroupSize)
			selectedID = id
		}
	}

	selected := responses[selectedID]
	n.log.Info("selected group %s with %d members", selected.GroupID, minMembers)
	n.groupPort = selected.GroupPort
	n.info.GroupID = selected.GroupID
	n.replicationView.AddOrUpdateNode(NodeInfo{
		ID:       selectedID,
		GroupID:  selected.GroupID,
		Host:     selected.LeaderHost,
		Port:     selected.LeaderPort,
		HttpPort: selected.HttpPort,
		IsLeader: true,
	})
	return true
}

func (n *Node) readGroupInfo(conn net.Conn) (MessageHeader, MessageGroupInfo, bool) {
	var header MessageHeader
	var msg MessageGroupInfo
	buf := make([]byte, header.SizeBytes()+msg.SizeBytes())

	if _, err := io.ReadFull(conn, buf[:4]); err != nil {
		n.log.Error("failed to read magic: %v", err)
		return header, msg, false
	}
	if !isClusterMessage(buf) {
		return header, msg, false
	}
	if _, err := io.ReadFull(conn, buf[4:header.SizeBytes()]); err != nil {
		n.log.Error("failed to read header: %v", err)
		return header, msg, false
	}
	if err := header.Unmarshal(buf); err != nil {
		n.log.Error("failed to unmarshal header: %v", err)
		return header, msg, false
	}
	if header.Type != MessageTypeGroupInfo {
		return header, msg, false
	}
	if _, err := io.ReadFull(conn, buf[header.SizeBytes():header.SizeBytes()+msg.SizeBytes()]); err != nil {
		n.log.Error("failed to read body: %v", err)
		return header, msg, false
	}
	if err := msg.Unmarshal(buf[header.SizeBytes():]); err != nil {
		n.log.Error("failed to unmarshal group info: %v", err)
		return header, msg, false
	}
	return header, msg, true
}

func (n *Node) startHttpServer() {
	httpAddr, err := netutil.ParseTcp4Addr(n.info.Host, 0)

	if err != nil {
		n.log.Fatal("invalid HTTp address: %v", err)
	}

	if err := n.httpServer.Bind(httpAddr); err != nil {
		n.log.Fatal("failed to start http server: %v", err)
	}
	n.info.HttpPort = n.httpServer.Port
	n.httpServer.RegisterHandler(httpserver.GET, "/kv", n.handleGetKey)
	n.httpServer.RegisterHandler(httpserver.PUT, "/kv", n.handlePutKey)
	n.httpServer.RegisterHandler(httpserver.DELETE, "/kv", n.handleDeleteKey)

	n.log.Info("Starting http server on port %d", n.info.HttpPort)
	if err := n.httpServer.Listen(); err != nil {
		n.log.Error("[Node] Error in HTTP Listen: %v", err)
	}
}

func (n *Node) handleGetKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
	key, ok := r.Params["key"]
	if !ok || key == "" {
		w.Status(400, "Bad Request")
		w.Write([]byte("key parameter is required"))
		return
	}

	// check whether the request of the client was made correctly
	destinationID, err := n.consistent.GetNode(key)
	if err != nil || (destinationID != n.info.GroupID) {
		w.Status(404, "Not Found")
		w.Write([]byte("Request was sent to wrong node"))
		return
	}

	val, ok := n.storage.Get(key)
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

	// check whether the request of the client was made correctly
	destinationID, err := n.consistent.GetNode(key)
	if err != nil || (destinationID != n.info.GroupID) {
		w.Status(404, "Not Found")
		w.Write([]byte("Request was sent to wrong node"))
		return
	}

	val := r.Body
	n.storage.Set(key, r.Body)
	n.writeNotifyMembers(key, val)

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

	// check whether the request of the client was made correctly
	destinationID, err := n.consistent.GetNode(key)
	if err != nil || (destinationID != n.info.GroupID) {
		w.Status(404, "Not Found")
		w.Write([]byte("Request was sent to wrong node"))
		return
	}

	_, ok = n.storage.Get(key)
	if !ok {
		w.Status(404, "Not Found")
		w.Write([]byte("key not found"))
		return
	}

	n.storage.Delete(key)
	n.deleteNotifyMembers(key)

	w.Status(200, "OK")
	w.Write([]byte("OK"))
}

func (n *Node) writeNotifyMembers(key string, value string) {
	w := WriteRequestMessage{
		Key:   []byte(key),
		Value: []byte(value),
	}
	n.notifyMembers(w.Marshal())
}

func (n *Node) deleteNotifyMembers(key string) {
	d := DeleteRequestMessage{
		Key: []byte(key),
	}
	n.notifyMembers(d.Marshal())
}

func (n *Node) notifyMembers(data []byte) {
	n.rw.RLock()
	mcast := n.mcast
	n.rw.RUnlock()

	if mcast == nil {
		n.log.Debug("[multicast] not initialized, skipping propagation")
		return
	}

	if _, err := mcast.SendMulticast(data); err != nil {
		n.log.Error("[multicast] failed to send: %v", err)
	}
}

// initMulticast tears down any existing multicast instance and creates a new
// one that reflects the current replication group membership. It is safe to
// call multiple times (e.g. after membership changes).
func (n *Node) initMulticast() {
	n.stopMulticast()

	nodes := n.replicationView.GetNodes()
	if len(nodes) < 2 {
		n.log.Debug("[multicast] not enough participants (%d), skipping init", len(nodes))
		return
	}

	// Determine leader and build participant list.
	var leaderID uuid.UUID
	var leaderAddr *net.UDPAddr
	participants := make([]uuid.UUID, 0, len(nodes))
	for _, ni := range nodes {
		participants = append(participants, ni.ID)
		if ni.IsLeader {
			leaderID = ni.ID
			leaderAddr = &net.UDPAddr{
				IP:   net.IP(ni.Host[:]),
				Port: int(ni.Port),
			}
		}
	}

	if leaderID == uuid.Nil {
		n.log.Warn("[multicast] no leader found, retrying init soon")
		time.AfterFunc(500*time.Millisecond, n.initMulticast)
		return
	}

	isLeader := n.info.ID == leaderID

	var rom *multicast.ReliableFIFOMulticast
	if isLeader {
		rom = multicast.NewReliableFIFOMulticast(n.info.ID, leaderID, participants, n.log)
	} else {
		rom = multicast.NewReliableFIFOMulticastWithLeaderAddr(n.info.ID, leaderID, participants, leaderAddr, n.log)
	}

	rom.Start()

	// Register unicast addresses for all peers so NAK/retransmit can reach them.
	for _, ni := range nodes {
		if ni.ID != n.info.ID {
			rom.SetPeerAddr(ni.ID, &net.UDPAddr{
				IP:   net.IP(ni.Host[:]),
				Port: int(ni.Port),
			})
		}
	}

	mcastPort := int(n.groupPort) + 1

	if err := rom.InitializeUnicastListener(int(n.info.Port)); err != nil {
		n.log.Error("[multicast] failed to start unicast listener: %v", err)
		rom.Stop()
		return
	}
	if err := rom.InitializeMulticastListenerOnPort(mcastPort); err != nil {
		n.log.Error("[multicast] failed to start multicast listener: %v", err)
		rom.Stop()
		return
	}

	stop := make(chan struct{})

	n.rw.Lock()
	n.mcast = rom
	n.mcastStop = stop
	n.rw.Unlock()

	// Non-leader nodes consume delivered messages and apply them to storage.
	if !isLeader {
		go n.consumeMulticastDeliveries(rom, stop)
	}

	n.log.Info("[multicast] initialized (leader=%v, participants=%d, mcastPort=%d)",
		isLeader, len(participants), mcastPort)
}

// stopMulticast tears down the current multicast instance, if any.
func (n *Node) stopMulticast() {
	n.rw.Lock()
	rom := n.mcast
	stop := n.mcastStop
	n.mcast = nil
	n.mcastStop = nil
	n.rw.Unlock()

	if rom != nil {
		rom.Stop()
	}
	if stop != nil {
		close(stop)
	}
}

// consumeMulticastDeliveries reads fully-reassembled messages from the
// multicast delivery channel and applies write/delete operations to storage.
func (n *Node) consumeMulticastDeliveries(rom *multicast.ReliableFIFOMulticast, stop <-chan struct{}) {
	ch := rom.GetDeliveryChannel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			n.applyMulticastPayload(msg.Payload)
		case <-stop:
			return
		}
	}
}

// applyMulticastPayload deserialises a replicated write or delete and applies
// it to the local storage.
func (n *Node) applyMulticastPayload(payload []byte) {
	msg, err := Unmarshal(bytes.NewReader(payload))
	if err != nil {
		n.log.Error("[multicast] failed to unmarshal payload: %v", err)
		return
	}

	switch m := msg.(type) {
	case *WriteRequestMessage:
		n.storage.Set(string(m.Key), string(m.Value))
		n.log.Info("[multicast] applied write: key=%s", string(m.Key))
	case *DeleteRequestMessage:
		n.storage.Delete(string(m.Key))
		n.log.Info("[multicast] applied delete: key=%s", string(m.Key))
	default:
		n.log.Error("[multicast] unexpected message type in payload")
	}
}

// getReplicationGroupParticipants returns the UUIDs of all nodes in this
// node's replication group.
func (n *Node) getReplicationGroupParticipants() []uuid.UUID {
	nodes := n.replicationView.GetNodes()
	participants := make([]uuid.UUID, 0, len(nodes))
	for _, ni := range nodes {
		if ni.GroupID == n.info.GroupID {
			participants = append(participants, ni.ID)
		}
	}
	return participants
}
