package node

import (
	"context"
	"distributed-kv-store/internal/broadcast"
	"distributed-kv-store/internal/httpserver"
	"distributed-kv-store/internal/logger"
	"distributed-kv-store/internal/netutil"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	HeartbeatInterval        = 2 * time.Second
	HeartbeatTimeout         = 8 * time.Second
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
	clusterView     *GroupView
	replicationView *GroupView
	clusterListener *net.TCPListener
	storage         *Storage
	rw              sync.RWMutex
	leaderAddr      string
	broadcastPort   uint16
	groupPort       uint16
	iface           *net.Interface
}

type nodeConfig struct {
	ip          string
	clusterPort uint16
	httpPort    uint16
	groupPort   uint16
	isLeader    bool
	logLevel    logger.Level
}

func StartNode(ip string, clusterPort uint16, httpPort uint16, groupPort uint16, logLevel logger.Level) {
	if err := netutil.ValidatePort(httpPort); err != nil {
		logger.New(logger.DEBUG).Fatal("invalid http port: %v", err)
	}

	if err := netutil.ValidatePort(groupPort); err != nil {
		logger.New(logger.DEBUG).Fatal("invalid group port: %v", err)
	}

	n, err := newBaseNode(nodeConfig{
		ip:          ip,
		clusterPort: clusterPort,
		httpPort:    httpPort,
		groupPort:   groupPort,
		isLeader:    true,
		logLevel:    logLevel,
	})
	if err != nil {
		logger.New(logger.DEBUG).Fatal("%v", err)
	}

	n.httpServer = httpserver.New()

	n.log.Info("starting node %s", n.info.ID.String())

	go n.listenClusterMessages()

	go func() {
		if err := broadcast.Listen(broadcastPort, n.log, n.handleBroadcastMessage); err != nil {
			n.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go func() {
		if err := broadcast.Listen(groupPort, n.log, n.handleReplicationBroadcastMessage); err != nil {
			n.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go n.sendHeartbeats(broadcastPort)
	go n.sendHeartbeats(n.groupPort)
	go n.clusterView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, n.InitiateElection)
	go n.replicationView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, n.InitiateElection)

	go n.startHttpServer()
}

func StartReplicationNode(ip string, clusterPort uint16, logLevel logger.Level) {
	n, err := newBaseNode(nodeConfig{
		ip:          ip,
		clusterPort: clusterPort,
		logLevel:    logLevel,
	})
	if err != nil {
		logger.New(logger.DEBUG).Fatal("%v", err)
	}

	n.log.Info("starting replication node %s with cluster port %d and broadcast port %d", n.info.ID.String(), clusterPort, broadcastPort)

	for !n.groupJoin() {
	}

	go n.listenClusterMessages()

	go func() {
		if err := broadcast.Listen(n.groupPort, n.log, n.handleReplicationBroadcastMessage); err != nil {
			n.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go n.sendHeartbeats(n.groupPort)
	go n.replicationView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, n.InitiateElection)
}

func newBaseNode(cfg nodeConfig) (*Node, error) {
	log := logger.New(cfg.logLevel)

	iface, ip, err := netutil.FindInterfaceByIP(cfg.ip)
	if err != nil {
		return nil, fmt.Errorf("invalid ip: %w", err)
	}

	if err := netutil.ValidatePort(cfg.clusterPort); err != nil {
		return nil, fmt.Errorf("invalid cluster port: %w", err)
	}

	info := NodeInfo{
		ID:       uuid.New(),
		Host:     ip,
		Port:     cfg.clusterPort,
		HttpPort: cfg.httpPort,
		IsLeader: cfg.isLeader,
	}
	if cfg.isLeader {
		info.GroupID = uuid.New()
	}

	n := &Node{
		info:            info,
		log:             log,
		clusterView:     NewGroupView(info.ID, log, ClusterGroupViewType),
		replicationView: NewGroupView(info.ID, log, ReplicationGroupViewType),
		storage:         NewStorage(),
		broadcastPort:   broadcastPort,
		groupPort:       cfg.groupPort,
		iface:           iface,
	}

	// TODO: add self to clusterView and replicationView

	clusterTcpAddr, err := netutil.ParseTcp4Addr(ip, cfg.clusterPort)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster tcp address: %w", err)
	}

	n.clusterListener, err = net.ListenTCP("tcp4", clusterTcpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to set up cluster listener: %w", err)
	}

	return n, nil
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

		n.clusterView.AddOrUpdateNode(msg.Info)
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

	// do not ignore own
	// if header.ID == n.info.ID {
	// 	return
	// }

	payload := buf[header.SizeBytes():]

	switch header.Type {
	case MessageTypeHeartbeat:
		msg := MessageHeartbeat{}
		if err := msg.Unmarshal(payload); err != nil {
			n.log.Error("Failed to unmarshal cluster heartbeat: %v", err)
			return
		}

		n.replicationView.AddOrUpdateNode(msg.Info)
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

func (n *Node) groupJoin() bool {
	responses := make(map[uuid.UUID]MessageGroupInfo)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// Start listener with timeout
	timeout := 5 * time.Second
	deadline := time.Now().Add(timeout)
	n.clusterListener.SetDeadline(deadline)
	defer n.clusterListener.SetDeadline(time.Time{})

	// Accept connections in background
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	go func() {
		header := MessageHeader{}
		msg := MessageGroupInfo{}

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
				n.log.Info("got messasge")
				defer wg.Done()
				defer c.Close()

				buf := make([]byte, header.SizeBytes()+msg.SizeBytes())

				// Read magic
				if _, err := io.ReadFull(c, buf[0:4]); err != nil {
					n.log.Error("failed to read magic: %v", err)
					return
				}
				if !isClusterMessage(buf) {
					n.log.Info("not node message")
					return
				}

				// Read rest of header
				if _, err := io.ReadFull(c, buf[4:header.SizeBytes()]); err != nil {
					n.log.Error("failed to read header: %v", err)
					return
				}
				if err := header.Unmarshal(buf); err != nil {
					n.log.Error("failed to unmarshal header: %v", err)
					return
				}
				if header.Type != MessageTypeGroupInfo {
					n.log.Info("not group info")
					return
				}

				// Read message body
				if _, err := io.ReadFull(c, buf[header.SizeBytes():header.SizeBytes()+msg.SizeBytes()]); err != nil {
					n.log.Error("failed to read message body: %v", err)
					return
				}
				if err := msg.Unmarshal(buf[header.SizeBytes():]); err != nil {
					n.log.Error("failed to unmarshal group info: %v", err)
					return
				}

				n.log.Info("got group info from %s", header.ID)
				mu.Lock()
				responses[header.ID] = msg
				mu.Unlock()
			}(conn)
		}
	}()

	// Broadcast join request
	h := MessageHeader{
		ID:   n.info.ID,
		Type: MessageTypeGroupJoin,
	}
	m := MessageGroupJoin{
		Host: n.info.Host,
		Port: n.info.Port,
	}
	buf := make([]byte, h.SizeBytes()+m.SizeBytes())
	if err := h.Marshal(buf); err != nil {
		n.log.Error("failed to marshal broadcast header: %v", err)
		return false
	}
	if err := m.Marshal(buf[h.SizeBytes():]); err != nil {
		n.log.Error("failed to marshal group join message: %v", err)
		return false
	}

	for range 4 {
		if err := broadcast.Send(broadcastPort, buf); err != nil {
			n.log.Error("failed to send group join broadcast message: %v", err)
		}
		n.log.Info("sent group join broadcast message")
		// if i < 3 {
		time.Sleep(1 * time.Second)
		// }
	}

	<-ctx.Done()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(responses) == 0 {
		return false
	}

	// Select the group with the least members
	var selectedID uuid.UUID
	minMembers := int(^uint(0) >> 1) // max int

	for id, info := range responses {
		if int(info.GroupSize) < minMembers {
			minMembers = int(info.GroupSize)
			selectedID = id
		}
	}

	n.log.Info("selected group %s with %d members", selectedID, minMembers)

	selected := responses[selectedID]

	n.groupPort = selected.GroupPort
	n.info.GroupID = selected.GroupID

	n.replicationView.AddOrUpdateNode(NodeInfo{
		ID:       selectedID,
		GroupID:  selected.GroupID,
		Host:     selected.LeaderHost,
		Port:     selected.LeaderPort,
		IsLeader: true,
	})

	return true
}

func (n *Node) startHttpServer() {
	httpAddr, err := netutil.ParseTcp4Addr(n.info.Host, n.info.HttpPort)
	if err != nil {
		n.log.Fatal("invalid HTTp address: %v", err)
	}

	if err := n.httpServer.Bind(httpAddr); err != nil {
		n.log.Fatal("failed to start http server: %v", err)
	}
	n.httpServer.RegisterHandler(httpserver.GET, "/kv", n.handleGetKey)
	n.httpServer.RegisterHandler(httpserver.PUT, "/kv", n.handlePutKey)
	n.httpServer.RegisterHandler(httpserver.DELETE, "/kv", n.handleDeleteKey)

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

	val := r.Body
	n.storage.Set(key, r.Body)
	n.writeNotifyMembers(key, val) // TODO: replace with multicast

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

	_, ok = n.storage.Get(key)
	if !ok {
		w.Status(404, "Not Found")
		w.Write([]byte("key not found"))
		return
	}

	n.storage.Delete(key)
	n.deleteNotifyMembers(key) // TODO: replace with multicast

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
	// TODO: use multicast for this
	// for addr := range n.group {
	// 	if err := n.notifyPeer(addr, data); err != nil {
	// 		// TODO: handle error appropriately
	// 	}
	// }
}
