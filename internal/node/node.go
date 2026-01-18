package node

import (
	"distributed-kv-store/internal/httpserver"
	"fmt"
	"net"
	"sync"
)

type Node struct {
	httpServer    *httpserver.Server
	store         map[string]string
	rw            sync.RWMutex
	groupListener net.Listener
	isLeader      bool
	group         map[string]bool // TODO: maybe store heartbeat here
	leaderAddr    string
}

func NewNode() *Node {
	return &Node{
		httpServer: httpserver.New(),
		store:      make(map[string]string),
		rw:         sync.RWMutex{},
		group:      map[string]bool{},
	}
}

func (n *Node) Init(httpAddr string, groupAddr string, isLeader bool, group []string, leaderAddr string) error {
	n.isLeader = isLeader
	n.leaderAddr = leaderAddr

	for _, member := range group {
		n.group[member] = true
	}

	err := n.httpServer.Bind(httpAddr)
	if err != nil {
		return err
	}

	n.httpServer.RegisterHandler(httpserver.GET, "/kv", n.handleGetKey)
	n.httpServer.RegisterHandler(httpserver.PUT, "/kv", n.handlePutKey)
	n.httpServer.RegisterHandler(httpserver.DELETE, "/kv", n.handleDeleteKey)

	go func() {
		for {
			err := n.httpServer.Listen()
			if err != nil {
				fmt.Println("Error in Listen:", err)
			}
		}
	}()

	n.groupListener, err = net.Listen("tcp", groupAddr)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := n.groupListener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err)
			}
			go n.handleGroupMessage(conn)
		}
	}()

	return nil
}

func (n *Node) handleGroupMessage(conn net.Conn) {
	defer conn.Close()

	msg, err := Unmarshal(conn)
	if err != nil {
		// TODO
		fmt.Println("failed to unmarshal group message")
		return
	}

	switch m := msg.(type) {
	case *HeartbeatMessage:
		// TODO
		break
	case *WriteRequestMessage:
		fmt.Println("received group write message")
		k := string(m.Key)
		v := string(m.Value)
		// Directly write to store to avoid infinite recursion
		n.rw.Lock()
		n.store[k] = v
		n.rw.Unlock()

		if n.isLeader {
			fmt.Println("sending key to members")
			n.writeNotifyMembers(k, v)
		}
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
	n.putKey(key, value)

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

	delete(n.store, key)

	w.Status(200, "OK")
	w.Write([]byte("OK"))
}

func (n *Node) putKey(key string, value string) {
	if n.isLeader {
		n.rw.Lock()
		defer n.rw.Unlock()
		n.store[key] = value
		n.writeNotifyMembers(key, value)
	} else {
		n.writeNotifyLeader(key, value)
	}
}

func (n *Node) deleteKey(key string) {
	n.rw.Lock()
	defer n.rw.Unlock()
	delete(n.store, key)
}

func (n *Node) writeNotifyLeader(key string, value string) error {
	w := WriteRequestMessage{
		Key:   []byte(key),
		Value: []byte(value),
	}
	bytes := w.Marshal()

	conn, err := net.Dial("tcp", n.leaderAddr)
	if err != nil {
		fmt.Println("conn to leader failed")
	}

	// TODO: set a timeout, handle partial write
	_, err = conn.Write(bytes)
	if err != nil {
		fmt.Println("write to leader failed")
	}

	return nil
}

func (n *Node) writeNotifyMembers(key string, value string) {
	w := WriteRequestMessage{
		Key:   []byte(key),
		Value: []byte(value),
	}
	bytes := w.Marshal()

	for addr := range n.group {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// TODO
			fmt.Println("conn to member failed")
		}

		// TODO: set a timeout, handle partial write
		_, err = conn.Write(bytes)
		if err != nil {
			// TODO
			fmt.Println("write to member failed")
		}
	}
}
