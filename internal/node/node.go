package node

import (
	"fmt"
	"sync"

	"distributed-kv-store/internal/httpserver"
)

type Node struct {
	httpServer *httpserver.Server
	store      map[string]string
	rw         sync.RWMutex
}

func NewNode() *Node {
	return &Node{
		httpServer: httpserver.New(),
		store:      make(map[string]string),
		rw:         sync.RWMutex{},
	}
}

func (n *Node) Init(httpAddr string) error {
	err := n.httpServer.Bind(httpAddr)
	if err != nil {
		return err
	}

	n.httpServer.RegisterHandler(httpserver.GET, "/kv", n.HandleGetKey)
	n.httpServer.RegisterHandler(httpserver.PUT, "/kv", n.HandlePutKey)
	n.httpServer.RegisterHandler(httpserver.DELETE, "/kv", n.HandleDeleteKey)

	go func() {
		for {
			err := n.httpServer.Listen()
			if err != nil {
				fmt.Println("Error in Listen:", err)
			}
		}
	}()

	return nil
}

func (n *Node) HandleGetKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
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

func (n *Node) HandlePutKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
	key, ok := r.Params["key"]
	if !ok || key == "" {
		w.Status(400, "Bad Request")
		w.Write([]byte("key parameter is required"))
		return
	}

	value := r.Body
	n.rw.Lock()
	defer n.rw.Unlock()
	n.store[key] = value
	w.Status(200, "OK")
	w.Write([]byte("OK"))
}

func (n *Node) HandleDeleteKey(w *httpserver.ResponseWriter, r *httpserver.Request) {
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
