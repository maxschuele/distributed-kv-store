package node

import (
	"distributed-kv-store/internal/broadcast"
	"distributed-kv-store/internal/httpclient"
	"distributed-kv-store/internal/logger"
	"fmt"
	"net"
	"strings"

	"github.com/google/uuid"
)

type Client struct {
	broadcastPort uint16
	clusterView   *GroupView
	log           *logger.Logger
	httpClient    *httpclient.Client
}

func StartNewClient(broadcastPort uint16, logFilePath string, logLevel logger.Level) (*Client, error) {
	log, err := logger.NewFileLogger(logFilePath, logLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to start client %w", err)
	}

	log.Info("Starting client")
	client := &Client{
		broadcastPort: broadcastPort,
		clusterView:   NewGroupView(uuid.Nil, log, ClusterGroupViewType),
		httpClient:    httpclient.NewClient(),
		log:           log,
	}
	go func() {
		if err := broadcast.Listen(broadcastPort, client.log, client.handleBroadcastMessage); err != nil {
			client.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	return client, nil
}

func (c *Client) handleBroadcastMessage(buf []byte, remoteAddr *net.UDPAddr) {
	if !isClusterMessage(buf) {
		c.log.Debug("got non node message")
		return
	}
	header := &MessageHeader{}
	if err := header.Unmarshal(buf); err != nil {
		c.log.Error("Failed to unmarshal header: %v", err)
		return
	}
	payload := buf[header.SizeBytes():]
	switch header.Type {
	case MessageTypeHeartbeat:
		msg := MessageHeartbeat{}
		if err := msg.Unmarshal(payload); err != nil {
			c.log.Error("Failed to unmarshal cluster heartbeat: %v", err)
			return
		}
		c.clusterView.AddOrUpdateNode(msg.Info)
	}
}

func (c *Client) ProcessInput(input string) {
	command, values, found := strings.Cut(input, " ")
	if !found {
		fmt.Println("invalid command")
		return
	}
	switch command {
	case "GET":
		key, _, found := strings.Cut(values, " ")
		if found {
			fmt.Println("error: GET takes only a key, no extra arguments")
			return
		}
		c.doGet(key)
	case "PUT":
		key, value, found := strings.Cut(values, " ")
		if !found {
			fmt.Println("error: PUT requires a key and a value")
			return
		}
		c.doPut(key, value)
	case "DELETE":
		key, _, found := strings.Cut(values, " ")
		if found {
			fmt.Println("error: DELETE takes only a key, no extra arguments")
			return
		}
		c.doDelete(key)
	default:
		fmt.Println("invalid command")
	}
}

// pickNode returns the address of an available node, or empty string if none.
func (c *Client) pickNode() string {
	nodes := c.clusterView.GetNodes()
	if len(nodes) == 0 {
		return ""
	}
	node := nodes[0]
	return fmt.Sprintf("%s:%d", node.HostString(), node.HttpPort)
}

func (c *Client) doGet(key string) {
	addr := c.pickNode()
	if addr == "" {
		fmt.Println("error: no nodes available")
		return
	}
	resp, err := c.httpClient.Get(addr, "/kv?key="+key)
	if err != nil {
		c.log.Error("GET failed: %v", err)
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("error %d: %s\n", resp.StatusCode, resp.Body)
		return
	}
	fmt.Println(resp.Body)
}

func (c *Client) doPut(key, value string) {
	addr := c.pickNode()
	if addr == "" {
		fmt.Println("error: no nodes available")
		return
	}
	resp, err := c.httpClient.Put(addr, "/kv?key="+key, value)
	if err != nil {
		c.log.Error("PUT failed: %v", err)
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("error %d: %s\n", resp.StatusCode, resp.Body)
		return
	}
	fmt.Println("OK")
}

func (c *Client) doDelete(key string) {
	addr := c.pickNode()
	if addr == "" {
		fmt.Println("error: no nodes available")
		return
	}
	resp, err := c.httpClient.Delete(addr, "/kv?key="+key)
	if err != nil {
		c.log.Error("DELETE failed: %v", err)
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("error %d: %s\n", resp.StatusCode, resp.Body)
		return
	}
	fmt.Println("OK")
}

func (n *NodeInfo) HostString() string {
	return fmt.Sprintf("%d.%d.%d.%d", n.Host[0], n.Host[1], n.Host[2], n.Host[3])
}
