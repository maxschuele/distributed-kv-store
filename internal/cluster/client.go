package cluster

import (
	"distributed-kv-store/internal/broadcast"
	"distributed-kv-store/internal/consistent"
	"distributed-kv-store/internal/httpclient"
	"distributed-kv-store/internal/logger"
	"distributed-kv-store/internal/netutil"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	broadcastPort uint16
	clusterView   *ClusterGroupView
	consistent    *consistent.ConsistentHash
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
		clusterView:   NewClusterView(log),
		consistent:    consistent.NewConsistentHash(150),
		httpClient:    httpclient.NewClient(),
		log:           log,
	}

	go func() {
		if err := broadcast.Listen(broadcastPort, client.log, client.handleBroadcastMessage); err != nil {
			client.log.Fatal("Error listening to broadcast: %v", err)
		}
	}()

	go client.clusterView.StartHeartbeatMonitor(HeartbeatTimeout, HeartbeatInterval, client.handleNodeRemoval)

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
		c.consistent.AddNode(msg.Info.GroupID)
	}
}

func (c *Client) handleNodeRemoval(removedNode NodeInfo) {
	c.log.Info("Removing node %s from hashing ring", removedNode.GroupID.String())
	c.consistent.RemoveNode(removedNode.GroupID)
}

func (c *Client) ProcessInput(input string) {
	command, values, _ := strings.Cut(input, " ")

	switch command {
	case "LIST":
		if values != "" {
			fmt.Println("error: LIST takes no arguments")
			return
		}
		c.doList()

	case "GET":
		if values == "" {
			fmt.Println("error: GET requires a key")
			return
		}
		key, _, found := strings.Cut(values, " ")
		if found {
			fmt.Println("error: GET takes only one key")
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
		if values == "" {
			fmt.Println("error: DELETE requires a key")
			return
		}
		key, _, found := strings.Cut(values, " ")
		if found {
			fmt.Println("error: DELETE takes only one key")
			return
		}
		c.doDelete(key)

	default:
		fmt.Println("invalid command")
	}
}

func (c *Client) pickNode(key string) (NodeInfo, string, error) {
	id, err := c.consistent.GetNode(key)
	if err != nil {
		return NodeInfo{}, "", err
	}

	node, exists := c.clusterView.GetNode(id)
	if !exists {
		return NodeInfo{}, "", fmt.Errorf("node %s not found in cluster view", id.String())
	}

	return node, netutil.FormatAddress(node.Host, node.HttpPort), nil
}

func (c *Client) doGet(key string) {
	node, addr, err := c.pickNode(key)
	if err != nil {
		fmt.Printf("failed to pick node: %v\n", err)
		return
	}

	resp, err := c.httpClient.Get(addr, "/kv?key="+key)
	if err != nil {
		c.log.Error("GET failed: %v", err)
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("error %d: %s | node: %s\n", resp.StatusCode, resp.Body, node.GroupID.String())
		return
	}
	fmt.Printf("%s | node: %s\n", resp.Body, node.GroupID.String())
}

func (c *Client) doPut(key, value string) {
	node, addr, err := c.pickNode(key)
	if err != nil {
		fmt.Printf("failed to pick node: %v\n", err)
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
	fmt.Printf("OK | node: %s\n", node.GroupID.String())
}

func (c *Client) doDelete(key string) {
	node, addr, err := c.pickNode(key)
	if err != nil {
		fmt.Printf("failed to pick node: %v\n", err)
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

	fmt.Printf("OK | node: %s\n", node.GroupID.String())
}

func (c *Client) doList() {
	nodes := c.clusterView.GetNodes()

	for _, node := range nodes {
		fmt.Printf("%s  %s\n", node.GroupID.String(), netutil.FormatAddress(node.Host, node.HttpPort))
	}
}
