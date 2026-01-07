package discovery

import (
	"net"
	"testing"
	"time"
)

func TestBroadcastAndListen(t *testing.T) {
	// Start listener in a goroutine
	receivedMsg := make(chan string)
	go func() {
		addr, _ := net.ResolveUDPAddr("udp4", "0.0.0.0:5972")
		conn, _ := net.ListenUDP("udp4", addr)
		defer conn.Close()

		buf := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, _ := conn.ReadFromUDP(buf)
		receivedMsg <- string(buf[:n])
	}()

	// Give listener time to start
	time.Sleep(100 * time.Millisecond)

	// Send broadcast
	testMsg := "ANNOUNCE|testNode|127.0.0.1|8080|1|false"
	err := Broadcast(BroadcastIP, BroadcastPort, testMsg)
	if err != nil {
		t.Fatalf("Broadcast failed: %v", err)
	}

	// Wait for message
	select {
	case msg := <-receivedMsg:
		if msg != testMsg {
			t.Errorf("Expected %s, got %s", testMsg, msg)
		}
		t.Logf("Successfully received: %s", msg)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout: no message received")
	}
}
