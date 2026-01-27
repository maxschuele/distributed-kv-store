package node

import (
	"fmt"
	"net"
)

type Tcp4Addr struct {
	Host [4]byte
	Port uint32
	Str  string
}

func parseTcp4Addr(addrStr string) (*Tcp4Addr, error) {
	addr, err := net.ResolveTCPAddr("tcp4", addrStr)
	if err != nil {
		return nil, err
	}

	if addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, fmt.Errorf("address %q missing host: expected format host:port (e.g., 192.168.0.99:8080)", addrStr)
	}

	return &Tcp4Addr{
		Host: [4]byte(addr.IP.To4()),
		Port: uint32(addr.Port),
		Str:  addrStr,
	}, nil

}

func formatAddress(host [4]byte, port uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d:%d",
		host[0], host[1], host[2], host[3],
		port)
}
