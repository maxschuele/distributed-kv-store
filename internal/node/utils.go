package node

import (
	"fmt"
	"net"
)

type Tcp4Addr struct {
	Host [4]byte
	Port uint16
	Addr *net.TCPAddr
}

func (addr *Tcp4Addr) String() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", addr.Host[0], addr.Host[1], addr.Host[2], addr.Host[3], addr.Port)
}

func parseTcp4Addr(addrStr string) (*Tcp4Addr, error) {
	addr, err := net.ResolveTCPAddr("tcp4", addrStr)
	if err != nil {
		return nil, err
	}

	if addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, fmt.Errorf("address %q missing host: expected format host:port (e.g., 192.168.0.99:8080)", addrStr)
	}

	ip4 := addr.IP.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("address %q is not a ip4 address", addrStr)
	}

	return &Tcp4Addr{
		Host: [4]byte(ip4),
		Port: uint16(addr.Port),
		Addr: addr,
	}, nil

}

func formatAddress(host [4]byte, port uint16) string {
	return fmt.Sprintf("%d.%d.%d.%d:%d",
		host[0], host[1], host[2], host[3],
		port)
}
