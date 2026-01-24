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

	if len(addr.IP) != 4 {
		return nil, fmt.Errorf("invalid tcp addr") // TODO proper error message
	}

	return &Tcp4Addr{
		Host: [4]byte(addr.IP),
		Port: uint32(addr.Port),
		Str:  addrStr,
	}, nil

}
