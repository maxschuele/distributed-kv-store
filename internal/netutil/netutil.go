package netutil

import (
	"fmt"
	"net"
)

type IPv4 [4]byte

func (ip IPv4) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
}

// FindInterfaceByIP validates the IP as IPv4 and returns the non-loopback
// network interface that has this IP assigned, along with the parsed IPv4 address.
func FindInterfaceByIP(ipStr string) (*net.Interface, IPv4, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, IPv4{}, fmt.Errorf("invalid IP address: %q", ipStr)
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return nil, IPv4{}, fmt.Errorf("not a valid IPv4 address: %q", ipStr)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, IPv4{}, fmt.Errorf("failed to get network interfaces: %w", err)
	}

	addr4 := IPv4(ip4)

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ifaceIP net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ifaceIP = v.IP
			case *net.IPAddr:
				ifaceIP = v.IP
			}

			if ifaceIP != nil && ifaceIP.Equal(ip) {
				return &iface, addr4, nil
			}
		}
	}

	return nil, IPv4{}, fmt.Errorf("IP %s not found on any non-loopback interface", ipStr)
}

func ValidatePort(port uint16) error {
	if port == 0 {
		return fmt.Errorf("port cannot be 0")
	}
	return nil
}

func FormatAddress(host IPv4, port uint16) string {
	return fmt.Sprintf("%s:%d", host.String(), port)
}

func ParseTcp4Addr(host IPv4, port uint16) (*net.TCPAddr, error) {
	addrStr := FormatAddress(host, port)
	return net.ResolveTCPAddr("tcp4", addrStr)
}
