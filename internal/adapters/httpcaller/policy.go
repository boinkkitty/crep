// Package httpcaller performs job HTTP requests with server-side request forgery protection.
package httpcaller

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// ErrBlockedDestination is returned for addresses or ports the policy forbids.
var ErrBlockedDestination = errors.New("destination blocked by policy")

// blockedPrefixes are non-public ranges: loopback, private, link-local (incl. cloud
// metadata 169.254.169.254), CGNAT, benchmarking, documentation, multicast, reserved.
var blockedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, s := range []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
		"172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16",
		"198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		"::/128", "::1/128", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001:db8::/32",
		"2002::/16", "fc00::/7", "fe80::/10", "ff00::/8",
	} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

// Policy decides which destinations jobs may call.
type Policy struct {
	AllowedPorts map[int]bool   // empty means 80 and 443
	AllowPrivate bool           // permit non-public addresses (local development only)
	DenyCIDRs    []netip.Prefix // additional operator-defined blocks
}

// ParsePorts parses "80,443,8080".
func ParsePorts(s string) (map[int]bool, error) {
	out := map[int]bool{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid port %q", p)
		}
		out[n] = true
	}
	return out, nil
}

func (p Policy) portAllowed(port int) bool {
	if len(p.AllowedPorts) == 0 {
		return port == 80 || port == 443
	}
	return p.AllowedPorts[port]
}

// CheckIP reports whether ip may be contacted.
func (p Policy) CheckIP(ip netip.Addr) error {
	ip = ip.Unmap()
	for _, pre := range p.DenyCIDRs {
		if pre.Contains(ip) {
			return fmt.Errorf("%w: %s", ErrBlockedDestination, ip)
		}
	}
	if p.AllowPrivate {
		return nil
	}
	if !ip.IsValid() || ip.IsUnspecified() {
		return fmt.Errorf("%w: %s", ErrBlockedDestination, ip)
	}
	for _, pre := range blockedPrefixes {
		if pre.Contains(ip) {
			return fmt.Errorf("%w: %s is not a public address", ErrBlockedDestination, ip)
		}
	}
	return nil
}

// CheckAddr validates a resolved "ip:port" at connect time.
func (p Policy) CheckAddr(address string) error {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBlockedDestination, err)
	}
	port, _ := strconv.Atoi(portStr)
	if !p.portAllowed(port) {
		return fmt.Errorf("%w: port %d not allowed", ErrBlockedDestination, port)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: unresolved host %q", ErrBlockedDestination, host)
	}
	return p.CheckIP(ip)
}

// CheckURL is the task-creation pre-check (ports.DestinationPolicy). It validates
// scheme, port, and literal IP hosts; names are resolved and checked on every connection.
func (p Policy) CheckURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return invalidURL("unparseable")
	}
	port := 0
	switch u.Scheme {
	case "http":
		port = 80
	case "https":
		port = 443
	default:
		return invalidURL("scheme must be http or https")
	}
	if ps := u.Port(); ps != "" {
		if port, err = strconv.Atoi(ps); err != nil {
			return invalidURL("invalid port")
		}
	}
	if !p.portAllowed(port) {
		return invalidURL(fmt.Sprintf("port %d is not allowed", port))
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if !p.AllowPrivate && (host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal")) {
		return invalidURL("host is not public")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if err := p.CheckIP(ip); err != nil {
			return invalidURL("host is not a public address")
		}
	}
	return nil
}
